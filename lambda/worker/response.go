package worker

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
	"github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	ResponseTimeout = 100 * time.Millisecond
)

type Preparer func(*SimpleResponse, resp.ResponseWriter)

type Response interface {
	redeo.Contextable

	// Command get command
	Command() string

	// Prepare overwrite to customize fields of a Response.
	Prepare()

	// Flush waits for the response to be received by the proxy.
	// Because the data will be written to a buffer and return immediately, a successful "flush"
	// may not guarantee that the data has been received by the proxy. In a function, the function
	// can be suspended any time and leave the data in the middle of the network stack, which may lead
	// to unexpected behavior. To avoid this, an acknowledgement is required to ensure that the data
	// has been received by the proxy. On timeout, the link will be closed.
	Flush() error

	// Size overwrite to return the size of a Response.
	Size() int64

	// Reset by binding the instance itself and redeo client. Must call close later.
	bind(*Link)

	// Get binded link
	getLink() *Link

	// Flush the buffer of specified writer which must match the specified client on calling reset.
	flush(resp.ResponseWriter) error

	// Mark an attempt to flush response, return attempts left.
	markAttempt() int

	// Get number of max attempts
	maxAttempts() int

	// Abandon the response
	abandon(error)

	// Close the response.
	close()
}

type BaseResponse struct {
	resp.ResponseWriter
	link      *Link
	attempted int
	err       error
	done      promise.Promise
	doneOnce  sync.Once
	inst      Response
	preparer  Preparer
	ctx       context.Context

	Attempts   int
	Cmd        string
	Body       []byte
	BodyStream resp.AllReadCloser
}

func (r *BaseResponse) Command() string {
	return r.Cmd
}

func (r *BaseResponse) String() string {
	return r.Cmd
}

// Overwrite me
func (r *BaseResponse) Prepare() {
}

func (r *BaseResponse) Flush() error {
	// Timeout added here, sometimes redeo may not handle all responses.
	r.done.SetTimeout(r.getTimeout())
	return r.done.Timeout()
}

func (r *BaseResponse) Size() int64 {
	if r.BodyStream != nil {
		return r.BodyStream.Len()
	} else {
		return int64(len(r.Body))
	}
}

// Context return the response context
func (r *BaseResponse) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// SetContext sets the client's context
func (r *BaseResponse) SetContext(ctx context.Context) {
	r.ctx = ctx
}

func (r *BaseResponse) bind(link *Link) {
	r.bindImpl(r, link)
}

func (r *BaseResponse) bindImpl(inst Response, link *Link) {
	if r.link == nil {
		if r.Attempts == 0 {
			r.Attempts = 1
		}
		r.done = promise.NewPromise()
		r.inst = inst
		r.link = link
	}
}

func (r *BaseResponse) getLink() *Link {
	return r.link
}

func (r *BaseResponse) flush(writer resp.ResponseWriter) error {
	r.ResponseWriter = writer
	r.err = nil
	if r.preparer != nil {
		r.preparer(r.inst.(*SimpleResponse), writer)
	} else {
		r.inst.Prepare()
	}

	client := redeo.GetClient(r.Context())
	conn := client.Conn()

	conn.SetWriteDeadline(time.Now().Add(ResponseTimeout)) // Set deadline for write
	defer conn.SetWriteDeadline(time.Time{})
	if err := r.ResponseWriter.Flush(); err != nil {
		r.err = err
		return err
	}

	hasBulk := true
	if r.Body != nil {
		conn.SetWriteDeadline(protocol.GetBodyDeadline(int64(len(r.Body))))
		if err := r.CopyBulk(bytes.NewReader(r.Body), int64(len(r.Body))); err != nil {
			r.err = err
			return err
		}
	} else if r.BodyStream != nil {
		conn.SetWriteDeadline(protocol.GetBodyDeadline(r.BodyStream.Len()))
		if err := r.CopyBulk(r.BodyStream, r.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := r.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			r.err = err
			return err
		}
	} else {
		hasBulk = false
	}

	if hasBulk {
		conn.SetWriteDeadline(time.Now().Add(ResponseTimeout)) // Set deadline for write
		r.err = r.ResponseWriter.Flush()
	}

	return r.err
}

func (r *BaseResponse) getTimeout() time.Duration {
	if r.Body != nil {
		return protocol.GetBodyTimeout(int64(len(r.Body)))
	} else if r.BodyStream != nil {
		return protocol.GetBodyTimeout(r.BodyStream.Len())
	} else {
		return ResponseTimeout
	}
}

func (r *BaseResponse) markAttempt() int {
	r.attempted++
	return r.Attempts - r.attempted
}

func (r *BaseResponse) maxAttempts() int {
	return r.Attempts
}

func (r *BaseResponse) abandon(err error) {
	r.err = err
	r.close()
}

func (r *BaseResponse) close() {
	if r.link != nil {
		r.doneOnce.Do(r.resolve)
	}
}

func (r *BaseResponse) resolve() {
	r.done.Resolve(&struct{}{})
}

type SimpleResponse struct {
	BaseResponse
}

func (r *SimpleResponse) bind(link *Link) {
	r.BaseResponse.bindImpl(r, link)
}

type BadResponse struct {
	BaseResponse
}

func (r *BadResponse) bind(link *Link) {
	r.BaseResponse.bindImpl(r, link)
}

func (r *BadResponse) flush(writer resp.ResponseWriter) error {
	stuck := make(chan struct{})
	<-stuck
	return nil
}

// ObjectResponse Response wrapper for objects.
type ObjectResponse struct {
	BaseResponse

	ReqId        string
	ChunkId      string
	Val          string
	Recovered    int64
	Extension    time.Duration
	PiggyFlags   int64
	PiggyPayload []byte
}

func (r *ObjectResponse) String() string {
	return fmt.Sprintf("%s %s(%s)", r.Cmd, r.ReqId, r.ChunkId)
}

func (r *ObjectResponse) Prepare() {
	r.AppendBulkString(r.Cmd)
	r.AppendBulkString(r.ReqId)
	r.AppendBulkString(r.ChunkId)
	if r.Cmd == protocol.CMD_GET {
		r.AppendInt(r.Recovered)
	}
	if len(r.Val) > 0 {
		r.AppendBulkString(r.Val)
	}
}

func (r *ObjectResponse) bind(link *Link) {
	r.BaseResponse.bindImpl(r, link)
}

func (r *ObjectResponse) Size() int64 {
	if r.BodyStream != nil {
		return r.BodyStream.Len()
	} else if r.Body != nil {
		return int64(len(r.Body))
	} else {
		return int64(len(r.Val))
	}
}

func (r *ObjectResponse) flush(writer resp.ResponseWriter) error {
	link := LinkFromClient(redeo.GetClient(r.Context()))
	link.acked.Reset()

	err := r.BaseResponse.flush(writer)
	if err != nil {
		// link.acked.Resolve()
		return err
	}

	if r.Extension > time.Duration(0) {
		writer.AppendInt(lifetime.GetSession().Timeout.GetEstimateDue(r.Extension).UnixNano())
		writer.AppendInt(r.PiggyFlags)
		if r.PiggyFlags&protocol.PONG_WITH_PAYLOAD > 0 {
			writer.AppendBulk(r.PiggyPayload)
		}
		if err = writer.Flush(); err != nil {
			// link.acked.Resolve()
			return err
		}
	}

	link.acked.SetTimeout(ResponseTimeout)
	return nil
}

// ErrorResponse Response wrapper for errors.
type ErrorResponse struct {
	BaseResponse

	Error interface{}
}

func (e *ErrorResponse) String() string {
	return fmt.Sprintf("%v", e.Error)
}

func (e *ErrorResponse) Prepare() {
	e.BaseResponse.Cmd = "error"
	e.AppendErrorf("%v", e.Error)
}

func (e *ErrorResponse) bind(link *Link) {
	e.BaseResponse.bindImpl(e, link)
}
