package worker

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	RequestTimeout = 1 * time.Second

	ctxKeyClient = struct{}{}
)

type Preparer func(*SimpleResponse, resp.ResponseWriter)

type Response interface {
	redeo.Contextable

	// String show friendly name
	String() string

	// Prepare overwrite to customize fields of a Response.
	Prepare()

	// Flush wait for flushing.
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

	// Close the response.
	close()
}

type BaseResponse struct {
	resp.ResponseWriter
	link      *Link
	attempted int
	err       error
	done      sync.WaitGroup
	inst      Response
	preparer  Preparer
	ctx       context.Context

	Attempts   int
	Cmd        string
	Body       []byte
	BodyStream resp.AllReadCloser
}

func (r *BaseResponse) String() string {
	return r.Cmd
}

// Overwrite me
func (r *BaseResponse) Prepare() {
}

func (r *BaseResponse) Flush() error {
	r.done.Wait()
	return r.err
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
			r.Attempts = MaxAttempts
		}
		r.done.Add(1)
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

	conn.SetWriteDeadline(time.Now().Add(RequestTimeout)) // Set deadline for write
	defer conn.SetWriteDeadline(time.Time{})
	if err := r.ResponseWriter.Flush(); err != nil {
		r.err = err
		return err
	}

	hasBulk := true
	if r.Body != nil {
		conn.SetWriteDeadline(time.Time{}) // Disable timeout for bulk data
		if err := r.CopyBulk(bytes.NewReader(r.Body), int64(len(r.Body))); err != nil {
			r.err = err
			return err
		}
	} else if r.BodyStream != nil {
		conn.SetWriteDeadline(time.Time{}) // Disable timeout for bulk data
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
		conn.SetWriteDeadline(time.Now().Add(RequestTimeout)) // Set deadline for write
		r.err = r.ResponseWriter.Flush()
	}

	return r.err
}

func (r *BaseResponse) markAttempt() int {
	r.attempted++
	return r.Attempts - r.attempted
}

func (r *BaseResponse) close() {
	r.done.Done()
}

type SimpleResponse struct {
	BaseResponse
}

func (r *SimpleResponse) bind(link *Link) {
	r.BaseResponse.bindImpl(r, link)
}

// ObjectResponse Response wrapper for objects.
type ObjectResponse struct {
	BaseResponse

	Cmd        string
	ConnId     string
	ReqId      string
	ChunkId    string
	Val        string
	Body       []byte
	BodyStream resp.AllReadCloser
}

func (r *ObjectResponse) Prepare() {
	r.AppendBulkString(r.Cmd)
	r.AppendBulkString(r.ConnId)
	r.AppendBulkString(r.ReqId)
	r.AppendBulkString(r.ChunkId)
	if len(r.Val) > 0 {
		r.AppendBulkString(r.Val)
	}
	r.BaseResponse.Body = r.Body
	r.BaseResponse.BodyStream = r.BodyStream
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

// ErrorResponse Response wrapper for errors.
type ErrorResponse struct {
	BaseResponse

	Error interface{}
}

func (e *ErrorResponse) Prepare() {
	e.BaseResponse.Cmd = "error"
	e.AppendErrorf("%v", e.Error)
}

func (e *ErrorResponse) bind(link *Link) {
	e.BaseResponse.bindImpl(e, link)
}
