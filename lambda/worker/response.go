package worker

import (
	"bytes"
	"sync"
	"time"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	RequestTimeout = 1 * time.Second
)

type Preparer func(resp.ResponseWriter)

type Response interface {
	// Overwrite to customize fields of a Response.
	Prepare()

	// Wait for flushing.
	Flush() error

	// Overwrite to return the size of a Response.
	Size() int64

	// Reset by binding the instance itself and redeo client. Must call close later.
	reset(Response, *redeo.Client)

	// Flush the buffer of specified writer which must match the specified client on calling reset.
	flush(resp.ResponseWriter) error

	// Close the response.
	close()
}

type BaseResponse struct {
	resp.ResponseWriter
	link     *redeo.Client
	err      error
	done     sync.WaitGroup
	inst     Response
	preparer Preparer

	Body       []byte
	BodyStream resp.AllReadCloser
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

func (r *BaseResponse) reset(inst Response, link *redeo.Client) {
	if r.link == nil {
		r.done.Add(1)
	}
	r.inst = inst
	r.link = link
}

func (r *BaseResponse) flush(writer resp.ResponseWriter) error {
	r.ResponseWriter = writer
	r.err = nil
	if r.preparer != nil {
		r.preparer(writer)
	} else {
		r.inst.Prepare()
	}

	conn := GetConnectionByLink(r.link)

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

func (r *BaseResponse) close() {
	r.done.Done()
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
	e.AppendErrorf("%v", e.Error)
}
