package types

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mason-leap-lab/redeo/resp"
)

type ResponseCloser func()

type ProxyResponse struct {
	Response interface{}
	Request  *Request

	ctx context.Context
}

// Context return the response context
func (r *ProxyResponse) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// SetContext sets the client's context
func (r *ProxyResponse) SetContext(ctx context.Context) {
	r.ctx = ctx
}

type Response struct {
	Id         Id
	Cmd        string
	Size       string
	Body       []byte
	BodyStream resp.AllReadCloser
	Status     int64

	closer ResponseCloser

	w  resp.ResponseWriter
	mu sync.Mutex
}

func (rsp *Response) String() string {
	return fmt.Sprintf("%s %v", rsp.Cmd, rsp.Id)
}

func (rsp *Response) PrepareForSet(w resp.ResponseWriter, seq int64) {
	w.AppendInt(seq)
	w.AppendBulkString(rsp.Id.ReqId)
	w.AppendBulkString(rsp.Id.ChunkId)
	w.AppendBulk(rsp.Body)
	rsp.w = w
}

func (rsp *Response) PrepareForGet(w resp.ResponseWriter, seq int64) {
	w.AppendInt(seq)
	w.AppendBulkString(rsp.Id.ReqId)
	w.AppendBulkString(rsp.Size)
	if rsp.Body == nil && rsp.BodyStream == nil {
		w.AppendBulkString("-1")
	} else {
		w.AppendBulkString(rsp.Id.ChunkId)
	}
	// Only one body field is returned, stream is prefered.
	if rsp.BodyStream == nil && rsp.Body != nil {
		w.AppendBulk(rsp.Body)
	}
	rsp.w = w
}

func (rsp *Response) Flush() error {
	if rsp.w == nil {
		return errors.New("writer for response not set")
	}
	w := rsp.w
	rsp.w = nil

	if rsp.BodyStream != nil {
		if err := w.CopyBulk(rsp.BodyStream, rsp.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := rsp.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
	}

	return w.Flush()
}

func (rsp *Response) OnClose(closer ResponseCloser) {
	if rsp.closer != nil {
		closer = func(oldCloser ResponseCloser, newCloser ResponseCloser) ResponseCloser {
			return func() {
				oldCloser()
				newCloser()
			}
		}(rsp.closer, closer)
	}
	rsp.closer = closer
}

// Close will block and wait for the stream to be flushed.
// Don't clean any fields if it can't be blocked until flushed.
func (rsp *Response) Close() error {
	rsp.mu.Lock()
	defer rsp.mu.Unlock()

	var err error
	if rsp.BodyStream != nil {
		err = rsp.BodyStream.Close()
		rsp.BodyStream = nil
	}
	if rsp.closer != nil {
		rsp.closer()
		rsp.closer = nil
	}
	return err
}
