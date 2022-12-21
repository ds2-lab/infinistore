package types

import (
	"context"
	"errors"
	"fmt"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

type ResponseFinalizer func(*Response)

type ProxyResponse interface {
	redeo.Contextable
	Request() *Request
	Response() interface{}
}

type proxyResponse struct {
	protocol.Contextable
	response interface{}
	request  *Request
}

// Context return the response context
func (r *proxyResponse) Response() interface{} {
	return r.response
}

// SetContext sets the client's context
func (r *proxyResponse) Request() *Request {
	return r.request
}

type Response struct {
	util.Closer
	protocol.Contextable

	Id         Id
	Cmd        string
	Size       string
	Body       []byte
	bodyStream resp.AllReadCloser
	Status     int64 // Customized status. For GET: 1 - recovered

	request   *Request
	finalizer ResponseFinalizer
	abandon   bool         // Abandon flag, set in Request class.
	cached    PersistChunk // Cached flag, the response is served from cache.

	w         resp.ResponseWriter
	ctxCancel context.CancelFunc
	ctxDone   <-chan struct{}
	from      string
}

func NewResponse(cmd string) *Response {
	rsp := &Response{Cmd: cmd}
	rsp.Closer.Init()
	rsp.from = "responded"
	return rsp
}

func (rsp *Response) Response() interface{} {
	return rsp
}

func (rsp *Response) Request() *Request {
	return rsp.request
}

func (rsp *Response) String() string {
	return fmt.Sprintf("%s %s:%v", rsp.from, rsp.Cmd, &rsp.Id)
}

func (rsp *Response) SetBodyStream(stream resp.AllReadCloser) {
	var ctx context.Context
	ctx, rsp.ctxCancel = context.WithCancel(rsp.Context())
	rsp.ctxDone = ctx.Done() // There is bug to laziliy call the ctx.Done() in highly parallelized settings. Initiate and cache the done channel to avoid the bug.
	rsp.SetContext(ctx)
	rsp.bodyStream = stream
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
	if rsp.Body == nil && rsp.bodyStream == nil {
		w.AppendBulkString("-1")
	} else if rsp.Context().Err() != nil { // Here is a good place to test the ctxCancellation again if the rsp was ctxCancelled before the client is available.
		// Cancelled request is treated as abandon.
		w.AppendBulkString("-1")
		// Clear body
		rsp.Body = nil
		if holdable, ok := rsp.bodyStream.(resp.Holdable); ok {
			holdable.Unhold()
		}
		rsp.bodyStream = nil
	} else {
		w.AppendBulkString(rsp.Id.ChunkId)
	}
	// Only one body field is returned, stream is prefered.
	if rsp.bodyStream == nil && rsp.Body != nil {
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

	if rsp.bodyStream != nil {
		if err := w.CopyBulk(rsp.bodyStream, rsp.bodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := rsp.bodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			// If error is cause by the CancelFlush, override the return error.
			if rsp.Context().Err() != nil {
				return rsp.Context().Err()
			} else {
				return err
			}
		}
	}

	// Override the error if the response is abandoned and err occurred.
	err := w.Flush()
	if err != nil && rsp.Context().Err() != nil {
		err = rsp.Context().Err()
	}
	return err
}

func (rsp *Response) IsAbandon() bool {
	return rsp.abandon
}

func (rsp *Response) IsCached() (stored int64, full bool, cached bool) {
	if rsp.cached == nil {
		return 0, false, false
	}
	return rsp.cached.BytesStored(), rsp.cached.IsStored(), true
}

func (rsp *Response) WaitFlush(ctxCancelable bool) error {
	stream := rsp.bodyStream
	if stream != nil {
		if !ctxCancelable {
			return stream.Close()
		}

		// Allow the wait be ctxCanceled.
		chWait := make(chan error)
		go func() {
			chWait <- stream.Close()
		}()
		defer rsp.CancelFlush()

		select {
		case err := <-chWait: // No need to store generated error, for it will be identical to the one generated during CopyBulk()
			return err
			// break
		case <-rsp.ctxDone:
			rsp.abandon = true
			// Try preempt transimission.
			// If the stream is served by Lambda, we disconnect the client and wait for stream consumed to reuse limited Lambda connections.
			if stored, _, cached := rsp.IsCached(); !cached {
				// Disconnect the client if it's available.
				client := redeo.GetClient(rsp.Context())
				if client != nil {
					client.Conn().Close()
				} // else test ctxCancellation after client is available.

				// Register finalizer to wait for the close of the stream.
				rsp.OnFinalize(func(_ *Response) {
					<-chWait
				})
			} else if stored > 0 {
				// Once the cahced starts accepting data, it is unlikely to interrupt. We will then not do abandon the stream for good throughput.
				return <-chWait
			}
			// If the stream is served from cache and waiting for data, we simply do nothing. Cached stream will be canceled automatically in proxy.waitForCache()

			return rsp.Context().Err()
		}
	}
	return nil
}

func (rsp *Response) CancelFlush() {
	if rsp.ctxCancel != nil {
		rsp.ctxCancel()
	}
}

func (rsp *Response) OnFinalize(finalizer ResponseFinalizer) {
	if rsp.finalizer != nil {
		finalizer = func(oldFinalizer ResponseFinalizer, newFinalizer ResponseFinalizer) ResponseFinalizer {
			return func(rsp *Response) {
				newFinalizer(rsp)
				oldFinalizer(rsp)
			}
		}(rsp.finalizer, finalizer)
	}
	rsp.finalizer = finalizer
}

func (rsp *Response) Close() {
	if rsp.finalizer != nil {
		rsp.finalizer(rsp)
		rsp.finalizer = nil
	}

	rsp.Closer.Close()
}

// Close will block and wait for the stream to be flushed.
// Don't clean any fields if it can't be blocked until flushed.
func (rsp *Response) Wait() {
	rsp.Closer.Wait()
}
