package client

import (
	"context"
	"errors"
	"sync/atomic"
)

const (
	REQUEST_INVOKED   = 0
	REQUEST_RESPONDED = 1
)

var (
	ErrResponded    = errors.New("responded")
	ErrNotConnected = errors.New("not connected")

	CtxKeyConn = requestCtxKey("conn")
)

type ResponseNotifier func(response interface{}, err error, reason string)

type Request interface {
	// Seq gets the sequence of the request.
	Seq() int64

	// SetSeq sets the sequence of the request.
	SetSeq(int64)

	// Conn gets connection.
	Conn() *Conn

	// Flush writes data to the connection.
	Flush() error

	// SetResponse sets the response for the request.
	SetResponse(response interface{}, reason string) error

	// IsResponded returns if the request is responded.
	IsResponded() (reason string, responded bool)

	// Response waits and returns the response and the error of the request.
	Response() (interface{}, error)

	// OnRespond registers the handler that will be notified when the response is available.
	OnRespond(ResponseNotifier)

	// Context returns context.
	Context() context.Context

	// SetContext sets context.
	SetContext(context.Context)
}

type Retriable interface {
	// GetAttempts returns the number of left attemps and the error of the last attempt.
	GetAttempts() (int, error)
}

type requestCtxKey string

type request struct {
	seq       int64
	response  interface{}
	err       error
	reason    string
	ctx       context.Context
	status    uint32
	responded chan struct{}
	onRespond ResponseNotifier
}

func NewRequest() Request {
	return NewRequestWithContext(context.TODO())
}

func NewRequestWithContext(ctx context.Context) Request {
	return &request{
		ctx:       ctx,
		responded: make(chan struct{}),
		onRespond: defaultResponseNotifier,
	}
}

func (req *request) Seq() int64 {
	return req.seq
}

func (req *request) SetSeq(seq int64) {
	req.seq = seq
}

func (req *request) Conn() *Conn {
	cn, _ := req.Context().Value(CtxKeyConn).(*Conn)
	return cn
}

func (req *request) Flush() error {
	cn := req.Conn()
	if cn == nil {
		return ErrNotConnected
	}

	return cn.Flush()
}

func (req *request) SetResponse(rsp interface{}, reason string) error {
	if !atomic.CompareAndSwapUint32(&req.status, REQUEST_INVOKED, REQUEST_RESPONDED) {
		return ErrResponded
	}

	if err, ok := rsp.(error); ok {
		req.err = err
	} else {
		req.response = rsp
	}
	req.reason = reason

	close(req.responded)
	if cn := req.Conn(); cn != nil {
		cn.EndRequest(req)
	}
	req.onRespond(req.response, req.err, reason)
	return nil
}

func (req *request) IsResponded() (string, bool) {
	return req.reason, atomic.LoadUint32(&req.status) == REQUEST_RESPONDED
}

func (req *request) Response() (interface{}, error) {
	if req.ctx != nil {
		select {
		case <-req.responded:
			break
		case <-req.ctx.Done():
			// Call SetResponse for thread safty.
			req.SetResponse(req.ctx.Err(), "context done")
		}
	}

	// Wait again in case response was just set.
	<-req.responded
	return req.response, req.err
}

func (req *request) OnRespond(notifier ResponseNotifier) {
	req.onRespond = notifier
}

func (req *request) Context() context.Context {
	ctx := req.ctx
	if ctx == nil {
		return context.Background()
	} else {
		return ctx
	}
}

func (req *request) SetContext(ctx context.Context) {
	req.ctx = ctx
}

func defaultResponseNotifier(_ interface{}, _ error, _ string) {
	// do nothing
}
