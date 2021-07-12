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
	ErrResponded = errors.New("responded")

	CtxKeyConn = requestCtxKey("conn")
)

type ResponseNotifier func(interface{}, error)

type Request interface {
	// Seq gets the sequence of the request.
	Seq() int64

	// SetSeq sets the sequence of the request.
	SetSeq(int64)

	Conn() *Conn

	// SetResponse sets the response for the request.
	SetResponse(interface{}) error

	// IsResponded returns if the request is responded.
	IsResponded() bool

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

func (req *request) SetResponse(rsp interface{}) error {
	if !atomic.CompareAndSwapUint32(&req.status, REQUEST_INVOKED, REQUEST_RESPONDED) {
		return ErrResponded
	}

	if err, ok := rsp.(error); ok {
		req.err = err
	} else {
		req.response = rsp
	}

	close(req.responded)
	if cn := req.Conn(); cn != nil {
		cn.EndRequest(req)
	}
	req.onRespond(req.response, req.err)
	return nil
}

func (req *request) IsResponded() bool {
	return atomic.LoadUint32(&req.status) == REQUEST_RESPONDED
}

func (req *request) Response() (interface{}, error) {
	if req.ctx != nil {
		select {
		case <-req.responded:
			break
		case <-req.ctx.Done():
			// Call SetResponse for thread safty.
			req.SetResponse(req.ctx.Err())
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
	if req.ctx == nil {
		req.ctx = context.Background()
	}
	return req.ctx
}

func (req *request) SetContext(ctx context.Context) {
	req.ctx = ctx
}

func defaultResponseNotifier(_ interface{}, _ error) {
	// do nothing
}
