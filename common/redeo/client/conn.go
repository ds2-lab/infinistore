package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo/client"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrUnexpectedType   = errors.New("unexpected response type")
	ErrNoHandler        = errors.New("response handler not set")
	ErrNotFound         = errors.New("resource not found")
)

type ResponseHandler interface {
	ReadResponse(Request) error
}

type ConnConfig func(*Conn)

type Conn struct {
	client.Conn
	Meta    interface{}
	Handler ResponseHandler

	shortcut  *types.MockConn
	lastError error
	cwnd      *Window
	respType  chan interface{}
	closed    uint32
	done      chan struct{}
	routings  sync.WaitGroup
	// mu        sync.Mutex
}

func NewShortcut(cn *types.MockConn, configs ...ConnConfig) *Conn {
	conn := NewConn(cn.Client, configs...)
	conn.shortcut = cn
	return conn
}

func NewConn(cn net.Conn, configs ...ConnConfig) *Conn {
	conn := &Conn{
		Conn:     client.Wrap(cn),
		cwnd:     NewWindow(),
		respType: make(chan interface{}),
		done:     make(chan struct{}),
	}
	for _, config := range configs {
		config(conn)
	}
	if conn.Handler != nil {
		go conn.handleResponses()
	}
	return conn
}

// StartRequest increase the request counter by 1
func (conn *Conn) StartRequest(req Request) {
	// Abort if the request has responded
	if req.IsResponded() {
		return
	}

	// Add request to the cwnd
	req.SetContext(context.WithValue(req.Context(), CtxKeyConn, conn))
	conn.cwnd.AddRequest(req)

	// If request get responded during adding, EndRequest may or may not be called successfully.
	// Ack again to ensure req getting removed from cwnd.
	if req.IsResponded() {
		conn.cwnd.AckRequest(req.Seq())
	}
}

// StartRequest increase the request counter by 1
func (conn *Conn) EndRequest(req Request) {
	conn.cwnd.AckRequest(req.Seq())
}

func (conn *Conn) LastError() error {
	return conn.lastError
}

func (conn *Conn) IsClosed() bool {
	return atomic.LoadUint32(&conn.closed) == 1
}

// Close Signal connection should be closed. Function close() will be called later for actural operation
func (conn *Conn) Close() error {
	if !atomic.CompareAndSwapUint32(&conn.closed, 0, 1) {
		return ErrConnectionClosed
	}

	// Signal sub-goroutings to quit.
	select {
	case <-conn.done:
	default:
		close(conn.done)
	}

	// Close connection to force block read to quit
	err := conn.GetConn().Close()
	conn.invalidate(conn.shortcut)

	go func() {
		// Wait for quiting of all sub-goroutings
		conn.routings.Wait()

		// Release resources
		conn.Release()
	}()

	return err
}

func (conn *Conn) handleResponses() {
	conn.routings.Add(1)
	defer conn.routings.Done()

	for {
		// Got response, reset read deadline.
		conn.SetReadDeadline(time.Time{})

		// Peek Response
		go conn.peekResponse()
		var retPeek interface{}
		select {
		case <-conn.done:
			conn.close()
			return
		case retPeek = <-conn.respType:
		}

		// Identify error
		var respType resp.ResponseType
		switch ret := retPeek.(type) {
		case error:
			conn.lastError = ret
			conn.close()
			return
		case resp.ResponseType:
			respType = ret
		}

		var readErr error
		switch respType {
		case resp.TypeError:
			strErr, err := conn.ReadError()
			if err != nil {
				readErr = err
				break
			}
			req, err := conn.readSeq()
			if err == nil {
				req.SetResponse(errors.New(strErr))
			} else if err != ErrAcked {
				readErr = err
			}
		case resp.TypeNil:
			err := conn.ReadNil()
			if err != nil {
				readErr = err
				break
			}
			req, err := conn.readSeq()
			if err == nil {
				req.SetResponse(ErrNotFound)
			} else if err != ErrAcked {
				readErr = err
			}
		case resp.TypeInt:
			req, err := conn.readSeq()
			if err != nil {
				// For now we can not skip the whole response blindly, disconnect.
				// TODO: Add frame protocol to skip response regardless inner structure.
				readErr = err
			} else if conn.Handler == nil {
				readErr = ErrNoHandler
				req.SetResponse(readErr)
			} else {
				err = conn.Handler.ReadResponse(req)
				if err != nil && util.IsConnectionFailed(err) {
					readErr = err
				}
			}
		default:
			if err := conn.skip(respType); err != nil {
				readErr = err
			}
		}

		if readErr != nil {
			conn.lastError = readErr
			conn.close()
			return
		}
	}
}

func (conn *Conn) peekResponse() {
	conn.routings.Add(1)
	var ret interface{}
	ret, err := conn.PeekType()
	if err != nil {
		ret = err
	}
	select {
	case conn.respType <- ret:
	default:
		// No consumer. The connection must be closed, abandon.
	}
	conn.routings.Done()
}

func (conn *Conn) close() error {
	// Call signal function to avoid duplicated close.
	err := conn.Close()

	// Clean up pending requests
	conn.cwnd.Close()

	return err
}

func (conn *Conn) invalidate(shortcut *types.MockConn) {
	// Thread safe implementation
	if shortcut != nil && atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&conn.shortcut)), unsafe.Pointer(shortcut), unsafe.Pointer(nil)) {
		shortcut.Invalid()
	}
}

func (conn *Conn) skip(t resp.ResponseType) error {
	if t == resp.TypeUnknown {
		var err error
		t, err = conn.PeekType()
		if err != nil {
			return err
		}
	}

	switch t {
	case resp.TypeArray:
		if arrLen, err := conn.ReadArrayLen(); err != nil {
			return err
		} else {
			for i := 0; i < arrLen; i++ {
				if err := conn.skip(resp.TypeUnknown); err != nil {
					return err
				}
			}
		}
	case resp.TypeBulk:
		if err := conn.SkipBulk(); err != nil {
			return err
		}
	case resp.TypeUnknown:
		return ErrUnexpectedType
	default:
		// We just want read a line, ignore other error
		if _, err := conn.ReadInlineString(); err != nil && !resp.IsProtocolError(err) {
			return err
		}
	}
	return nil
}

func (conn *Conn) readSeq() (Request, error) {
	// Handle response
	seq, err := conn.ReadInt()
	if err != nil {
		return nil, err
	}
	return conn.cwnd.AckRequest(seq)
}
