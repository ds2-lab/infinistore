package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	sysnet "net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo/client"
	"github.com/mason-leap-lab/redeo/resp"
)

// Define different Errors during the connection
var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrUnexpectedType   = errors.New("unexpected response type")
	ErrNoHandler        = errors.New("response handler not set")
	ErrNoResponse       = errors.New("response not set")
)

type RequestWriter func(Request) error

type ResponseHandler interface {
	ReadResponse(Request) error
}

type ConnConfig func(*Conn)

type Conn struct {
	client.Conn
	Meta    interface{}
	Handler ResponseHandler

	lastError error
	cwnd      *Window
	rseq      chan interface{}
	closed    uint32
	done      chan struct{}
	routings  sync.WaitGroup
	wMu       sync.Mutex   // Mutex to avoid concurrent writing to the connection.
	wReq      *RequestMeta // Request that is writing to the connection now.
}

func NewConn(cn sysnet.Conn, configs ...ConnConfig) *Conn {
	conn := &Conn{
		Conn: client.Wrap(cn),
		cwnd: NewWindow(),
		rseq: make(chan interface{}),
		done: make(chan struct{}),
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
func (conn *Conn) StartRequest(req Request, writes ...RequestWriter) (err error) {
	// Abort if the request has responded
	if reason, ok := req.IsResponded(); ok {
		return fmt.Errorf("%v: %s", ErrResponded, reason)
	}
	req.SetContext(context.WithValue(req.Context(), CtxKeyConn, conn))

	// Lock writer
	if err := conn.writeStart(req); err != nil { // If connection is closed, err will be returned.
		return err
	}

	if conn.IsClosed() {
		// Request will be cleared.
		return conn.writeEnd(req, ErrConnectionClosed)
	}

	// Both calback writer and request writer (Flush) are supported.
	conn.SetWriteDeadline(time.Now().Add(DefaultTimeout))
	for _, write := range writes {
		err = write(req) // Keep track of the last error
		if err != nil {
			break
		}
	}
	// Flush if no error
	if err == nil {
		err = req.Flush()
	}

	// Handle result
	if err != nil {
		// Handle connection error
		if conn.isConnectionFailed(err) {
			conn.Close()
		}
	} else if _, ok := req.IsResponded(); ok {
		// If request get responded during adding, EndRequest may or may not be called successfully.
		// Ack again to ensure req getting removed from cwnd.
		conn.cwnd.AckRequest(req.Seq())
	}

	// End request
	return conn.writeEnd(req, err)
}

// StartRequest increase the request counter by 1
func (conn *Conn) EndRequest(req Request) {
	conn.cwnd.AckRequest(req.Seq())
}

func (conn *Conn) LastError() error {
	return conn.lastError
}

func (conn *Conn) IsClosed() bool {
	if atomic.LoadUint32(&conn.closed) == 1 {
		// Wait for the connection to be closed.
		<-conn.done
		return true
	} else {
		return false
	}
}

func (conn *Conn) Close() error {
	return conn.CloseWithReason("closed")
}

// Close Signal connection should be closed. Function close() will be called later for actural operation
func (conn *Conn) CloseWithReason(reason string) error {
	if !atomic.CompareAndSwapUint32(&conn.closed, 0, 1) {
		return ErrConnectionClosed
	}

	// Close connection to force block read to quit
	err := util.CloseWithReason(conn.GetConn(), reason)

	// Signal sub-goroutings to quit.
	select {
	case <-conn.done:
		return ErrConnectionClosed
	default:
		close(conn.done)
	}

	go func() {
		// Wait for quiting of all sub-goroutings
		conn.routings.Wait()

		// Release resources
		conn.Conn.Close()
	}()

	return err
}

// SetDeadline overwrites default implementation by reset the timeout of both reading and writing request.
func (conn *Conn) SetDeadline(t time.Time) error {
	err := conn.Conn.SetDeadline(t)
	if err != nil {
		return err
	}

	// Set deadline of wReq is mainly for reading response.
	wReq := conn.wReq
	if wReq != nil {
		wReq.Deadline = t
	}
	return nil
}

// SetReadDeadline overwrites default implementation by reset the timeout of reading request.
func (conn *Conn) SetReadDeadline(t time.Time) error {
	err := conn.Conn.SetReadDeadline(t)
	if err != nil {
		return err
	}

	wReq := conn.wReq
	if wReq != nil {
		wReq.Deadline = t
	}
	return nil
}

func (conn *Conn) SetWindowSize(size int) {
	conn.cwnd.SetSize(size)
}

func (conn *Conn) writeStart(req Request) (err error) {
	conn.wMu.Lock()
	meta, err := conn.cwnd.AddRequest(req) // If connection is closed, err will be returned.
	if err != nil {
		conn.wMu.Unlock()
		return err
	}
	conn.wReq = meta
	conn.routings.Add(1) // Avoid connection being release during writing.
	return
}

func (conn *Conn) writeEnd(req Request, err error) error {
	conn.routings.Done()
	if err == nil {
		conn.wReq.Sent = true
		conn.wReq.Deadline = time.Now().Add(DefaultTimeout)
	} else {
		conn.cwnd.AckRequest(req.Seq())
	}
	conn.wReq = nil
	conn.wMu.Unlock()
	return err
}

func (conn *Conn) handleResponses() {
	conn.routings.Add(1)
	defer conn.routings.Done()

	for {
		// Peek Response
		go conn.readSeq()
		var read interface{}
		select {
		case <-conn.done:
			conn.close("closed")
			return
		case read = <-conn.rseq:
		}

		// Identify error
		var rseq int64
		switch ret := read.(type) {
		case error:
			conn.lastError = ret
			conn.close("closedError")
			return
		case int64:
			rseq = ret
		}

		var readErr error
		req, err := conn.cwnd.MatchRequest(rseq)
		if err != nil {
			// For now we can not skip the whole response blindly, disconnect.
			// TODO: Add duplex support.
			// TODO: Add frame protocol to skip response regardless inner structure.
			readErr = err
		} else if conn.Handler == nil {
			readErr = ErrNoHandler
			req.SetResponse(readErr, "handling responses")
		} else {
			err = conn.Handler.ReadResponse(req)
			if err != nil && conn.isConnectionFailed(err) {
				readErr = err
			}
			// Set ErrNoResponse as default response
			req.SetResponse(ErrNoResponse, "handling responses")
		}

		if readErr != nil {
			conn.lastError = readErr
			conn.close("closedError")
			return
		}
	}
}

func (conn *Conn) readSeq() {
	if conn.IsClosed() {
		return
	}

	conn.routings.Add(1)
	defer conn.routings.Done()

	// Got response, reset read deadline.
	conn.SetReadDeadline(time.Time{})

	ret, err := conn.PeekType()
	// ret, err := conn.ReadInt()
	if err != nil {
		conn.notifyRseq(err)
		return
	}

	switch ret {
	case resp.TypeInt:
		seq, err := conn.ReadInt()
		if err != nil {
			conn.notifyRseq(err)
		} else {
			conn.notifyRseq(seq)
		}
	default:
		conn.notifyRseq(fmt.Errorf("unexpect sequence type: \"%v\"", ret))
	}
}

func (conn *Conn) notifyRseq(rseq interface{}) {
	select {
	case conn.rseq <- rseq:
	default:
		// No consumer. The connection can be closed, double check.
		if !conn.IsClosed() {
			conn.rseq <- rseq
		}
	}
}

func (conn *Conn) close(reason string) error {
	// Call signal function to avoid duplicated close.
	err := conn.CloseWithReason(reason)

	// Drain possible stucks
	select {
	case <-conn.rseq:
	default:
	}

	// Clean up pending requests
	conn.cwnd.Close()

	return err
}

func (conn *Conn) isConnectionFailed(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe {
		return true
	} else if _, ok := err.(sysnet.Error); ok {
		// All net.Error counts, they are either timeout or permanent(non-temporary) error.
		return true
	}

	return false
}
