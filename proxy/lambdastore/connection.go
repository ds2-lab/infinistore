package lambdastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/lambda/invoker"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/redeo/resp"
)

const (
	ConnectionOpen uint32 = iota
	ConnectionClosing
	ConnectionClosed
)

var (
	defaultConnectionLog = &logger.ColorLogger{
		Prefix: "Undesignated ",
		Color:  !global.Options.NoColor,
	}
	readerPool sync.Pool
	writerPool sync.Pool

	ErrConnectionClosed      = errors.New("connection closed")
	ErrMissingResponse       = errors.New("missing response")
	ErrUnexpectedCommand     = errors.New("unexpected command")
	ErrUnexpectedType        = errors.New("unexpected type")
	ErrMissingRequest        = errors.New("missing request")
	ErrUnexpectedSendRequest = errors.New("unexpected SendRequest call")
)

// TODO: use bsm/pool
type Connection struct {
	Id uint32
	net.Conn
	workerId int32 // Identify a unique lambda worker
	control  bool  // Identify if the connection is control link or not.
	lm       *LinkManager

	instance    *Instance
	log         logger.ILogger
	w           *resp.RequestWriter
	r           resp.ResponseReader
	mu          sync.Mutex
	chanWait    chan *types.Request
	headRequest unsafe.Pointer
	peekGrant   chan struct{}
	peeking     sync.WaitGroup
	closed      uint32
	done        chan struct{}
}

func NewConnection(cn net.Conn) *Connection {
	conn := &Connection{
		Conn:      cn,
		log:       defaultConnectionLog,
		chanWait:  make(chan *types.Request, 1),
		peekGrant: make(chan struct{}, 1),
		closed:    ConnectionOpen,
		done:      make(chan struct{}),
	}
	if v := readerPool.Get(); v != nil {
		rd := v.(resp.ResponseReader)
		rd.Reset(cn)
		conn.r = rd
	} else {
		conn.r = resp.NewResponseReader(cn)
	}
	if v := writerPool.Get(); v != nil {
		wr := v.(*resp.RequestWriter)
		wr.Reset(cn)
		conn.w = wr
	} else {
		conn.w = resp.NewRequestWriter(cn)
	}
	defaultConnectionLog.Level = global.Log.GetLevel()
	conn.peekGrant <- struct{}{} // Granted for the first pong.
	return conn
}

func (conn *Connection) String() string {
	if conn.control {
		return fmt.Sprintf("%d%s", conn.workerId, "c")
	} else {
		return fmt.Sprintf("%d%s-%d", conn.workerId, "d", conn.Id)
	}
}

func (conn *Connection) Writer() *resp.RequestWriter {
	return conn.w
}

func (conn *Connection) IsSameWorker(another *Connection) bool {
	if another == nil {
		return false
	}
	return conn.workerId == another.workerId
}

func (conn *Connection) BindInstance(ins *Instance) *Connection {
	if conn.instance == ins {
		return conn
	}

	conn.instance = ins
	conn.lm = ins.lm
	conn.log = ins.log
	if l, ok := conn.log.(*logger.ColorLogger); ok {
		copied := *l
		copied.Prefix = fmt.Sprintf("%s%v ", copied.Prefix, conn)
		conn.log = &copied
	}
	return conn
}

// SendPing send ping with piggyback infos.
func (conn *Connection) SendPing(payload []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.IsClosed() {
		return ErrConnectionClosed
	}

	conn.w.WriteMultiBulkSize(4)
	conn.w.WriteBulkString(protocol.CMD_PING)
	conn.w.WriteBulkString(strconv.Itoa(conn.lm.DataLinks().Len()))
	conn.w.WriteBulkString(strconv.FormatInt(time.Now().UnixNano(), 10))
	conn.w.WriteBulk(payload)
	conn.SetWriteDeadline(time.Now().Add(DefaultConnectTimeout))
	defer conn.SetWriteDeadline(time.Time{})
	err := conn.w.Flush()
	if err != nil {
		conn.log.Warn("Flush ping error: %v", err)
		conn.Close()
		return err
	}

	return nil
}

func (conn *Connection) SendControl(ctrl *types.Control) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Close check can prevent resouce to be used from releasing. However, close signal is not guranteed.
	if conn.IsClosed() {
		return ErrConnectionClosed
	}

	switch ctrl.Name() {
	case protocol.CMD_DATA:
		ctrl.PrepareForData(conn)
	case protocol.CMD_MIGRATE:
		ctrl.PrepareForMigrate(conn)
	case protocol.CMD_DEL:
		ctrl.PrepareForDel(conn)
	case protocol.CMD_RECOVER:
		ctrl.PrepareForRecover(conn)
	default:
		conn.log.Error("Unexpected control command: %s", ctrl)
		return ErrUnexpectedCommand
	}

	if err := ctrl.Flush(); err != nil {
		conn.log.Error("Flush control error: %v - %v", ctrl, err)
		conn.Close()
		return err
	}

	return nil
}

func (conn *Connection) SendRequest(req *types.Request, args ...interface{}) error {
	var lm *LinkManager
	useDataLink := false
	if len(args) > 1 {
		useDataLink, _ = args[0].(bool)
		lm, _ = args[1].(*LinkManager)
	}

	if lm == nil {
		if conn.control {
			conn.log.Warn("Unexpectly calling SendRequest as a data link, ignore.")
			return ErrUnexpectedSendRequest
		}
		conn.log.Debug("Sending %v(wait: %d)", req, len(conn.chanWait))
		select {
		case conn.chanWait <- req:
			if conn.IsClosed() {
				return ErrConnectionClosed
			}
			go conn.sendRequest(req)
			return nil
		case <-conn.done: // Final defense, bounce back to instance and retry.
			return ErrConnectionClosed
		}
	}

	// For control link, this should work even control link is closed.
	// Note: We'll not try to use control as fallback on datalink timeout. Control link should never be overloaded, it will be closed by the function worker if PING timeout.
	al := lm.GetAvailableForRequest()
	al.SetTimeout(DefaultConnectTimeout)
	if useDataLink {
		conn.log.Debug("Waiting for available data link to %v", req)
		// To avoid multi-source(called from different control link) block, we check both request consumption and link close.
		select {
		case al.Request() <- req:
		case <-al.Closed():
		}
		return al.Error()
	} else {
		conn.log.Debug("Waiting for available link to %v (ctrlwait: %d)", req, len(conn.chanWait))
		select {
		case conn.chanWait <- req:
			al.Close()
			if conn.IsClosed() {
				return ErrConnectionClosed
			}
			go conn.sendRequest(req)
			return nil
		case al.Request() <- req:
			return al.Error()
		case <-al.Closed():
			// Two cases here. All will lead to abandon current attempt.
			// Reset: the function worker has returned.
			// Timeout: which also applies to control.
			return al.Error()
		case <-conn.done:
			// If control is closed before got a datalink, simple abandon and make another try after revalidation.
			al.Close()
			return ErrConnectionClosed
		}
	}
}

func (conn *Connection) sendRequest(req *types.Request) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	ins := conn.instance // Save a reference in case the connection being released later.
	// Clear busy status unless waitResponse is set.
	var responded promise.Promise
	var waitTimeout = false
	defer func() {
		if !waitTimeout {
			conn.doneRequest(ins, req, responded)
		}
	}()

	// Close check can prevent resource to be used from releasing. However, close signal is not guranteed.
	if conn.IsClosed() {
		retries := req.MarkError(ErrConnectionClosed)
		conn.log.Warn("Unexpected closed connection when sending request: %v, %d retries left.", req, retries)
		if retries > 0 {
			ins.mustDispatch(req)
		} else {
			req.SetErrorResponse(ErrConnectionClosed)
		}
		return
	}

	// In case there is a request already, wait to be consumed (for response).
	// Lock free: request sent after headRequest get set.
	// Moved to SendRequest start
	// conn.log.Debug("Waiting for sending %v(wait: %d)", req, len(conn.chanWait))
	// conn.chanWait <- req
	// Moved end
	conn.prePushRequest(req)
	// Abandon if the request is already responded. However, if persist chunk is available,
	// all chunks can benifit later requests, and we will not abandon it.
	if !req.Validate(true) {
		conn.popRequest(req)
		conn.log.Debug("Abandon requesting %v: responded.", &req.Id)
		return
	}

	responded = req.InitPromise()
	switch req.Name() {
	case protocol.CMD_SET:
		req.PrepareForSet(conn)
		if req.PersistChunk != nil {
			req.PersistChunk.StartPersist(req, protocol.PersistTimeout, ins.retryPersist)
		}
	case protocol.CMD_GET:
		req.PrepareForGet(conn)
	case protocol.CMD_DEL:
		req.PrepareForDel(conn)
	case protocol.CMD_RECOVER:
		req.PrepareForRecover(conn)
	default:
		conn.setErrorResponse(fmt.Errorf("unexpected request command: %s", req))
		// Unrecoverable
		return
	}

	// Updated by Tianium: 20210523
	// Write timeout is added back. See comments in req.Flush()
	if err := req.Flush(); err != nil {
		conn.log.Warn("Flush request error: %v - %v", req, err)
		ins.ResetDue(false, "connection error on sending request")
		if req.MarkError(err) > 0 {
			ins.mustDispatch(req)
		} else {
			conn.setErrorResponse(err)
		}
		conn.Close()
		return
	} else if !conn.control {
		// Only data link need to wait for response.
		conn.peekGrant <- struct{}{}
	}

	ins.SetDue(time.Now().Add(protocol.HeaderTimeout).UnixNano(), false, "granting extension for serving request") // Set long due until response received.

	waitTimeout = true
	go func() {
		// The request has been send, call doneBusy after response set.
		defer conn.doneRequest(ins, req, responded)

		// Wait for response or timeout.
		// Noted that even the client-request can be responeded, Timeout() will wait for lambda-request.
		_, err := req.Timeout()
		if err == nil {
			rsp, _ := responded.Value().(*types.Response)
			if rsp == nil {
				// If error, we are done.
				return
			} else if rsp.IsAbandon() {
				conn.log.Debug("Abandoned %v", &rsp.Id)
				if conn.control {
					// Consume the request in the queue, unblock queue, and wait for the next request.
					conn.setResponse(rsp)
				} else {
					// If a request is abandoned, the response must be set.
					// If abandoned before receiving response from Lambda, the rsp is generated in types.Request and
					// Wait() will return immiediately.
					// To keep reusing and avoid disconnection the connection (due to Lambda open files limit),
					// we wait for the response by trying inserting a new dummy request to the wait queue. Other reasons are:
					// 1: The response are this link has already been late, keep pushing request on this link will only make next request worse.
					// 2: By keep instance busy, we encourage other requests to find a better instance.
					select {
					case conn.chanWait <- &types.Request{}:
						// Once the queue is free (because we can inserted into it), consume it to allow next request.
						// This approach can be only used for data link, because no request will be sent on data link before it is returned to the link manager.
						<-conn.chanWait // This is equivalent to rsp.Wait()
					// Guarded by the same timeout for original request. It is avaiable after we call the req.Timeout() before.
					case <-time.After(req.ResponseTimeout()):
						conn.CloseWithReason("timeout on waiting for response of abandoned request", false)
					}
				}
			} else {
				// Wait for response to finalize or connection to close.
				rsp.Wait()
			}
			return
		}

		if resErr := conn.setErrorResponse(err); resErr == nil {
			conn.log.Warn("Request timeout: %v", req)
		} else if resErr != types.ErrResponded && resErr != ErrMissingRequest {
			conn.log.Warn("Request timeout: %v, error: %v", req, resErr)
		}
		// If req is responded, err has been reported somewhere.

		// close connection to discard late response.
		conn.Close()
	}()
}

func (conn *Connection) prePushRequest(req *types.Request) bool {
	return atomic.CompareAndSwapPointer(&conn.headRequest, nil, unsafe.Pointer(req))
}

func (conn *Connection) peekRequest() *types.Request {
	ptr := atomic.LoadPointer(&conn.headRequest)
	if ptr == nil {
		return nil
	} else {
		return (*types.Request)(ptr)
	}
}

// popRequest pop the head of channel lock freely by:
// 1. Set head before queuing.
// 2. Test head using peekRequest.
// 3. Clear head before dequeuing.
// So if the queue channel is unlocked, head must be nil.
// If peekRequest test (2) is passed, and popRequest is guarded by atomic operation and can't pop twice.
func (conn *Connection) popRequest(req *types.Request) *types.Request {
	if atomic.CompareAndSwapPointer(&conn.headRequest, unsafe.Pointer(req), nil) {
		select {
		case req := <-conn.chanWait:
			return req
		default:
			return nil
		}
	}
	return nil
}

// loadRequest returns the last request if it is available.
// loadRequest will fail if the last request has been responded and removed.
func (conn *Connection) loadRequest(rsp *types.Response) (*types.Request, error) {
	// Last request can be responded, either bacause error or timeout, which causes nil or unmatch
	req := conn.peekRequest()
	if req == nil || !req.IsResponse(rsp) {
		// ignore
		conn.log.Debug("response discarded: %v", rsp)
		return nil, ErrMissingRequest
	}

	return req, nil
}

// doneRequest resets instance and connection status after a request.
func (conn *Connection) doneRequest(ins *Instance, req *types.Request, responded promise.Promise) {
	ins.doneBusy(req)

	// Nil response suggests an error without proper response. Error during tranmission response will be handled differently.
	if req.PersistChunk != nil && !req.PersistChunk.IsStored() && !req.PersistChunk.IsClosed() {
		conn.log.Warn("Detected unfulfilled persist chunk during %v, predicted due in: %v", req, req.PredictedDue)
		// ASSERION: responded is not nil.
		var err error
		if responded != nil {
			err, _ = responded.Value().(error)
		}
		if err != nil {
			req.PersistChunk.CloseWithError(err)
		} else if conn.IsClosed() {
			// If responded is abandoned, the chunk can failed to store due to connection closed. See history logs why the reason is closed.
			req.PersistChunk.CloseWithError(ErrConnectionClosed)
		} else {
			req.PersistChunk.CloseWithError(types.ErrUnexpectedClose)
		}
	}
	// if responded != nil {
	// 	promise.Recycle(responded)
	// }

	lm := ins.lm
	if !conn.control && lm != nil && !conn.IsClosed() {
		lm.FlagAvailableForRequest(conn)
	}
}

// func (conn *Connection) isRequestPending() bool {
// 	return len(conn.chanWait) > 0
// }

func (conn *Connection) IsClosed() bool {
	return atomic.LoadUint32(&conn.closed) != ConnectionOpen
}

func (conn *Connection) Close() error {
	// Don't block. Close() is called in goroutine.
	return conn.CloseWithReason("", false)
}

func (conn *Connection) CloseAndWait() error {
	// Don't block. Close() is called in goroutine.
	return conn.CloseWithReason("", true)
}

// Close Signal connection should be closed. Function close() will be called later for actural operation
func (conn *Connection) CloseWithReason(reason string, block bool) error {
	if !atomic.CompareAndSwapUint32(&conn.closed, ConnectionOpen, ConnectionClosing) {
		return ErrConnectionClosed
	}

	select {
	case <-conn.done:
	default:
		close(conn.done)
	}

	// Signal closed only. This allow ongoing transmission to finish.
	if reason != "" {
		conn.log.Warn("Signal to close:%s.", reason)
	} else {
		conn.log.Debug("Signal to close.")
	}

	closeConn := func(check bool) {
		// Wait for peeking to unblock.
		// On peeking, we block and read the connection, which can only be unblocked from the other side.
		// Closing from this side will only make the zombie connection.
		// We can simple wait given the following assumption:
		// 1. For control link, heartbeat will trigger the response.
		// 2. For data link, a request is sent and waiting for response.
		// 3. On lambda return, the other side close the connection.
		// NOTE: It could leads to forever wait if we try to close the connection from our side without sending any request.
		if check {
			go conn.warnAfter(time.Second, conn.isClosed, "Failed to close.")
		}
		conn.peeking.Wait()

		// Disconnect connection to trigger close()
		// Don't use conn.Conn.Close(), it will stuck and wait for lambda.
		// 1. If lambda is running, lambda will close the connection.
		//    When we close it here, the connection has been closed.
		// 2. In current implementation, the instance will close the connection
		///   if the lambda will not respond to the link any more.
		//    For data link, it is when the lambda has returned.
		//    For ctrl link, it is likely that the system is shutting down.
		// 3. If we know lambda is active, close conn.Conn first.
		if tcp, ok := conn.Conn.(*net.TCPConn); ok {
			tcp.SetLinger(0) // The operating system discards any unsent or unacknowledged data.
		}
		// conn.Conn.SetDeadline(time.Now().Add(-time.Second)) // Force timeout to stop blocking read.
		conn.Conn.Close()

		if conn.control && conn.instance != nil {
			conn.instance.ResetDue(false, "control connection closed")
		}
	}

	if block {
		closeConn(false)
	} else {
		go closeConn(true)
	}

	return nil
}

// close must be called from ServeLambda()
func (conn *Connection) close() {
	// Remove from link manager
	if conn.lm != nil {
		if conn.control {
			conn.lm.InvalidateControl(conn)
		} else {
			conn.lm.RemoveDataLink(conn)
		}
	}

	// Putting close in mutex can prevent resouces used by ongoing transactions being released.
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Call signal function to avoid duplicated close.
	conn.CloseAndWait()

	// Clear pending requests after TCP connection closed, so current request got chance to return first.
	conn.ClearResponses()

	var w, r interface{}
	conn.w, w = nil, conn.w
	conn.r, r = nil, conn.r
	// Ensure peeking get unblocked.
	conn.peeking.Wait()
	readerPool.Put(r)
	writerPool.Put(w)
	conn.lm = nil
	// Don't reset instance to nil, we may need it to resend request.

	atomic.CompareAndSwapUint32(&conn.closed, ConnectionClosing, ConnectionClosed)
	conn.log.Debug("Closed.")
}

// blocking on lambda peek Type
// lambda handle incoming lambda store response
//
// field 0 : conn id
// field 1 : req id
// field 2 : chunk id
// field 3 : obj val
func (conn *Connection) ServeLambda() {
	chRsp := make(chan interface{})
	for {
		go conn.peekResponse(chRsp)

		var retPeek interface{}
		select {
		case <-conn.done:
			conn.close()
			return
		case retPeek = <-chRsp:
			// Double check	if connection is closed.
			select {
			case <-conn.done:
				conn.close()
				return
			default:
			}
		}

		// Got response, reset read deadline.
		conn.Conn.SetReadDeadline(time.Time{})

		var respType resp.ResponseType
		switch ret := retPeek.(type) {
		case error:
			if util.IsConnectionFailed(ret) {
				if conn.control {
					conn.log.Debug("Lambda store disconnected.")
				} else {
					conn.log.Debug("Disconnected.")
				}
			} else {
				conn.log.Warn("Failed to peek response type: %s", ret.Error())
			}
			conn.close()
			return
		case resp.ResponseType:
			respType = ret
		}

		var readErr error
		start := time.Now()
		switch respType {
		case resp.TypeError:
			strErr, err := conn.r.ReadError()
			if err != nil {
				readErr = err
				break
			}
			err = fmt.Errorf("response error: %s", strErr)
			conn.log.Warn("%v", err)
			conn.setErrorResponse(err)
		case resp.TypeBulk:
			cmd, err := conn.r.ReadBulkString()
			if err != nil {
				readErr = err
				break
			}

			if cmd == protocol.CMD_PONG {
				conn.pongHandler()
				break
			} else if conn.instance == nil {
				// all other commands require instance association, or could be a rouge request.
				conn.log.Warn("Rouge request, disconnect")
				conn.close()
				return
			}

			switch cmd {
			case protocol.CMD_RECOVERED:
				conn.recoveredHandler()
			case protocol.CMD_GET:
				conn.getHandler(start)
			case protocol.CMD_SET:
				conn.setHandler(start)
			case protocol.CMD_PERSISTED:
				conn.persistedHandler(true)
			case protocol.CMD_PERSIST_FAILED:
				conn.persistedHandler(false)
			case protocol.CMD_DEL:
				conn.delHandler()
			case protocol.CMD_DATA:
				conn.receiveData()
			case protocol.CMD_INITMIGRATE:
				go conn.initMigrateHandler()
			case protocol.CMD_BYE:
				conn.bye()
			case protocol.CMD_RECOVER:
				conn.recoverHandler()
			default:
				conn.log.Warn("Unsupported response type: %s", logger.SafeString(cmd, 20))
			}
		default:
			if err := conn.skipField(respType); err != nil {
				readErr = err
			}
		}

		if readErr != nil {
			if !util.IsConnectionFailed(readErr) {
				conn.log.Warn("Error on handle response %s: %v", respType, readErr)
			} else if conn.control {
				conn.log.Warn("Lambda store disconnected.")
			} else {
				conn.log.Debug("Disconnected.")
			}
			conn.close()
			return
		}

		if conn.instance != nil {
			conn.instance.flagWarmed()
		}
	}
}

func (conn *Connection) skipField(t resp.ResponseType) error {
	if t == resp.TypeUnknown {
		var err error
		t, err = conn.r.PeekType()
		if err != nil {
			return err
		}
	}

	switch t {
	case resp.TypeArray:
		if arrLen, err := conn.r.ReadArrayLen(); err != nil {
			return err
		} else {
			conn.log.Warn("Skipping %s of len %d", t, arrLen)
			for i := 0; i < arrLen; i++ {
				if err := conn.skipField(resp.TypeUnknown); err != nil {
					return err
				}
			}
		}
	case resp.TypeBulk:
		conn.log.Warn("Skipping %s", t)
		if err := conn.r.SkipBulk(); err != nil {
			return err
		}
	case resp.TypeUnknown:
		return ErrUnexpectedType
	default:
		conn.log.Warn("Skipping %s", t)
		// We just want read a line, ignore other error
		if _, err := conn.r.ReadInlineString(); err != nil && !resp.IsProtocolError(err) {
			return err
		}
	}
	return nil
}

func (conn *Connection) ClearResponses() {
	for req := conn.peekRequest(); req != nil; req = conn.peekRequest() {
		if conn.popRequest(req) == req {
			req.SetErrorResponse(ErrConnectionClosed)
		}
		// Yield for pending req a chance to push.
		runtime.Gosched()
	}
}

func (conn *Connection) peekResponse(chRsp chan interface{}) {
	conn.peeking.Add(1)
	defer conn.peeking.Done()

	// Wait for granted
	// For control connection, the peekGrant is closed and always granted.
	// For data connection, the peekGrant is granted after a request is made.
	select {
	case <-conn.peekGrant:
	case <-conn.done:
		return
	}

	// Start peeking
	r := conn.r
	if r == nil {
		return
	}
	var ret interface{}
	ret, err := r.PeekType()
	if err != nil {
		ret = err
	}
	select {
	case chRsp <- ret:
	default:
		// No consumer. The connection must be closed, abandon.
	}
}

// setResponse Set response for last request.
func (conn *Connection) setResponse(rsp *types.Response) (*types.Request, error) {
	req, err := conn.loadRequest(rsp)
	if err != nil {
		return nil, err
	}

	// Lock free: double check that poped is what we peeked. Poped will be either req or nil.
	if poped := conn.popRequest(req); poped == req {
		conn.log.Debug("response matched: %v", req)
		return req, req.SetResponse(rsp)
	}

	return nil, ErrMissingRequest
}

// setErrorResponse Set response to last request as a error.
// You may need to flag the connection as available manually depends on the error.
func (conn *Connection) setErrorResponse(err error) error {
	// Last request can be responded, either bacause error or timeout, which causes nil or unmatch
	req := conn.peekRequest()
	if req != nil && conn.popRequest(req) == req {
		return req.SetErrorResponse(err)
	}

	return ErrMissingRequest
}

func (conn *Connection) closeIfError(prompt string, err error) bool {
	if err != nil {
		conn.log.Warn(prompt, err)
		conn.CloseAndWait() // Ensure closed and safe to wait for the close in a handler.
		return true
	}

	return false
}

func (conn *Connection) closeStream(stream io.Closer, prompt string, id *types.Id) {
	// To keep the connection being resued (due to limited Lambda open files), avoid close the connnection.
	if err := stream.Close(); err != nil && !conn.IsClosed() {
		conn.log.Debug("%s %v: %v", prompt, id, err)
		conn.CloseAndWait() // Ensure closed and safe to wait for the close in a handler.
	}
}

func (conn *Connection) pongHandler() {
	// Read lambdaId, if it is negatvie, we need a parallel recovery.
	id, err := conn.r.ReadInt()
	if conn.closeIfError("Discard rouge PONG for missing store id: %v.", err) {
		return
	}
	storeId := id & 0xFFFF
	conn.workerId = int32(id >> 32)

	sid, err := conn.r.ReadBulkString()
	if conn.closeIfError("Discard rouge PONG for missing session id: %v.", err) {
		return
	}

	flags, err := conn.r.ReadInt()
	if conn.closeIfError("Discard rouge PONG for missing flags: %v.", err) {
		return
	}
	conn.control = flags&protocol.PONG_FOR_CTRL > 0

	var payload []byte
	if flags&protocol.PONG_WITH_PAYLOAD > 0 {
		reader, err := conn.r.StreamBulk()
		if conn.closeIfError("Failed to read payload: %v.", err) {
			return
		}
		payload, err = reader.ReadAll()
		if conn.closeIfError("Failed to read payload: %v.", err) {
			return
		}
	}

	conn.log.Debug("PONG from lambda(%d,flag:%d).", storeId, flags)
	instance := conn.instance
	if instance == nil {
		// Look up lambda instance
		instance = CM.Instance(uint64(storeId))
	}
	if instance == nil {
		conn.log.Error("Failed to match lambda: %d", storeId)
		return
	}

	validated, _, err := instance.TryFlagValidated(conn, sid, flags)
	if err != nil && err != ErrNotCtrlLink && err != ErrInstanceValidated {
		conn.log.Warn("Discard rouge PONG(%v) for %d, current %v", conn, storeId, validated)
		conn.Conn.Close() // Close connection normally, so lambda will close itself.
		conn.Close()
	}
	// Disable peek granting.
	if conn.control {
		select {
		case <-conn.peekGrant:
		default:
			close(conn.peekGrant)
		}
	}

	conn.log.Debug("PONG from lambda confirmed.")
	conn.piggybackHandler(flags, payload)
}

func (conn *Connection) recoveredHandler() {
	conn.log.Debug("RECOVERED from lambda.")

	if conn.instance == nil {
		conn.log.Error("No instance set on recovered")
		return
	}

	conn.instance.ResumeServing()
}

func (conn *Connection) getHandler(start time.Time) {
	conn.log.Debug("GET from lambda.")

	// Exhaust all values to keep protocol aligned.
	reqId, _ := conn.r.ReadBulkString()
	chunkId, _ := conn.r.ReadBulkString()
	recovered, _ := conn.r.ReadInt()
	stream, err := conn.r.StreamBulk()
	if err != nil {
		if conn.IsClosed() {
			// The error should be logged by somewhere that close the connection.
			return
		}
		conn.setErrorResponse(err)
		conn.log.Warn("Failed to get body reader of response: %v", err)
		conn.CloseAndWait() // Ensure closed and safe to wait for the close in a handler.
		return
	}
	conn.instance.SetDue(protocol.GetBodyDeadline(stream.Len()).UnixNano(), false, "granting extension for transfering chunk")

	rsp := types.NewResponse(protocol.CMD_GET)
	rsp.Id.ReqId = reqId
	rsp.Id.ChunkId = chunkId
	rsp.Status = recovered
	chunk, _ := strconv.Atoi(chunkId)

	// Find request first
	req, err := conn.loadRequest(rsp)
	if err != nil {
		// Its too late. the request must have been timeout. Discard the response.
		conn.closeStream(stream, "Failed to skip bulk on discard", &rsp.Id)
		conn.finalizeCommmand(rsp.Cmd)
		rsp.Close()
		return
	}

	// Send ack and pop request.
	rsp.OnFinalize(conn.getPopRequestFinalizer(req))
	rsp.OnFinalize(conn.getCommandFinalizer(rsp.Cmd))
	// Set rsp as closed so it can be released safely.
	defer rsp.Close()

	// Store stream if persist chunk is avaiable
	if req.PersistChunk != nil {
		newStream, err := req.PersistChunk.Store(stream)
		if err != nil {
			conn.log.Error("Unexpected error on caching chunk %v: %v", &rsp.Id, err)
			req.PersistChunk.CloseWithError(err)
		} else {
			stream = newStream
		}
	}

	counter, _ := global.ReqCoordinator.Load(reqId).(*global.RequestCounter)
	if counter == nil {
		conn.log.Debug("Request not found: %v, can be fulfilled already.", &rsp.Id)
		// Set response and exhaust value
		req.SetResponse(rsp)
		conn.closeStream(stream, "Failed to skip bulk on request mismatch", &rsp.Id)
		return
	}
	defer counter.Close()

	status, returned := counter.AddSucceeded(chunk, recovered == 1)
	if returned || counter.IsLate(status) {
		conn.log.Debug("GOT %v, abandon (duplicated: %v)", &rsp.Id, returned)
		// Most likely, the req has been abandoned already. But we still need to consume the connection side req.
		// Connection will not be flagged available until SkipBulk() is executed.
		req.SetResponse(rsp)
		if !returned {
			_, err := collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), recovered)
			if err != nil {
				conn.log.Warn("LogRequestFuncResponse err %v", err)
			}
		}

		// Consume and abandon the response.
		conn.closeStream(stream, "Failed to skip bulk on abandon", &rsp.Id)
		return
	}

	conn.log.Debug("GOT %v, confirmed. size:%d", &rsp.Id, stream.Len())

	// Finish the response by setting the BodyStream
	rsp.SetBodyStream(stream)
	stream.(resp.Holdable).Hold()

	// Connection will not be flagged available until BodyStream is closed.
	if err := req.SetResponse(rsp); err != nil {
		// Failed to set response, release hold.
		stream.(resp.Holdable).Unhold()
		conn.closeStream(stream, "Failed to skip bulk after setResponse", &rsp.Id)
		return
	} else {
		collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), recovered)
	}
	// Abandon rest chunks in case of the proxy first-d optimization.
	if counter.IsFulfilled(status) && !counter.IsAllReturned() { // IsAllReturned will load updated status.
		conn.log.Debug("Request fulfilled: %v, abandon unresponded chunks.", &rsp.Id)
		for _, req := range counter.Requests {
			if req != nil && !req.IsReturnd() {
				// For returned requests, it can be faster one or late one, their connection will decide.
				req.Abandon()
			}
		}
	}

	// Close will block until the stream is flushed.
	if err := rsp.WaitFlush(true); err != nil {
		if err != context.Canceled {
			conn.log.Warn("Failed to stream %v: %v", &rsp.Id, err)
		} else {
			conn.log.Debug("Abandoned streaming %v", &rsp.Id)
		}
	} else {
		conn.log.Debug("Flushed streaming %v", &rsp.Id)
		status := counter.AddFlushed(chunk)
		// conn.log.Debug("Flushed %v. Fulfilled: %v(%x), %v", rsp.Id, counter.IsFulfilled(status), status, counter.IsAllFlushed(status))
		// Abandon rest chunks.
		if counter.IsFulfilled(status) && !counter.IsAllFlushed(status) {
			conn.log.Debug("Request fulfilled: %v, abandon unflushed chunks.", &rsp.Id)
			for _, req := range counter.Requests {
				if req != nil {
					req.Abandon()
				}
			}
		}
	}
}

func (conn *Connection) setHandler(start time.Time) {
	conn.log.Debug("SET from lambda.")

	rsp := types.NewResponse(protocol.CMD_SET)
	rsp.Body = []byte(strconv.FormatUint(conn.instance.Id(), 10))
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()
	chunk, _ := strconv.Atoi(rsp.Id.ChunkId)
	// Send ack
	conn.finalizeCommmand(rsp.Cmd)
	// Close response
	defer rsp.Close()

	counter, _ := global.ReqCoordinator.Load(rsp.Id.ReqId).(*global.RequestCounter)
	// There are two cases that the request is not found:
	// 1. The request timeouts.
	// 2. This is a repeated SET for persistence retrial (See cache/persist_chunk:Store()).
	if counter == nil {
		// conn.log.Warn("Request not found: %s, can be canceled already", rsp.Id.ReqId)
		// Set response
		conn.setResponse(rsp)
		return
	}
	defer counter.Close()

	conn.log.Debug("SET %v, confirmed.", &rsp.Id)
	// Added by Tianium 2022-11-02
	// Flag chunk set as succeeded.
	counter.AddSucceeded(chunk, false)
	req, err := conn.setResponse(rsp)
	if err == nil {
		collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), int64(0))
	}
}

func (conn *Connection) persistedHandler(success bool) {
	conn.log.Debug("Persisted from lambda: %v.", success)

	// Exhaust all values to keep protocol aligned.
	key, _ := conn.r.ReadBulkString()
	persistChunk := CM.GetPersistCache().Get(key)
	if persistChunk == nil {
		conn.log.Warn("Persisted chunk not found: %s.", key)
		return
	}

	if success {
		persistChunk.DonePersist()
		return
	}

	// Trigger retry.
	conn.instance.retryPersist(persistChunk)
}

func (conn *Connection) delHandler() {
	conn.log.Debug("DEL from lambda.")

	rsp := types.NewResponse(protocol.CMD_DEL)
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()

	conn.log.Debug("DEL %v, confirmed.", &rsp.Id)

	// Send ack.
	conn.finalizeCommmand(rsp.Cmd)
	// Pop request and send response.
	conn.setResponse(rsp)
	// All done.
	rsp.Close()
}

func (conn *Connection) receiveData() {
	conn.log.Debug("DATA from lambda.")

	ok, err := conn.r.ReadBulkString()
	if err != nil {
		conn.log.Error("Error on processing result of data collection: %v", err)
	}
	conn.instance.FlagDataCollected(ok)
}

func (conn *Connection) initMigrateHandler() {
	conn.log.Debug("Request to initiate migration")

	conn.instance.Migrate()
}

func (conn *Connection) bye() {
	conn.log.Debug("BYE from lambda.")
	if conn.instance != nil {
		client := conn.instance.client
		if client != nil {
			if local, ok := client.(*invoker.LocalInvoker); ok {
				sid, _ := conn.r.ReadBulkString()
				payload, _ := conn.r.ReadBulkString()
				local.SetOutputPayload(sid, []byte(payload))
			}
		}
		conn.instance.bye(conn)
	}
}

func (conn *Connection) recoverHandler() {
	conn.log.Debug("RECOVER from lambda.")

	conn.r.ReadBulkString() // reqId
	conn.r.ReadBulkString() // chunkId

	// Ack lambda if it is supported
	conn.finalizeCommmand(protocol.CMD_RECOVER)
}

func (conn *Connection) piggybackHandler(flags int64, payload []byte) error {
	if flags&protocol.PONG_WITH_PAYLOAD == 0 {
		return nil
	}

	// Try read payload if not read yet.
	if payload == nil {
		reader, err := conn.r.StreamBulk()
		if conn.closeIfError("Failed to read payload: %v.", err) {
			return err
		}
		payload, err = reader.ReadAll()
		if conn.closeIfError("Failed to read payload: %v.", err) {
			return err
		}
	}

	instance := conn.instance
	if flags&protocol.PONG_RECONCILE > 0 {
		var shortMeta protocol.ShortMeta
		if err := binary.Unmarshal(payload, &shortMeta); err != nil {
			conn.log.Warn("Invalid meta on reconciling: %v", err)
		} else if instance != nil {
			instance.reconcileStatus(conn, &shortMeta)
		}
	}
	return nil
}

func (conn *Connection) readAndSetDue(ins *Instance) error {
	due, err := conn.r.ReadInt()
	if conn.closeIfError("Failed to read due: %v.", err) {
		// Reset possible optimistic estimation.
		ins.ResetDue(true, "failing to read due feedback")
		return err
	}
	ins.SetDue(due, true, "updating due feedback") // As long as any connection is busy, delay the update of due.

	flags, err := conn.r.ReadInt()
	if conn.closeIfError("Failed to read piggyback flags: %v.", err) {
		return err
	}
	conn.piggybackHandler(flags, nil)

	return nil
}

func (conn *Connection) ackCommand(cmd string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.w.WriteCmd(protocol.CMD_ACK)
	if err := conn.w.Flush(); err != nil {
		conn.log.Warn("Failed to acknowledge %s: %v", cmd, err)
		conn.Close()
		return err
	}
	return nil
}

func (conn *Connection) finalizeCommmand(cmd string) error {
	if conn.IsClosed() {
		conn.instance.ResetDue(true, "connection closed before reading due feedback")
		return ErrConnectionClosed
	} else if err := conn.readAndSetDue(conn.instance); err != nil {
		// The connection will be closed if error occurs.
		return err
	} else if err = conn.ackCommand(cmd); err != nil {
		// The connection will be closed if error occurs.
		return err
	}

	return nil
}

func (conn *Connection) getCommandFinalizer(cmd string) types.ResponseFinalizer {
	return func(_ *types.Response) {
		conn.finalizeCommmand(cmd)
	}
}

func (conn *Connection) getPopRequestFinalizer(req *types.Request) types.ResponseFinalizer {
	return func(_ *types.Response) {
		conn.popRequest(req)
	}
}

func (conn *Connection) warnAfter(timeout time.Duration, tester func() bool, format string, args ...interface{}) {
	<-time.After(timeout)
	if !tester() {
		conn.log.Warn(format, args...)
	}
}

func (conn *Connection) isClosed() bool {
	return atomic.LoadUint32(&conn.closed) == ConnectionClosed
}
