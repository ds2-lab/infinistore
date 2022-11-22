package lambdastore

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/lambda/invoker"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/redeo/resp"
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
	respType    chan interface{}
	closed      uint32
	done        chan struct{}
}

func NewConnection(cn net.Conn) *Connection {
	conn := &Connection{
		Conn:     cn,
		log:      defaultConnectionLog,
		chanWait: make(chan *types.Request, 1),
		respType: make(chan interface{}),
		done:     make(chan struct{}),
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
	waitResponse := false
	defer func() {
		if !waitResponse {
			conn.doneRequest(ins, req)
		}
	}()

	// Close check can prevent resource to be used from releasing. However, close signal is not guranteed.
	if conn.IsClosed() {
		if req.MarkError(ErrConnectionClosed) > 0 {
			ins.chanPriorCmd <- req
		} else {
			req.SetResponse(ErrConnectionClosed)
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
	if req.IsResponded() {
		conn.popRequest(req)
		return
	}

	switch req.Name() {
	case protocol.CMD_SET:
		req.PrepareForSet(conn)
	case protocol.CMD_GET:
		req.PrepareForGet(conn)
	case protocol.CMD_DEL:
		req.PrepareForDel(conn)
	case protocol.CMD_RECOVER:
		req.PrepareForRecover(conn)
	default:
		conn.SetErrorResponse(fmt.Errorf("unexpected request command: %s", req))
		// Unrecoverable
		return
	}

	// Updated by Tianium: 20210523
	// Write timeout is added back. See comments in req.Flush()
	if err := req.Flush(); err != nil {
		conn.log.Warn("Flush request error: %v - %v", req, err)
		if req.MarkError(err) > 0 {
			conn.instance.chanPriorCmd <- req
		} else {
			conn.SetErrorResponse(err)
		}
		conn.Close()
		return
	}

	conn.log.Debug("Sent %v", req)

	waitResponse = true
	go func() {
		// The request has been send, call doneBusy after response set.
		defer conn.doneRequest(ins, req)

		// Wait for response or timeout.
		err := req.Timeout()
		if err == nil {
			rsp := req.Response()
			if rsp != nil {
				// Wait for response to finalize.
				rsp.Wait()
			}
			return
		}

		if resErr := conn.SetErrorResponse(err); resErr == nil {
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

// doneRequest resets instance and connection status after a request.
func (conn *Connection) doneRequest(ins *Instance, req *types.Request) {
	ins.doneBusy(req)

	lm := ins.lm
	if !conn.control && lm != nil && !conn.IsClosed() {
		lm.FlagAvailableForRequest(conn)
	}
}

// func (conn *Connection) isRequestPending() bool {
// 	return len(conn.chanWait) > 0
// }

func (conn *Connection) IsClosed() bool {
	return atomic.LoadUint32(&conn.closed) == 1
}

// Close Signal connection should be closed. Function close() will be called later for actural operation
func (conn *Connection) Close() error {
	if !atomic.CompareAndSwapUint32(&conn.closed, 0, 1) {
		return ErrConnectionClosed
	}

	// Signal closed only. This allow ongoing transmission to finish.
	conn.log.Debug("Signal to close.")
	select {
	case <-conn.done:
	default:
		close(conn.done)
	}

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
	conn.Conn.Close()

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
	conn.Close()

	// Clear pending requests after TCP connection closed, so current request got chance to return first.
	conn.ClearResponses()

	var w, r interface{}
	conn.w, w = nil, conn.w
	conn.r, r = nil, conn.r
	readerPool.Put(r)
	writerPool.Put(w)
	conn.lm = nil
	// Don't reset instance to nil, we may need it to resend request.

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
	for {
		go conn.peekResponse()

		var retPeek interface{}
		select {
		case <-conn.done:
			conn.close()
			return
		case retPeek = <-conn.respType:
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
			conn.SetErrorResponse(err)
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

// SetResponse Set response for last request.
// If parameter release is set and no error, the connection will be flagged available.
func (conn *Connection) SetResponse(rsp *types.Response) (*types.Request, error) {
	// Last request can be responded, either bacause error or timeout, which causes nil or unmatch
	req := conn.peekRequest()
	if req == nil || !req.IsResponse(rsp) {
		// ignore
		conn.log.Debug("response discarded: %v", rsp)
		return nil, ErrMissingRequest
	}

	// Lock free: double check that poped is what we peeked. Poped will be either req or nil.
	if poped := conn.popRequest(req); poped == req {
		conn.log.Debug("response matched: %v", req)
		return req, req.SetResponse(rsp)
	}

	return nil, ErrMissingRequest
}

// SetErrorResponse Set response to last request as a error.
// You may need to flag the connection as available manually depends on the error.
func (conn *Connection) SetErrorResponse(err error) error {
	// Last request can be responded, either bacause error or timeout, which causes nil or unmatch
	req := conn.peekRequest()
	if req != nil && conn.popRequest(req) == req {
		return req.SetResponse(err)
	}

	return ErrMissingRequest
}

func (conn *Connection) ClearResponses() {
	for req := conn.peekRequest(); req != nil; req = conn.peekRequest() {
		if conn.popRequest(req) == req {
			req.SetResponse(ErrConnectionClosed)
		}
		// Yield for pending req a chance to push.
		runtime.Gosched()
	}
}

func (conn *Connection) peekResponse() {
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
	case conn.respType <- ret:
	default:
		// No consumer. The connection must be closed, abandon.
	}
}

func (conn *Connection) closeIfError(prompt string, err error) bool {
	if err != nil {
		conn.log.Warn(prompt, err)
		conn.Close()
		return true
	}

	return false
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

	validated, err := instance.TryFlagValidated(conn, sid, flags)
	if err != nil && err != ErrNotCtrlLink && err != ErrInstanceValidated {
		conn.log.Warn("Discard rouge PONG(%v) for %d, current %v", conn, storeId, validated)
		conn.Conn.Close() // Close connection normally, so lambda will close itself.
		conn.Close()
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
		conn.SetErrorResponse(err)
		conn.log.Warn("Failed to get body reader of response: %v", err)
		conn.Close()
		return
	}
	conn.instance.SetDue(protocol.GetBodyDeadline(stream.Len()).UnixNano())

	rsp := types.NewResponse(protocol.CMD_GET)
	rsp.Id.ReqId = reqId
	rsp.Id.ChunkId = chunkId
	rsp.Status = recovered
	chunk, _ := strconv.Atoi(chunkId)
	// Send ack after response has been send.
	rsp.OnFinalize(conn.getCommandFinalizer(rsp.Cmd))
	// Set rsp as closed so it can be released safely.
	defer rsp.Done()

	counter, _ := global.ReqCoordinator.Load(reqId).(*global.RequestCounter)
	if counter == nil {
		// conn.log.Warn("Request not found: %s, can be fulfilled already.", reqId)
		// Set response and exhaust value
		conn.SetResponse(rsp)
		if err := stream.Close(); err != nil {
			conn.Close()
			conn.log.Warn("Failed to skip bulk on request mismatch: %v", err)
		}
		return
	}
	defer counter.Close()

	status, returned := counter.AddSucceeded(chunk, recovered == 1)
	if returned || counter.IsLate(status) {
		conn.log.Debug("GOT %v, abandon (duplicated: %v)", rsp.Id, returned)
		// Most likely, the req has been abandoned already. But we still need to consume the connection side req.
		// Connection will not be flagged available until SkipBulk() is executed.
		req, _ := conn.SetResponse(rsp)
		if req != nil && !returned {
			_, err := collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), recovered)
			if err != nil {
				conn.log.Warn("LogRequestFuncResponse err %v", err)
			}
		}

		// Consume and abandon the response.
		if err := stream.Close(); err != nil {
			conn.Close()
			conn.log.Warn("Failed to skip bulk on abandon: %v", err)
		}
		return
	}

	// Finish the response by setting the BodyStream
	rsp.BodyStream = stream
	stream.(resp.Holdable).Hold()

	conn.log.Debug("GOT %v, confirmed. size:%d", rsp.Id, rsp.BodyStream.Len())
	// Connection will not be flagged available until BodyStream is closed.
	if req, err := conn.SetResponse(rsp); err != nil {
		// Failed to set response, release hold.
		stream.(resp.Holdable).Unhold()
	} else {
		collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), recovered)
	}
	// Abandon rest chunks.
	if counter.IsFulfilled(status) && !counter.IsAllReturned() { // IsAllReturned will load updated status.
		conn.log.Debug("Request fulfilled: %v, abandon rest chunks.", rsp.Id)
		for _, req := range counter.Requests {
			if req != nil && !req.IsReturnd() {
				// For returned requests, it can be faster one or late one, their connection will decide.
				req.Abandon()
			}
		}
	}

	// Close will block until the stream is flushed.
	if err := stream.Close(); err != nil {
		conn.Close()
	}
}

func (conn *Connection) setHandler(start time.Time) {
	conn.log.Debug("SET from lambda.")

	rsp := types.NewResponse(protocol.CMD_SET)
	rsp.Body = []byte(strconv.FormatUint(conn.instance.Id(), 10))
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()
	chunk, _ := strconv.Atoi(rsp.Id.ChunkId)
	// Check lambda if it is supported
	rsp.OnFinalize(conn.getCommandFinalizer(rsp.Cmd))
	defer rsp.Done()

	counter, _ := global.ReqCoordinator.Load(rsp.Id.ReqId).(*global.RequestCounter)
	if counter == nil {
		// conn.log.Warn("Request not found: %s, can be cancelled already", rsp.Id.ReqId)
		// Set response
		conn.SetResponse(rsp)
		return
	}
	defer counter.Close()

	conn.log.Debug("SET %v, confirmed.", rsp.Id)
	// Added by Tianium 2022-11-02
	// Flag chunk set as succeeded.
	counter.AddSucceeded(chunk, false)
	req, err := conn.SetResponse(rsp)
	if err == nil {
		collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), int64(0))
	}
}

func (conn *Connection) delHandler() {
	conn.log.Debug("DEL from lambda.")

	rsp := types.NewResponse(protocol.CMD_DEL)
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()
	// Ack lambda if it is supported
	rsp.OnFinalize(conn.getCommandFinalizer(rsp.Cmd))
	defer rsp.Done()

	conn.log.Debug("DEL %v, confirmed.", rsp.Id)
	conn.SetResponse(rsp) // if del is control cmd, should return False
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
	if err != nil {
		// Reset possible optimistic estimation.
		ins.ResetDue()
		conn.log.Warn("Failed to read due: %v", err)
		conn.Close()
		return err
	}
	ins.SetDue(due)

	flags, err := conn.r.ReadInt()
	if err != nil {
		conn.log.Warn("Failed to read piggyback flags: %v", err)
		conn.Close()
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
		conn.instance.ResetDue()
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

func (conn *Connection) getCommandFinalizer(cmd string) func() {
	return func() {
		conn.finalizeCommmand(cmd)
	}
}
