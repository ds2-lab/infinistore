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

	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
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

	ErrConnectionClosed  = errors.New("connection closed")
	ErrMissingResponse   = errors.New("missing response")
	ErrUnexpectedCommand = errors.New("unexpected command")
	ErrUnexpectedType    = errors.New("unexpected type")
)

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
	headRequest *types.Request
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

func (conn *Connection) SendPing(payload []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.IsClosed() {
		return ErrConnectionClosed
	}

	conn.w.WriteMultiBulkSize(2)
	conn.w.WriteBulkString(protocol.CMD_PING)
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

	if !conn.control {
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

	// Close check can prevent resource to be used from releasing. However, close signal is not guranteed.
	if conn.IsClosed() {
		if req.MarkError(ErrConnectionClosed) > 0 {
			conn.instance.chanPriorCmd <- req
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
	conn.headRequest = req
	if req.IsResponded() {
		conn.popRequest()
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
		conn.flagAvailable()
		// Unrecoverable
		return
	}

	// Updated by Tianium: 20210523
	// Write timeout is added back. See comments in req.Flush()
	if err := req.Flush(); err != nil {
		if req.MarkError(err) > 0 {
			conn.instance.chanPriorCmd <- req
		} else {
			conn.SetErrorResponse(err)
		}
		conn.log.Warn("Flush request error: %v - %v", req, err)
		conn.Close()
		return
	}
	// Set instance busy. Yes requests will call busy twice:
	// on schedule(instance.handleRequest) and on request.
	ins := conn.instance // Save a reference in case the connection being released later.
	ins.busy()
	go func() {
		if err := req.Timeout(); err != nil && req.SetResponse(err) { // If req is responded, err has been reported somewhere.
			conn.log.Warn("Request timeout: %v", req)
			conn.popRequest()
			// close connection to discard late response.
			conn.Close()
		}
		ins.doneBusy()
	}()
}

func (conn *Connection) peekRequest() *types.Request {
	return conn.headRequest
}

func (conn *Connection) popRequest() *types.Request {
	conn.headRequest = nil
	select {
	case req := <-conn.chanWait:
		return req
	default:
		return nil
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
					conn.log.Warn("Lambda store disconnected.")
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
			conn.flagAvailable()
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
// If parameter release is set, the connection will be flagged available.
func (conn *Connection) SetResponse(rsp *types.Response, release bool) (*types.Request, bool) {
	// Last request can be responded, either bacause error or timeout, which causes nil or unmatch
	req := conn.peekRequest()
	if req == nil || !req.IsResponse(rsp) {
		// ignore
		conn.log.Debug("response discarded: %v", rsp)
		return nil, false
	}

	// Lock free: double check that poped is what we peeked.
	if poped := conn.popRequest(); poped == req {
		if release {
			conn.flagAvailable()
		}
		conn.log.Debug("response matched: %v", req)
		return req, req.SetResponse(rsp)
	}

	return nil, false
}

// SetErrorResponse Set response to last request as a error.
//   You may need to flag the connection as available manually depends on the error.
func (conn *Connection) SetErrorResponse(err error) bool {
	if req := conn.popRequest(); req != nil {
		return req.SetResponse(err)
	}

	conn.log.Warn("Unexpected error response: no request pending. err: %v", err)
	return false
}

func (conn *Connection) ClearResponses() {
	req := conn.popRequest()
	for req != nil {
		req.SetResponse(ErrConnectionClosed)
		// Yield for pending req a chance to push.
		runtime.Gosched()

		req = conn.popRequest()
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

func (conn *Connection) flagAvailable() bool {
	lm := conn.lm
	if conn.control || lm == nil || conn.IsClosed() {
		return false
	}

	lm.FlagAvailableForRequest(conn)
	return true
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
	if err == nil || err == ErrNotCtrlLink || err == ErrInstanceValidated {
		conn.log.Debug("PONG from lambda confirmed.")
	} else {
		conn.log.Warn("Discard rouge PONG(%v) for %d, current %v", conn, storeId, validated)
		conn.Conn.Close() // Close connection normally, so lambda will close itself.
		conn.Close()
	}
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

	rsp := &types.Response{Cmd: protocol.CMD_GET}
	rsp.Id.ReqId = reqId
	rsp.Id.ChunkId = chunkId
	rsp.Status = recovered
	chunk, _ := strconv.Atoi(chunkId)
	// Check lambda if it is supported
	defer conn.finalizeCommmand(rsp.Cmd)

	counter, _ := global.ReqCoordinator.Load(reqId).(*global.RequestCounter)
	if counter == nil {
		conn.log.Warn("Request not found: %s", reqId)
		// Set response and exhaust value
		conn.SetResponse(rsp, false)
		if err := stream.Close(); err != nil {
			conn.Close()
			conn.log.Warn("Failed to skip bulk on request mismatch: %v", err)
		}
		return
	}

	status := counter.AddSucceeded(chunk, recovered == 1)
	// Check if chunks are enough? Shortcut response if YES.
	if counter.IsLate(status) {
		conn.log.Debug("GOT %v, abandon.", rsp.Id)
		// Most likely, the req has been abandoned already. But we still need to consume the connection side req.
		// Connection will not be flagged available until SkipBulk() is executed.
		req, _ := conn.SetResponse(rsp, false)
		if req != nil {
			_, err := collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), recovered)
			if err != nil {
				conn.log.Warn("LogRequestFuncResponse err %v", err)
			}
		}
		counter.ReleaseIfAllReturned(status)

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
	if req, ok := conn.SetResponse(rsp, false); !ok {
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

	// Close will block until BodyStream unhold.
	if err := stream.Close(); err != nil {
		conn.Close()
	}
}

func (conn *Connection) setHandler(start time.Time) {
	conn.log.Debug("SET from lambda.")

	rsp := &types.Response{Cmd: protocol.CMD_SET, Body: []byte(strconv.FormatUint(conn.instance.Id(), 10))}
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()

	conn.log.Debug("SET %v, confirmed.", rsp.Id)
	req, ok := conn.SetResponse(rsp, false)
	if ok {
		collector.CollectRequest(collector.LogRequestFuncResponse, req.CollectorEntry, start.UnixNano(), int64(time.Since(start)), int64(0), int64(0))
	}

	// Check lambda if it is supported
	conn.finalizeCommmand(rsp.Cmd)
}

func (conn *Connection) delHandler() {
	conn.log.Debug("DEL from lambda.")

	rsp := &types.Response{Cmd: protocol.CMD_DEL}
	rsp.Id.ReqId, _ = conn.r.ReadBulkString()
	rsp.Id.ChunkId, _ = conn.r.ReadBulkString()

	conn.log.Debug("DEL %v, confirmed.", rsp.Id)
	conn.SetResponse(rsp, false) // if del is control cmd, should return False

	// Check lambda if it is supported
	conn.finalizeCommmand(rsp.Cmd)
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
		conn.instance.bye(conn)
	}
}

func (conn *Connection) recoverHandler() {
	conn.log.Debug("RECOVER from lambda.")

	reqId, _ := conn.r.ReadBulkString()
	conn.r.ReadBulkString() // chunkId

	ctrl := global.ReqCoordinator.Load(reqId)
	if ctrl == nil {
		conn.log.Warn("No control found for %s", reqId)
	} else if ctrl.(*types.Control).Callback == nil {
		conn.log.Warn("Control callback not defined for recover request %s", reqId)
	} else {
		ctrl.(*types.Control).Callback(ctrl.(*types.Control), conn.instance)
	}

	// Check lambda if it is supported
	conn.finalizeCommmand(protocol.CMD_RECOVER)
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
	return nil
}

func (conn *Connection) ackCommand(cmd string) error {
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
		return err
	} else if err = conn.ackCommand(cmd); err != nil {
		return err
	}

	conn.flagAvailable()
	return nil
}
