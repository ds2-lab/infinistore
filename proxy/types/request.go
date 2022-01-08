package types

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
)

const (
	REQUEST_INVOKED   = 0
	REQUEST_RETURNED  = 1
	REQUEST_RESPONDED = 2

	CHANGE_PLACEMENT = 0x0001
	MAX_ATTEMPTS     = 3

	DEBUG_INITPROMISE_WAIT = 0x0001
)

var (
	ErrStreamingReq       = errors.New("can not retry a streaming request")
	ErrMaxAttemptsReached = errors.New("max attempts reached")
	ErrResponded          = errors.New("responded")
	ErrNoClient           = errors.New("no client set")
	ErrNotSuppport        = errors.New("not support")

	PlaceholderResponse = &response{}
)

type RequestCloser interface {
	MarkReturnd(*Id) bool
	Close()
}

type response struct {
	status uint32
	client *redeo.Client
}

type Request struct {
	Seq            int64
	Id             Id
	InsId          uint64 // Instance the request targeted.
	Cmd            string
	Key            string
	RetCommand     string
	BodySize       int64
	Body           []byte
	BodyStream     resp.AllReadCloser
	Info           interface{}
	Changes        int
	CollectorEntry interface{}
	Option         int64

	conn             Conn
	streamingStarted bool
	err              error
	leftAttempts     int
	reason           error
	responseTimeout  time.Duration
	responded        promise.Promise

	// Shared if request to be send to multiple lambdas
	response *response
	Cleanup  RequestCloser
}

func GetRequest(client *redeo.Client) *Request {
	return &Request{response: &response{
		status: REQUEST_INVOKED,
		client: client,
	}}
}

func (req *Request) String() string {
	return fmt.Sprintf("%s %v", req.Cmd, req.Id)
}

func (req *Request) Name() string {
	return strings.ToLower(req.Cmd)
}

func (req *Request) GetInfo() interface{} {
	return req.Info
}

func (req *Request) GetRequest() *Request {
	return req
}

func (req *Request) MarkError(err error) int {
	req.err = err
	if req.BodyStream != nil && req.streamingStarted {
		req.leftAttempts = 0
		req.reason = ErrStreamingReq
	} else if req.leftAttempts == 0 {
		req.leftAttempts = MAX_ATTEMPTS - 1
	} else {
		req.leftAttempts--
	}
	return req.leftAttempts
}

func (req *Request) LastError() (int, error) {
	if req.err == nil && req.leftAttempts == 0 {
		req.leftAttempts = MAX_ATTEMPTS
	}
	return req.leftAttempts, req.err
}

func (req *Request) FailureError() error {
	if req.reason != nil {
		return req.reason
	}
	return ErrMaxAttemptsReached
}

func (req *Request) Size() int64 {
	if req.BodyStream != nil {
		return req.BodyStream.Len()
	} else if req.Body != nil {
		return int64(len(req.Body))
	} else if req.BodySize > 0 {
		return req.BodySize
	} else {
		return 0
	}
}

func (req *Request) PrepareForSet(conn Conn) {
	conn.Writer().WriteMultiBulkSize(5)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	req.conn = conn
}

func (req *Request) PrepareForGet(conn Conn) {
	conn.Writer().WriteMultiBulkSize(6)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	conn.Writer().WriteBulkString(strconv.FormatInt(req.BodySize, 10))
	conn.Writer().WriteBulkString(strconv.FormatInt(req.Option, 10))
	req.conn = conn
	req.responseTimeout = protocol.GetBodyTimeout(req.BodySize)
}

func (req *Request) PrepareForDel(conn Conn) {
	conn.Writer().WriteMultiBulkSize(4)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	req.conn = conn
}

func (req *Request) ToRecover() *Request {
	recover := *req
	recover.Cmd = protocol.CMD_RECOVER
	recover.RetCommand = protocol.CMD_GET
	recover.Changes = req.Changes & CHANGE_PLACEMENT
	recover.conn = nil
	recover.responded = nil
	return &recover
}

func (req *Request) PrepareForRecover(conn Conn) {
	conn.Writer().WriteMultiBulkSize(6)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	conn.Writer().WriteBulkString(req.RetCommand)
	conn.Writer().WriteBulkString(strconv.FormatInt(req.BodySize, 10))
	req.conn = conn
	req.responseTimeout = protocol.GetBodyTimeout(req.BodySize) // Consider the time to download and cache the object
	if req.RetCommand == protocol.CMD_GET {
		req.responseTimeout *= 2
	}
}

func (req *Request) Flush() error {
	if req.conn == nil {
		return errors.New("connection for request not set")
	}
	conn := req.conn
	req.conn = nil

	// Write deadline is added back.
	// When read timeout on lambda side, link close may be delayed and flush may be blocked. This blockage can happen, especially on streaming body.
	defer conn.SetWriteDeadline(time.Time{})
	if req.Body != nil {
		req.responseTimeout = protocol.GetBodyTimeout(int64(len(req.Body)))
		conn.SetWriteDeadline(protocol.GetBodyDeadline(int64(len(req.Body))))
		if err := conn.Writer().CopyBulk(bytes.NewReader(req.Body), int64(len(req.Body))); err != nil {
			return err
		}
	} else if req.BodyStream != nil {
		req.streamingStarted = true
		req.responseTimeout = protocol.GetBodyTimeout(req.BodyStream.Len())
		conn.SetWriteDeadline(protocol.GetBodyDeadline(req.BodyStream.Len()))
		if err := conn.Writer().CopyBulk(req.BodyStream, req.BodyStream.Len()); err != nil {
			return err
		}
	}

	conn.SetWriteDeadline(protocol.GetHeaderDeadline())
	return conn.Writer().Flush()
}

// Close cleans up resources of the request: drain unread data in the request frame.
func (req *Request) close() {
	// Unhold the stream if bodyStream was set. So the bodyStream.Close can be unblocked.
	bodyStream := req.BodyStream
	// Lock free pointer swap.
	if bodyStream != nil && atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&req.BodyStream)), *(*unsafe.Pointer)(unsafe.Pointer(&bodyStream)), unsafe.Pointer(nil)) {
		if holdable, ok := bodyStream.(resp.Holdable); ok {
			holdable.Unhold()
		}
	}

	if req.Cleanup != nil {
		req.Cleanup.Close()
		req.Cleanup = nil
	}
}

func (req *Request) IsReturnd() bool {
	if req.response == nil {
		return false
	}
	return atomic.LoadUint32(&req.response.status) >= REQUEST_RETURNED
}

func (req *Request) IsResponded() bool {
	if req.response == nil {
		return false
	}
	return atomic.LoadUint32(&req.response.status) >= REQUEST_RESPONDED
}

func (req *Request) MarkReturned() bool {
	if req.response == nil {
		return false
	}
	return atomic.CompareAndSwapUint32(&req.response.status, REQUEST_INVOKED, REQUEST_RETURNED)
}

func (req *Request) IsResponse(rsp *Response) bool {
	return (req.Cmd == rsp.Cmd || req.RetCommand == rsp.Cmd) &&
		req.Id.ReqId == rsp.Id.ReqId &&
		req.Id.ChunkId == rsp.Id.ChunkId
}

// SetResponse sets response of the request. Concurrent request dispatching is
// supported, in which case the request will be dispatched to multiple instances,
// one of which is required and others are optional. SetResponse ensure only one
// response will be sent by:
// 1. Cancel timeout for individual request copy.
// 2. Ignore err if the request copy is optional.
// 3. Exclusively set response for first responder.
func (req *Request) SetResponse(rsp interface{}) (err error) {
	defer util.PanicRecovery("proxy/types/Request.SetResponse", &err)

	responded := req.responded
	req.responded = nil
	if responded != nil {
		responded.Resolve(rsp)
	}

	// Ignore err if request is optional
	if _, ok := rsp.(error); ok && req.Option&protocol.REQUEST_GET_OPTIONAL > 0 {
		return nil
	}

	if req.response == nil {
		return ErrNoClient
	}

	// Makeup: do cleanup
	// req.close safe
	cleanup := req.Cleanup
	if cleanup != nil {
		cleanup.MarkReturnd(&req.Id)
	} else {
		req.MarkReturned()
	}
	if !atomic.CompareAndSwapUint32(&req.response.status, REQUEST_RETURNED, REQUEST_RESPONDED) {
		return ErrResponded
	}
	req.close()

	if req.response.client == nil {
		return ErrNoClient
	}

	ret := req.response.client.AddResponses(&ProxyResponse{Response: rsp, Request: req})
	// Release reference so chan can be garbage collected.
	req.response.client = nil
	return ret
}

// Only appliable to GET so far.
func (req *Request) Abandon() error {
	if req.Cmd != protocol.CMD_GET {
		return ErrNotSuppport
	}
	return req.SetResponse(&Response{Id: req.Id, Cmd: req.Cmd})
}

func (req *Request) Timeout(opts ...int) (err error) {
	defer util.PanicRecovery("proxy/types/Request.SetResponse", &err)

	if req.responseTimeout < protocol.GetHeaderTimeout() {
		req.responseTimeout = protocol.GetHeaderTimeout()
	}
	// Initialize
	req.initPromise(opts...)
	p := req.responded
	if p != nil {
		p.SetTimeout(req.responseTimeout)
		return p.Timeout()
	} else {
		// Responded
		return nil
	}
}

func (req *Request) initPromise(opts ...int) {
	if req.responded == nil {
		req.responded = promise.NewPromise()
	}
	if len(opts) > 0 && opts[0]&DEBUG_INITPROMISE_WAIT > 0 {
		<-time.After(50 * time.Millisecond)
	}
}
