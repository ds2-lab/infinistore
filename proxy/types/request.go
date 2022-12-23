package types

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	"github.com/mason-leap-lab/go-utils/promise"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
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

type RequestGroup interface {
	MarkReturnd(*Id) (uint64, bool)
	IsFulfilled(status ...uint64) bool
	IsAllReturned(status ...uint64) bool
	Close()
}

// Shared response container.
type response struct {
	status uint32
	client *redeo.Client
	rsp    interface{}
	mu     sync.Mutex
	once   sync.Once
}

func (r *response) set(rsp interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !atomic.CompareAndSwapUint32(&r.status, REQUEST_RETURNED, REQUEST_RESPONDED) {
		return ErrResponded
	}
	r.rsp = rsp
	return nil
}

func (r *response) get() interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rsp
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
	RequestGroup   RequestGroup
	PersistChunk   PersistChunk
	PredictedDue   time.Duration

	conn             Conn
	streamingStarted bool
	err              error
	leftAttempts     int
	reason           error
	responseTimeout  time.Duration
	responded        *atomic.Value // Use pointer to allow the promise being cleared.

	// Shared if request to be send to multiple lambdas
	response *response
	// Added by Tianium 20221102
	AllDone      bool // Is sharded request done after chunk request
	AllSucceeded bool // Is sharded request succeeded after chunk request
}

func GetRequest(client *redeo.Client) *Request {
	return &Request{
		response: &response{
			status: REQUEST_INVOKED,
			client: client,
		},
		responded: &atomic.Value{},
	}
}

func (req *Request) String() string {
	return fmt.Sprintf("%s %v", req.Cmd, &req.Id)
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

// MarkError updates the error of the request and returns the remaining attempts. Error specified can be nil to reset the error.
func (req *Request) MarkError(err error) int {
	req.err = err
	// No change to number of attempts if error is nil. For asynchronous request, error can be reported later.
	// Or if the request is success, it does not matter to retry.
	if err == nil {
		return req.leftAttempts
	}

	if req.BodyStream != nil && req.streamingStarted {
		if req.PersistChunk != nil && req.leftAttempts > 1 {
			// Once persist chunk start streaming, it will continue streaming until stored.
			// Start a new downstream to read data. No need to hold.
			req.BodyStream, _ = req.PersistChunk.Load(context.Background())
			req.leftAttempts--
		} else {
			req.leftAttempts = 0
			req.reason = ErrStreamingReq
		}
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

func (req *Request) ToSetRetrial(stream resp.AllReadCloser) *Request {
	// Initiate a request with no response needed.
	retrial := GetRequest(nil)
	retrial.Key = req.Key
	retrial.Id = req.Id
	retrial.InsId = req.InsId
	retrial.Cmd = req.Cmd
	retrial.BodyStream = stream
	retrial.Info = req.Info
	return retrial
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

func (req *Request) ToCachedResponse(cached PersistChunk) *Response {
	rsp := &Response{
		Id:     req.Id,
		Cmd:    req.Cmd,
		cached: cached,
		from:   "cached",
	}
	if req.RetCommand != "" {
		rsp.Cmd = req.RetCommand
	}
	return rsp
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
	recover := *req // Shadow copy, req.response get shared.
	recover.Cmd = protocol.CMD_RECOVER
	recover.RetCommand = protocol.CMD_GET
	recover.Changes = req.Changes & CHANGE_PLACEMENT
	recover.conn = nil
	recover.resetPromise()
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

	if req.RequestGroup != nil {
		req.RequestGroup.Close()
		req.RequestGroup = nil
	}
}

// Validate confirms if the request should be sent to the Lambda. Pass true for the final confirmation.
func (req *Request) Validate(final bool) bool {
	if !req.isResponded() {
		return true
	} else if !req.MustRequest() {
		return false
	}
	if ok := req.PersistChunk.CanClose(); !ok {
		return true
	} else if !final {
		// Close the chunk so no further request will pending on this chunk. Will confirm again later.
		req.PersistChunk.Close()
		return true
	} else {
		// If Closed, it is safe to quit the request.
		return !req.PersistChunk.IsClosed()
	}
}

// MustRequest indicates that the request must be sent to the Lambda.
func (req *Request) MustRequest() bool {
	return req.PersistChunk != nil
}

// IsReturnd indicates if a response is received, especially as one of the request set.
// The content of the response may not available now and can change depends on the status of request set. (e.g. first-d optimization)
func (req *Request) IsReturnd() bool {
	if req.response == nil {
		return false
	}
	return atomic.LoadUint32(&req.response.status) >= REQUEST_RETURNED
}

// IsResponded indicates if a response is available for the request.
func (req *Request) isResponded() bool {
	if req.response == nil {
		return false
	}
	return atomic.LoadUint32(&req.response.status) >= REQUEST_RESPONDED
}

// MarkReturned marks the request as returned/response received and returns if this is the first time to mark.
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

func (req *Request) SetResponse(rsp *Response) error {
	return req.setResponse(rsp)
}

func (req *Request) SetErrorResponse(err error) error {
	return req.setResponse(err)
}

// Only appliable to GET so far.
func (req *Request) Abandon() error {
	if req.Cmd != protocol.CMD_GET {
		return ErrNotSuppport
	}
	err := req.SetResponse(&Response{Id: req.Id, Cmd: req.Cmd, abandon: true, from: "abandoned"})
	if err != nil && err != ErrResponded {
		return err
	}

	// Try abandon streaming.
	if rsp := req.getResponse(); rsp != nil && !rsp.IsAbandon() {
		rsp.CancelFlush()
	}
	return nil
}

func (req *Request) Timeout(opts ...int) (responded promise.Promise, err error) {
	// defer util.PanicRecovery("proxy/types/Request.SetResponse", &err)

	// responseTimeout has been calculated in PrepareForXXX()
	if req.responseTimeout < protocol.GetHeaderTimeout() {
		req.responseTimeout = protocol.GetHeaderTimeout()
	}

	// Initialize promise
	p := req.InitPromise(opts...)
	defer req.resetPromise() // One time use
	// Set timeout
	p.SetTimeout(req.responseTimeout)

	// We will not check if the shared response is available, a promise for sent request must be resolved.

	// Wait for timeout
	return p, p.Timeout()
}

func (req *Request) ResponseTimeout() time.Duration {
	return req.responseTimeout
}

// SetResponse sets response of the request. Concurrent request dispatching is
// supported, in which case the request will be dispatched to multiple instances,
// one of which is required and others are optional. SetResponse ensure only one
// response will be sent by:
// 1. Cancel timeout for individual request copy.
// 2. Ignore err if the request copy is optional.
// 3. Exclusively set response for first responder.
func (req *Request) setResponse(rsp interface{}) (err error) {
	// defer util.PanicRecovery("proxy/types/Request.SetResponse", &err)

	// Responsed promise has high priority because:
	// * The response promise is associated with the sent request.
	// * If the promise is available, the request must have been sent to the Lambda and waiting for response.
	// * When response duel, in which case multiple requests are sent to different Lambdas, multiple promises exist to be resolved.
	responded, ok := req.responded.Load().(promise.Promise)
	if ok && !responded.IsResolved() {
		responded.Resolve(rsp)
	}

	// Ignore err if request is optional
	if _, ok := rsp.(error); ok && req.Option&protocol.REQUEST_GET_OPTIONAL > 0 {
		return nil
	}

	if req.response == nil {
		return ErrNoClient
	}

	// Notified the request group that the request is returned but not necessarily succeeded.
	// Should req.close safe. However, we have to use sync.Once to ensure it.
	req.response.once.Do(req.notifyRequestGroup)

	// Atomically set response, only the first responder will succeed.
	if err := req.response.set(rsp); err != nil {
		return ErrResponded
	}

	// We are done with the request, close it.
	req.close()

	// Send repsonse to client. Skip if client is nil or reset.
	if req.response.client == nil {
		return nil
	}
	if response, ok := rsp.(*Response); ok {
		response.request = req
		err = req.response.client.AddResponses(response)
	} else {
		// Handle unexpected response and normalize error response.
		errResponse, ok := rsp.(error)
		if !ok {
			errResponse = fmt.Errorf("unexpected response: %v", rsp)
		}
		err = req.response.client.AddResponses(&proxyResponse{response: errResponse, request: req})
		// Make sure the persisting chunk being notified the error.
		if req.PersistChunk != nil && !req.PersistChunk.IsStored() {
			req.PersistChunk.CloseWithError(errResponse)
		}
	}
	// if err != nil {
	// 	err = fmt.Errorf("client %v: %v", req.response.client.Conn(), err)
	// }

	// Release reference so chan can be garbage collected.
	req.response.client = nil
	return err
}

func (req *Request) notifyRequestGroup() {
	// Makeup:
	// 1. Ensure return mark is marked
	// 2. Do cleanup
	group := req.RequestGroup
	if group != nil {
		status, _ := group.MarkReturnd(&req.Id)
		// Added by Tianium: 20221102
		// Update status of shared request.
		req.AllDone = group.IsAllReturned(status)
		req.AllSucceeded = group.IsFulfilled(status)
	} else {
		req.MarkReturned()
	}
}

// Response returns shared response if available.
func (req *Request) getResponse() *Response {
	// Read from shared response first.
	if req.isResponded() {
		rsp, _ := req.response.get().(*Response)
		return rsp
	}

	return nil
}

// InitPromise initialize a promise for later use. NOTE: This function is not thread-safe
func (req *Request) InitPromise(opts ...int) (ret promise.Promise) {
	ret, _ = req.responded.Load().(promise.Promise)
	if ret == nil {
		ret = promise.NewPromise()
		req.responded.Store(ret)
	}

	if len(opts) > 0 && opts[0]&DEBUG_INITPROMISE_WAIT > 0 {
		<-time.After(50 * time.Millisecond)
	}
	return
}

func (req *Request) resetPromise(opts ...int) {
	req.responded = &atomic.Value{}
}
