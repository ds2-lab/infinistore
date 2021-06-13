package types

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
)

const (
	REQUEST_INVOKED   = 0
	REQUEST_RETURNED  = 1
	REQUEST_RESPONDED = 2

	CHANGE_PLACEMENT = 0x0001
	MAX_ATTEMPTS     = 3
)

var (
	ErrStreamingReq       = errors.New("can not retry a streaming request")
	ErrMaxAttemptsReached = errors.New("max attempts reached")
	ErrResponded          = errors.New("responded")
	ErrNoClient           = errors.New("no client set")
	ErrNotSuppport        = errors.New("not support")
)

type Request struct {
	Id             Id
	InsId          uint64 // Instance the request targeted.
	Cmd            string
	Key            string
	RetCommand     string
	BodySize       int64
	Body           []byte
	BodyStream     resp.AllReadCloser
	Client         *redeo.Client
	Info           interface{}
	Changes        int
	CollectorEntry interface{}
	QueuedAt       time.Time

	conn             Conn
	status           uint32
	streamingStarted bool
	responseTimeout  time.Duration
	responded        promise.Promise
	err              error
	leftAttempts     int
	reason           error
}

func (req *Request) String() string {
	return fmt.Sprintf("%s %v", req.Cmd, req.Id)
}

func (req *Request) Name() string {
	return strings.ToLower(req.Cmd)
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
	conn.Writer().WriteMultiBulkSize(5)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	conn.Writer().WriteBulkString(strconv.FormatInt(req.BodySize, 10))
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
	req.Cmd = protocol.CMD_RECOVER
	req.RetCommand = protocol.CMD_GET
	req.Changes = req.Changes & CHANGE_PLACEMENT
	return req
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
		conn.SetWriteDeadline(protocol.GetBodyDeadline(int64(len(req.Body))))
		if err := conn.Writer().CopyBulk(bytes.NewReader(req.Body), int64(len(req.Body))); err != nil {
			return err
		}
	} else if req.BodyStream != nil {
		req.streamingStarted = true
		conn.SetWriteDeadline(protocol.GetBodyDeadline(req.BodyStream.Len()))
		if err := conn.Writer().CopyBulk(req.BodyStream, req.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := req.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
	}

	conn.SetWriteDeadline(protocol.GetHeaderDeadline())
	return conn.Writer().Flush()
}

func (req *Request) IsReturnd() bool {
	return atomic.LoadUint32(&req.status) >= REQUEST_RETURNED
}

func (req *Request) IsResponded() bool {
	return atomic.LoadUint32(&req.status) >= REQUEST_RESPONDED
}

func (req *Request) MarkReturned() {
	atomic.CompareAndSwapUint32(&req.status, REQUEST_INVOKED, REQUEST_RETURNED)
}

func (req *Request) IsResponse(rsp *Response) bool {
	return (req.Cmd == rsp.Cmd || req.RetCommand == rsp.Cmd) &&
		req.Id.ReqId == rsp.Id.ReqId &&
		req.Id.ChunkId == rsp.Id.ChunkId
}

func (req *Request) SetResponse(rsp interface{}) error {
	req.MarkReturned()
	if !atomic.CompareAndSwapUint32(&req.status, REQUEST_RETURNED, REQUEST_RESPONDED) {
		return ErrResponded
	}
	if req.responded != nil {
		req.responded.Resolve(rsp)
		req.responded = nil
	}
	if req.Client == nil {
		return ErrNoClient
	}

	ret := req.Client.AddResponses(&ProxyResponse{Response: rsp, Request: req})
	// Release reference so chan can be garbage collected.
	req.Client = nil
	return ret
}

// Only appliable to GET so far.
func (req *Request) Abandon() error {
	if req.Cmd != protocol.CMD_GET {
		return ErrNotSuppport
	}
	return req.SetResponse(&Response{Id: req.Id, Cmd: req.Cmd})
}

func (req *Request) Timeout() error {
	if req.responseTimeout < protocol.GetHeaderTimeout() {
		req.responseTimeout = protocol.GetHeaderTimeout()
	}
	p := req.initPromise()
	p.SetTimeout(req.responseTimeout)
	return p.Timeout()
}

func (req *Request) initPromise() promise.Promise {
	if req.responded == nil {
		req.responded = promise.NewPromise()
	}
	return req.responded
}
