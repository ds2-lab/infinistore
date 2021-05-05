package types

import (
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

	conn             Conn
	status           uint32
	streamingStarted bool
	responded        promise.Promise
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

func (req *Request) Retriable() bool {
	return req.BodyStream == nil || !req.streamingStarted
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
	if req.Body != nil {
		conn.Writer().WriteBulk(req.Body)
	}
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
}

func (req *Request) Flush() error {
	if req.conn == nil {
		return errors.New("connection for request not set")
	}
	conn := req.conn
	req.conn = nil

	if req.BodyStream != nil {
		req.streamingStarted = true
		if err := conn.Writer().CopyBulk(req.BodyStream, req.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := req.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
	}

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

func (req *Request) SetResponse(rsp interface{}) bool {
	req.MarkReturned()
	if !atomic.CompareAndSwapUint32(&req.status, REQUEST_RETURNED, REQUEST_RESPONDED) {
		return false
	}
	if req.responded != nil {
		req.responded.Resolve(rsp)
	}
	if req.Client != nil {
		ret := req.Client.AddResponses(&ProxyResponse{Response: rsp, Request: req})

		// Release reference so chan can be garbage collected.
		req.Client = nil
		return ret == nil
	}

	return true
}

// Only appliable to GET so far.
func (req *Request) Abandon() bool {
	if req.Cmd != protocol.CMD_GET {
		return false
	}
	return req.SetResponse(&Response{Id: req.Id, Cmd: req.Cmd})
}

func (req *Request) SetTimeout(timeout time.Duration) {
	req.initPromise().SetTimeout(timeout)
}

func (req *Request) Timeout() error {
	return req.initPromise().Timeout()
}

func (req *Request) initPromise() promise.Promise {
	if req.responded == nil {
		req.responded = promise.NewPromise()
	}
	return req.responded
}
