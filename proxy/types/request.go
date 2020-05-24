package types

import (
	"errors"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
	"strconv"
	"sync/atomic"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

const (
	REQUEST_INVOKED = 0
	REQUEST_RETURNED = 1
	REQUEST_RESPONDED = 2
)

type Request struct {
	Id           Id
	InsId        uint64   // Instance the request targeted.
	Cmd          string
	Key          string
	Body         []byte
	BodyStream   resp.AllReadCloser
	Client       *redeo.Client
	EnableCollector bool
	Info         interface{}

	conn         Conn
	status        uint32
	streamingStarted bool
}

func (req *Request) String() string {
	return req.Cmd
}

func (req *Request) GetRequest() *Request {
	return req
}

func (req *Request) Retriable() bool {
	return req.BodyStream == nil || !req.streamingStarted
}

func (req *Request) PrepareForSet(conn Conn) {
	conn.Writer().WriteMultiBulkSize(6)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(strconv.Itoa(req.Id.ConnId))
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
	conn.Writer().WriteBulkString(strconv.Itoa(req.Id.ConnId))
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString("")
	conn.Writer().WriteBulkString(req.Key)
	req.conn = conn
}

func (req *Request) PrepareForDel(conn Conn) {
	conn.Writer().WriteMultiBulkSize(5)
	conn.Writer().WriteBulkString(req.Cmd)
	conn.Writer().WriteBulkString(strconv.Itoa(req.Id.ConnId))
	conn.Writer().WriteBulkString(req.Id.ReqId)
	conn.Writer().WriteBulkString(req.Id.ChunkId)
	conn.Writer().WriteBulkString(req.Key)
	req.conn = conn
}

func (req *Request) Flush(timeout time.Duration) error {
	if req.conn == nil {
		return errors.New("Connection for request not set.")
	}
	conn := req.conn
	req.conn = nil

	conn.SetWriteDeadline(time.Now().Add(timeout)) // Set deadline for write
	defer conn.SetWriteDeadline(time.Time{})
	if err := conn.Writer().Flush(); err != nil {
		return err
	}

	if req.BodyStream != nil {
		req.streamingStarted = true
		conn.SetWriteDeadline(time.Time{})
		if err := conn.Writer().CopyBulk(req.BodyStream, req.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := req.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}

		conn.SetWriteDeadline(time.Now().Add(timeout))
		return conn.Writer().Flush()
	}

	return nil
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
	return req.Cmd == rsp.Cmd &&
		req.Id.ReqId == rsp.Id.ReqId &&
		req.Id.ChunkId == rsp.Id.ChunkId
}

func (req *Request) SetResponse(rsp interface{}) bool {
	req.MarkReturned()
	if !atomic.CompareAndSwapUint32(&req.status, REQUEST_RETURNED, REQUEST_RESPONDED) {
		return false
	}
	if req.Client != nil {
		ret := req.Client.AddResponses(&ProxyResponse{ rsp, req })

		// Release reference so chan can be garbage collected.
		req.Client = nil
		return ret == nil
	}

	return false
}

// Only appliable to GET so far.
func (req *Request) Abandon() bool {
	if req.Cmd != protocol.CMD_GET {
		return false
	}
	return req.SetResponse(&Response{ Id: req.Id, Cmd: req.Cmd })
}
