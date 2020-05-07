package types

import (
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

const (
	REQUEST_INVOKED   = 0
	REQUEST_RETURNED  = 1
	REQUEST_RESPONDED = 2
)

type Request struct {
	Id              Id
	InsId           uint64 // Instance the request targeted.
	Cmd             string
	Key             string
	RetCommand      string
	Body            []byte
	BodyStream      resp.AllReadCloser
	Client          *redeo.Client
	EnableCollector bool
	Obj             interface{}

	w                *resp.RequestWriter
	status           uint32
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

func (req *Request) PrepareForSet(w *resp.RequestWriter) {
	w.WriteMultiBulkSize(6)
	w.WriteBulkString(req.Cmd)
	w.WriteBulkString(strconv.Itoa(req.Id.ConnId))
	w.WriteBulkString(req.Id.ReqId)
	w.WriteBulkString(req.Id.ChunkId)
	w.WriteBulkString(req.Key)
	if req.Body != nil {
		w.WriteBulk(req.Body)
	}
	req.w = w
}

func (req *Request) PrepareForGet(w *resp.RequestWriter) {
	w.WriteMultiBulkSize(5)
	w.WriteBulkString(req.Cmd)
	w.WriteBulkString(strconv.Itoa(req.Id.ConnId))
	w.WriteBulkString(req.Id.ReqId)
	w.WriteBulkString("") // Obsoleted. Chunk Id is included in the key.
	w.WriteBulkString(req.Key)
	req.w = w
}

//func (req *Request) PrepareForData(w *resp.RequestWriter) {
//	w.WriteMultiBulkSize(1)
//	w.WriteBulkString(req.Cmd)
//	req.w = w
//}

func (req *Request) PrepareForDel(w *resp.RequestWriter) {
	w.WriteMultiBulkSize(5)
	w.WriteBulkString(req.Cmd)
	w.WriteBulkString(strconv.Itoa(req.Id.ConnId))
	w.WriteBulkString(req.Id.ReqId)
	w.WriteBulkString(req.Id.ChunkId)
	w.WriteBulkString(req.Key)
	req.w = w
}

func (req *Request) PrepareForRecover(w *resp.RequestWriter) {
	w.WriteMultiBulkSize(6)
	w.WriteBulkString(req.Cmd)
	w.WriteBulkString("") // Obsoleted. ConnId.
	w.WriteBulkString(req.Id.ReqId)
	w.WriteBulkString("") // Keep consistent with GET
	w.WriteBulkString(req.Key)
	w.WriteBulkString(req.RetCommand)
	req.w = w
}

func (req *Request) Flush() error {
	if req.w == nil {
		return errors.New("Writer for request not set.")
	}
	w := req.w
	req.w = nil

	if err := w.Flush(); err != nil {
		return err
	}

	if req.BodyStream != nil {
		req.streamingStarted = true
		if err := w.CopyBulk(req.BodyStream, req.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := req.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
		return w.Flush()
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
		ret := req.Client.AddResponses(&ProxyResponse{rsp, req})

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
	return req.SetResponse(&Response{Id: req.Id, Cmd: req.Cmd})
}
