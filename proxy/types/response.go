package types

import (
	"errors"
	"strconv"

	"github.com/mason-leap-lab/redeo/resp"
)

type ProxyResponse struct {
	Response interface{}
	Request  *Request
}

type Response struct {
	Id         Id
	Cmd        string
	Size       int64
	Body       []byte
	BodyStream resp.AllReadCloser

	w resp.ResponseWriter
}

func (rsp *Response) PrepareForSet(w resp.ResponseWriter) {
	w.AppendBulkString(rsp.Id.ReqId)
	w.AppendBulkString(rsp.Id.ChunkId)
	w.AppendBulk(rsp.Body)
	rsp.w = w
}

func (rsp *Response) PrepareForGet(w resp.ResponseWriter) {
	w.AppendBulkString(rsp.Id.ReqId)
	w.AppendBulkString(strconv.FormatInt(rsp.Size, 10))
	if rsp.Body == nil && rsp.BodyStream == nil {
		w.AppendBulkString("-1")
	} else {
		w.AppendBulkString(rsp.Id.ChunkId)
	}
	if rsp.Body != nil {
		w.AppendBulk(rsp.Body)
	}
	rsp.w = w
}

func (rsp *Response) Flush() error {
	if rsp.w == nil {
		return errors.New("writer for response not set")
	}
	w := rsp.w
	rsp.w = nil

	if rsp.BodyStream != nil {
		if err := w.CopyBulk(rsp.BodyStream, rsp.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := rsp.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
	}

	return w.Flush()
}
