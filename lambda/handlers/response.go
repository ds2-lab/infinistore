package handlers

import (
	"bytes"
	"github.com/mason-leap-lab/redeo/resp"
	"net"
	"time"
)

var (
	RequestTimeout = 1 * time.Second
)

type Response struct {
	resp.ResponseWriter
	Conn net.Conn

	Cmd string
	ConnId string
	ReqId string
	ChunkId string
	Val string
	Body []byte
	BodyStream resp.AllReadCloser
}

func NewResponse(conn net.Conn, writer resp.ResponseWriter) *Response {
	if writer == nil {
		writer = resp.NewResponseWriter(conn)
	}
	return &Response{
		ResponseWriter: writer,
		Conn: conn,
	}
}

func (r *Response) Prepare() {
	r.AppendBulkString(r.Cmd)
	r.AppendBulkString(r.ConnId)
	r.AppendBulkString(r.ReqId)
	r.AppendBulkString(r.ChunkId)
	if len(r.Val) > 0 {
		r.AppendBulkString(r.Val)
	}
}

func (r *Response) PrepareByResponse(reader resp.ResponseReader) (err error) {
	r.Cmd, err = reader.ReadBulkString()
	if err != nil {
		return
	}
	r.ConnId, err = reader.ReadBulkString()
	if err != nil {
		return
	}
	r.ReqId, err = reader.ReadBulkString()
	if err != nil {
		return
	}
	r.ChunkId, err = reader.ReadBulkString()
	if err != nil {
		return
	}
	r.BodyStream, err = reader.StreamBulk()
	if err != nil {
		return
	}

	r.Prepare()
	return
}

func (r *Response) Flush() error {
	r.Conn.SetWriteDeadline(time.Now().Add(RequestTimeout)) // Set deadline for write
	defer r.Conn.SetWriteDeadline(time.Time{})
	r.ResponseWriter.Flush()

	hasBulk := true
	if r.Body != nil {
		r.Conn.SetWriteDeadline(time.Time{}) // Disable timeout for bulk data
		if err := r.CopyBulk(bytes.NewReader(r.Body), int64(len(r.Body))); err != nil {
			return err
		}
	} else if r.BodyStream != nil {
		r.Conn.SetWriteDeadline(time.Time{}) // Disable timeout for bulk data
		if err := r.CopyBulk(r.BodyStream, r.BodyStream.Len()); err != nil {
			// On error, we need to unhold the stream, and allow Close to perform.
			if holdable, ok := r.BodyStream.(resp.Holdable); ok {
				holdable.Unhold()
			}
			return err
		}
	} else {
		hasBulk = false
	}

	if hasBulk {
		r.Conn.SetWriteDeadline(time.Now().Add(RequestTimeout)) // Set deadline for write
		return r.ResponseWriter.Flush()
	}

	return nil
}
