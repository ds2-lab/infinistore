package server

import (
	"github.com/mason-leap-lab/redeo/resp"
)

type Response interface {
	Flush() error
}

func NewNilResponse(w resp.ResponseWriter, seq int64) Response {
	w.AppendNil()
	w.AppendInt(seq)
	return w
}

func NewErrorResponse(w resp.ResponseWriter, seq int64, msg string, args ...interface{}) Response {
	w.AppendErrorf(msg, args...)
	w.AppendInt(seq)
	return w
}
