package handlers

import (
	"fmt"
	"strconv"
)

type ResponseError struct {
	error
	StatusCode int
}

func NewResponseError(status int, msg interface{}, args ...interface{}) *ResponseError {
	switch msg := msg.(type) {
	case error:
		return &ResponseError{
			error:      msg,
			StatusCode: status,
		}
	default:
		return &ResponseError{
			error:      fmt.Errorf(msg.(string), args...),
			StatusCode: status,
		}
	}
}

func (e *ResponseError) Status() string {
	return strconv.Itoa(e.StatusCode)
}

type PongError struct {
	error
	flags int64
}

func (e *PongError) Flags() int64 {
	return e.flags
}
