package handlers

import (
	"errors"
	"fmt"
	"strconv"
)

type ResponseError struct {
	error
	StatusCode int
}

func NewResponseError(status int, msg interface{}, args ...interface{}) *ResponseError {
	switch msg.(type) {
	case error:
		return &ResponseError{
			error: msg.(error),
			StatusCode: status,
		}
	default:
		return &ResponseError{
			error: errors.New(fmt.Sprintf(msg.(string), args...)),
			StatusCode: status,
		}
	}
}

func (e *ResponseError) Status() string {
	return strconv.Itoa(e.StatusCode)
}
