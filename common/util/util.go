package util

import (
	"io"
	"net"
)

func Ifelse(expr bool, then interface{}, or interface{}) interface{} {
	if expr {
		return then
	} else {
		return or
	}
}

func IsConnectionFailed(err error) bool {
	if err == io.EOF {
		return true
	} else if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || !netErr.Temporary()) {
		return true
	}

	return false
}
