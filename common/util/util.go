package util

import (
	"errors"
	"io"
	"log"
	"net"
)

var (
	ErrPanicRecovered = errors.New("recovered from panic")
)

func Ifelse(expr bool, then interface{}, or interface{}) interface{} {
	if expr {
		return then
	} else {
		return or
	}
}

func IsConnectionFailed(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	} else if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || !netErr.Temporary()) {
		return true
	}

	return false
}

func PanicRecovery(from string, err *error) {
	if recovered := recover(); recovered != nil {
		log.Printf("Error: panic recovered from %s: %v", from, err)
		if err != nil {
			*err = ErrPanicRecovered
		}
	}
}
