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
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe {
		return true
	} else if _, ok := err.(net.Error); ok {
		// All net.Error counts, they are either timeout or permanent(non-temporary) error.
		return true
	}

	return false
}

func PanicRecovery(from string, err *error) {
	if recovered := recover(); recovered != nil {
		log.Printf("Error: panic recovered from %s: %v", from, recovered)
		if err != nil {
			*err = ErrPanicRecovered
		}
	}
}

type VerboseCloser interface {
	CloseWithReason(reason string) error
}

func CloseWithReason(c io.Closer, reason string) error {
	if v, ok := c.(VerboseCloser); ok {
		return v.CloseWithReason(reason)
	} else {
		return c.Close()
	}
}
