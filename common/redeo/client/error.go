package client

import "errors"

var (
	ErrWindow  = errors.New("window error")
	ErrTimeout = &timeoutError{}
)

type windowError struct {
	error
}

func newWindowError(text string) error {
	return &windowError{error: errors.New(text)}
}

func (e *windowError) Is(target error) bool { return target == ErrWindow }

func IsWindoeError(err error) bool {
	return errors.Is(err, ErrWindow)
}

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
