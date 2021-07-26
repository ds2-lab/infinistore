package client

import "errors"

var (
	ErrWindow = errors.New("windoe error")
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
