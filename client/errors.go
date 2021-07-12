package client

import "errors"

var (
	ErrInvalidSize         = errors.New("invalid size")
	ErrInvalidNumFragments = errors.New("invalid number of fragments")
)
