package client

import "errors"

var (
	ErrInvalidSize         = errors.New("invalid size")
	ErrInvalidNumFragments = errors.New("invalid number of fragments")
	ErrAbandon             = errors.New("late chunk abandoned")
	ErrCorrupted           = errors.New("data corrupted")
)
