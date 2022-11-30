package promise

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	PromiseInit = int64(0)
)

var (
	ErrResolved     = errors.New("resolved already")
	ErrTimeoutNoSet = errors.New("timeout not set")
	ErrTimeout      = errors.New("timeout")
)

type Promise interface {
	// Reset Reset promise
	Reset()

	// ResetWithOptions Reset promise will options
	ResetWithOptions(interface{})

	// SetTimeout Set how long the promise should timeout.
	SetTimeout(time.Duration)

	// Close Close the promise
	Close()

	// IsResolved If the promise is resolved
	IsResolved() bool

	// Get the time the promise last resolved. time.Time{} if the promise is unresolved.
	ResolvedAt() time.Time

	// Resolve Resolve the promise with value or (value, error)
	Resolve(...interface{}) (Promise, error)

	// Options Get options
	Options() interface{}

	// Value Get resolved value
	Value() interface{}

	// Result Helper function to get (value, error)
	Result() (interface{}, error)

	// Error Get last error on resolving
	Error() error

	// Timeout Return ErrTimeout if timeout, or ErrTimeoutNoSet if the timer not set.
	Timeout() error
}

func Resolved(rets ...interface{}) Promise {
	return ResolvedChannel(rets...)
}

func NewPromise() Promise {
	return NewChannelPromiseWithOptions(nil)
}

func NewPromiseWithOptions(opts interface{}) Promise {
	return NewChannelPromiseWithOptions(opts)
}

func InitPromise(promise *unsafe.Pointer, opts ...interface{}) Promise {
	if len(opts) == 0 {
		return InitPromise(promise, nil)
	}

	a := LoadPromise(promise)
	if a == nil {
		b := NewChannelPromiseWithOptions(opts[0])
		if atomic.CompareAndSwapPointer(promise, nil, unsafe.Pointer(b)) {
			return b
		} else {
			return LoadPromise(promise)
		}
	}
	return a
}

func LoadPromise(promise *unsafe.Pointer) Promise {
	if loaded := atomic.LoadPointer(promise); loaded != nil {
		return (*ChannelPromise)(loaded)
	}
	return nil
}
