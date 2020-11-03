package promise

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PromiseInit     = 0
	PromiseResolved = 1
)

var (
	ErrResolved     = errors.New("resolved already")
	ErrTimeoutNoSet = errors.New("timeout not set")
	ErrTimeout      = errors.New("timeout")
)

type Promise struct {
	cond     *sync.Cond
	mu       sync.Mutex
	timer    *time.Timer
	resolved uint32

	Options interface{}
	val     interface{}
	err     error
}

func Resolved() *Promise {
	promise := NewPromiseWithOptions(nil)
	promise.resolved = PromiseResolved
	return promise
}

func NewPromise() *Promise {
	return NewPromiseWithOptions(nil)
}

func NewPromiseWithOptions(opts interface{}) *Promise {
	promise := &Promise{
		Options: opts,
	}
	promise.cond = sync.NewCond(&promise.mu)
	return promise
}

func (p *Promise) Reset(opts interface{}) {
	p.ResetWithOptions(nil)
}

func (p *Promise) ResetWithOptions(opts interface{}) {
	atomic.StoreUint32(&p.resolved, PromiseInit)
	p.Options = opts
	p.val = nil
	p.err = nil
}

func (p *Promise) SetTimeout(timeout time.Duration) {
	if p.IsResolved() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check again
	if p.IsResolved() {
		return
	}

	if p.timer == nil {
		p.timer = time.NewTimer(timeout)
		return
	}
	if !p.timer.Stop() {
		<-p.timer.C
	}
	p.timer.Reset(timeout)
}

func (p *Promise) Close() {

}

func (p *Promise) IsResolved() bool {
	return atomic.LoadUint32(&p.resolved) == PromiseResolved
}

func (p *Promise) Resolve(rets ...interface{}) (*Promise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadUint32(&p.resolved) == PromiseResolved {
		return p, ErrResolved
	}

	switch len(rets) {
	case 0:
		break
	case 1:
		p.val = rets[0]
	default:
		p.val = rets[0]
		if rets[1] == nil {
			p.err = nil
		} else {
			p.err = rets[1].(error)
		}
	}
	atomic.StoreUint32(&p.resolved, PromiseResolved)
	p.cond.Broadcast()
	if p.timer != nil {
		p.stopTimerLocked()
	}
	return p, nil
}

func (p *Promise) Value() interface{} {
	p.wait()
	return p.val
}

func (p *Promise) Result() (interface{}, error) {
	p.wait()
	return p.val, p.err
}

func (p *Promise) Error() error {
	p.wait()
	return p.err
}

func (p *Promise) Timeout() error {
	p.mu.Lock()
	timer := p.timer
	p.timer = nil

	if p.IsResolved() {
		p.mu.Unlock()
		return nil
	}

	p.mu.Unlock()

	if timer == nil {
		return ErrTimeoutNoSet
	}
	<-timer.C
	if p.IsResolved() {
		return nil
	} else {
		return ErrTimeout
	}
}

func (p *Promise) wait() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for atomic.LoadUint32(&p.resolved) != PromiseResolved {
		p.cond.Wait()
	}
}

func (p *Promise) stopTimerLocked() {
	if p.timer == nil {
		return
	} else if p.timer.Stop() {
		p.timer.Reset(time.Duration(0))
	}
}
