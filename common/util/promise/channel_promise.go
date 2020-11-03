package promise

import (
	"sync"
	"sync/atomic"
	"time"
)

type ChannelPromise struct {
	cond     chan struct{}
	mu       sync.Mutex
	timer    *time.Timer
	resolved uint32

	Options interface{}
	val     interface{}
	err     error
}

func ResolvedChannel() *ChannelPromise {
	promise := NewChannelPromiseWithOptions(nil)
	close(promise.cond)
	promise.resolved = PromiseResolved
	return promise
}

func NewChannelPromise() *ChannelPromise {
	return NewChannelPromiseWithOptions(nil)
}

func NewChannelPromiseWithOptions(opts interface{}) *ChannelPromise {
	promise := &ChannelPromise{
		Options: opts,
	}
	promise.cond = make(chan struct{})
	return promise
}

func (p *ChannelPromise) Reset(opts interface{}) {
	p.ResetWithOptions(nil)
}

func (p *ChannelPromise) ResetWithOptions(opts interface{}) {
	atomic.StoreUint32(&p.resolved, PromiseInit)
	p.cond = make(chan struct{})
	p.Options = opts
	p.val = nil
	p.err = nil
}

func (p *ChannelPromise) SetTimeout(timeout time.Duration) {
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

func (p *ChannelPromise) Close() {

}

func (p *ChannelPromise) IsResolved() bool {
	return atomic.LoadUint32(&p.resolved) == PromiseResolved
}

func (p *ChannelPromise) Resolve(rets ...interface{}) (*ChannelPromise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.cond:
		return p, ErrResolved
	default:
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
		close(p.cond)
	}
	return p, nil
}

func (p *ChannelPromise) Value() interface{} {
	<-p.cond
	return p.val
}

func (p *ChannelPromise) Result() (interface{}, error) {
	<-p.cond
	return p.val, p.err
}

func (p *ChannelPromise) Error() error {
	<-p.cond
	return p.err
}

func (p *ChannelPromise) Timeout() error {
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
	select {
	case <-timer.C:
		return ErrTimeout
	case <-p.cond:
		if !timer.Stop() {
			<-timer.C
		}
		return nil
	}
}
