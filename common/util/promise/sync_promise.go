package promise

import (
	"sync"
	"sync/atomic"
	"time"
)

type SyncPromise struct {
	cond     *sync.Cond
	mu       sync.Mutex
	timer    *time.Timer
	resolved int64

	options interface{}
	val     interface{}
	err     error
}

func ResolvedSync(rets ...interface{}) *SyncPromise {
	promise := NewSyncPromiseWithOptions(nil)
	if promise.resolve(rets...) {
		promise.resolved = time.Now().UnixNano()
	} else {
		promise.resolved = int64(1) // Differentiate with PromiseInit
	}
	return promise
}

func NewSyncPromise() *SyncPromise {
	return NewSyncPromiseWithOptions(nil)
}

func NewSyncPromiseWithOptions(opts interface{}) *SyncPromise {
	promise := &SyncPromise{
		options: opts,
	}
	promise.cond = sync.NewCond(&promise.mu)
	return promise
}

func (p *SyncPromise) Reset() {
	p.ResetWithOptions(nil)
}

func (p *SyncPromise) ResetWithOptions(opts interface{}) {
	atomic.StoreInt64(&p.resolved, PromiseInit)
	p.options = opts
	p.val = nil
	p.err = nil
}

func (p *SyncPromise) SetTimeout(timeout time.Duration) {
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

func (p *SyncPromise) Close() {

}

func (p *SyncPromise) IsResolved() bool {
	return atomic.LoadInt64(&p.resolved) != PromiseInit
}

func (p *SyncPromise) ResolvedAt() time.Time {
	ts := atomic.LoadInt64(&p.resolved)
	if ts == PromiseInit {
		return time.Time{}
	} else {
		return time.Unix(0, ts)
	}
}

func (p *SyncPromise) Resolve(rets ...interface{}) (Promise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt64(&p.resolved) != PromiseInit {
		return p, ErrResolved
	}

	p.resolve(rets...)
	atomic.StoreInt64(&p.resolved, time.Now().UnixNano())
	p.cond.Broadcast()
	if p.timer != nil {
		p.stopTimerLocked()
	}
	return p, nil
}

func (p *SyncPromise) Options() interface{} {
	return p.options
}

func (p *SyncPromise) Value() interface{} {
	p.wait()
	return p.val
}

func (p *SyncPromise) Result() (interface{}, error) {
	p.wait()
	return p.val, p.err
}

func (p *SyncPromise) Error() error {
	p.wait()
	return p.err
}

func (p *SyncPromise) Timeout() error {
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

func (p *SyncPromise) resolve(rets ...interface{}) bool {
	switch len(rets) {
	case 0:
		return false
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
	return true
}

func (p *SyncPromise) wait() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for atomic.LoadInt64(&p.resolved) == PromiseInit {
		p.cond.Wait()
	}
}

func (p *SyncPromise) stopTimerLocked() {
	if p.timer == nil {
		return
	} else if p.timer.Stop() {
		p.timer.Reset(time.Duration(0))
	}
}
