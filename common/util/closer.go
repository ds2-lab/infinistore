package util

import "sync/atomic"

type Closer struct {
	status uint32
	done   chan struct{}
}

func (c *Closer) Init() {
	c.status = 0
	c.done = make(chan struct{})
}

func (c *Closer) IsClosed() bool {
	return atomic.LoadUint32(&c.status) > 0
}

func (c *Closer) Close() {
	if !atomic.CompareAndSwapUint32(&c.status, 0, 1) {
		return
	}

	if c.done == nil {
		return
	}

	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *Closer) Wait() {
	if c.done == nil {
		return
	}

	<-c.done
}
