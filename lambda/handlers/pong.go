package handlers

import (
	"context"
	"sync"
	"time"
	"net"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	. "github.com/mason-leap-lab/infinicache/lambda/store"
)

var (
	ContextKeyReady = "ready"
	DefaultPongTimeout = 30 * time.Millisecond
	DefaultRetry = 3
	log = Log
)

type pong func(*Response, int64) error

type PongHandler struct {
	// Pong limiter prevent pong being sent duplicatedly on launching lambda while a ping arrives
	// at the same time.
	limiter chan int
	timeout *time.Timer
	mu      sync.Mutex
	done    chan struct{}
	pong    pong
	canceled bool
}

func NewPongHandler() *PongHandler {
	handler := &PongHandler{
		limiter: make(chan int, 1),
		timeout: time.NewTimer(0),
		done: make(chan struct{}, 1),
	}
	handler.pong = handler.sendPong
	return handler
}

func (p *PongHandler) Issue(retry bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	retries := 0
	if retry {
		retries = DefaultRetry
	}
	select {
	case p.limiter <- retries:
		p.canceled = false
		return true
	default:
		// if limiter is full, move on
		return false
	}
}

func (p *PongHandler) SendToConnection(ctx context.Context, conn net.Conn, flags int64) error {
	if conn == nil {
		pongLog(flags)
		ready := ctx.Value(&ContextKeyReady)
		close(ready.(chan struct{}))
		return nil
	}
	writer := NewResponse(conn, nil)   // One time per connection, so be it.
	return p.sendImpl(writer, flags)
}

func (p *PongHandler) SendTo(rsp *Response) error {
	return p.sendImpl(rsp, 0)
}

func (p *PongHandler) Cancel() {
	p.mu.Lock()
	p.canceled = true
	select {
	case p.done <- struct{}{}:
	default:
	}
	// cancel limiter
	select {
	case <-p.limiter:
		// Quota avaiable or abort.
	default:
	}
	p.mu.Unlock()
}

func (p *PongHandler) IsCancelled() bool {
	return p.canceled
}

func (p *PongHandler) sendImpl(rsp *Response, flags int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var retry int
	select {
	case retry = <-p.limiter:
		// Quota avaiable or abort.
	default:
		return nil
	}

	pongLog(flags)

	if err := p.pong(rsp, flags); err != nil {
		return err
	}

	// To keep a ealier pong will always send first, occupy the limiter now.
	if retry > 0 {
		p.limiter <- retry - 1

		// Drain timer
		if !p.timeout.Stop() {
			select {
			case <-p.timeout.C:
			default:
			}
		}
		p.timeout.Reset(DefaultPongTimeout)

		// Drain done
		select {
		case <-p.done:
		default:
		}

		// Monitor and wait
		go func() {
			select {
			case <-p.timeout.C:
				// Timeout. retry
				log.Warn("retry PONG")
				p.sendImpl(rsp, flags)
			case <-p.done:
				return
			}
		}()
	}

	return nil
}

func pongLog(flags int64) {
	var claim string
	if flags > 0 {
		// These two claims are exclusive because backing only mode will enable reclaimation claim and disable fast recovery.
		if flags & protocol.PONG_RECOVERY > 0 {
			claim = " with fast recovery requested."
		} else if flags & protocol.PONG_RECLAIMED > 0 {
			claim = " with claiming the node has experienced reclaimation."
		}
	}
	log.Debug("POND%s", claim)
}

func (p *PongHandler) sendPong(rsp *Response, flags int64) (err error) {
	rsp.AppendBulkString("pong")
	rsp.AppendInt(int64(Store.Id()))
	rsp.AppendBulkString(lambdaLife.GetSession().Sid)
	rsp.AppendInt(flags)
	if err = rsp.Flush(); err != nil {
		log.Error("Error on PONG flush: %v", err)
	}
	return
}
