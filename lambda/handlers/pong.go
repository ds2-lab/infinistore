package handlers

import (
	"context"
	"sync"
	"time"

	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/types"
	. "github.com/mason-leap-lab/infinicache/lambda/store"
)

var (
	RequestTimeout = 20 * time.Millisecond
	ContextKeyReady = "ready"
	DefaultPongTimeout = 30 * time.Millisecond
	DefaultRetry = 3
	log = Log
)

type pong func(*types.Response, int64) error

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

func (p *PongHandler) SendToConnection(ctx context.Context, conn *types.ProxyConnection, recover bool) error {
	if conn == nil {
		log.Debug("Issue pong, request fast recovery: %v", recover)
		ready := ctx.Value(&ContextKeyReady)
		close(ready.(chan struct{}))
		return nil
	}
	writer := types.NewResponse(conn, nil)   // One time per connection, so be it.
	return p.sendImpl(writer, recover)
}

func (p *PongHandler) SendTo(rsp *types.Response) error {
	return p.sendImpl(rsp, false)
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

func (p *PongHandler) sendImpl(rsp *types.Response, recover bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var retry int
	select {
	case retry = <-p.limiter:
		// Quota avaiable or abort.
	default:
		return nil
	}

	log.Debug("POND")

	flags := int64(0)
	if recover {
		flags += 0x01
	}

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
				p.sendImpl(rsp, recover)
			case <-p.done:
				return
			}
		}()
	}

	return nil
}

func (p *PongHandler) sendPong(rsp *types.Response, flags int64) (err error) {
	rsp.AppendBulkString("pong")
	rsp.AppendInt(int64(Store.Id()))
	rsp.AppendBulkString(lambdaLife.GetSession().Sid)
	rsp.AppendInt(flags)
	if err = rsp.Flush(RequestTimeout); err != nil {
		log.Error("Error on PONG flush: %v", err)
	}
	return
}
