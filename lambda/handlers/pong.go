package handlers

import (
	"context"
	"github.com/mason-leap-lab/redeo/resp"
	"net"
	"sync"
	"time"

	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	. "github.com/mason-leap-lab/infinicache/lambda/store"
)

var (
	ContextKeyReady = "ready"
	DefaultPongTimeout = 30 * time.Millisecond
	DefaultRetry = 3
	log = Log
)

type PongHandler struct {
	// Pong limiter prevent pong being sent duplicatedly on launching lambda while a ping arrives
	// at the same time.
	limiter chan int
	timeout *time.Timer
	mu      sync.Mutex
	done    chan struct{}
}

func NewPongHandler() *PongHandler {
	return &PongHandler{
		limiter: make(chan int, 1),
		timeout: time.NewTimer(0),
		done: make(chan struct{}, 1),
	}
}

func (p *PongHandler) Issue(retry bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	retryTime := 0
	if retry {
		retryTime = DefaultRetry
	}
	select {
	case p.limiter <- retryTime:
		return true
	default:
		// if limiter is full, move on
		return false
	}
}

func (p *PongHandler) SendToConnection(ctx context.Context, conn net.Conn, recover bool) error {
	if conn == nil {
		log.Debug("Issue pong, request fast recovery: %v", recover)
		ready := ctx.Value(&ContextKeyReady)
		close(ready.(chan struct{}))
		return nil
	}
	writer := resp.NewResponseWriter(conn)   // One time per connection, so be it.
	return p.sendImpl(writer, recover)
}

func (p *PongHandler) SendTo(w resp.ResponseWriter) error {
	return p.sendImpl(w, false)
}

func (p *PongHandler) Cancel() {
	p.mu.Lock()
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

func (p *PongHandler) sendImpl(w resp.ResponseWriter, recover bool) error {
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

	flag := int64(0)
	if recover {
		flag += 0x01
	}

	w.AppendBulkString("pong")
	w.AppendInt(int64(Store.Id()))
	w.AppendBulkString(lambdaLife.GetSession().Sid)
	w.AppendInt(flag)
	if err := w.Flush(); err != nil {
		log.Error("Error on PONG flush: %v", err)
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
				p.sendImpl(w, recover)
			case <-p.done:
				return
			}
		}()
	}

	return nil
}
