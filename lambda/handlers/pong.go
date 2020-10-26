package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

var (
	ContextKeyReady    = "ready"
	DefaultPongTimeout = 30 * time.Millisecond
	DefaultRetry       = 0 // Disable retrial for backend link intergrated retrial and reconnection.

	log = store.Log
)

type pong func(*worker.Link, int64) error

type PongHandler struct {
	// Pong limiter prevent pong being sent duplicatedly on launching lambda while a ping arrives
	// at the same time.
	limiter  chan int
	timeout  *time.Timer
	mu       sync.Mutex
	done     chan struct{}
	pong     pong // For test
	canceled bool
}

func NewPongHandler() *PongHandler {
	handler := &PongHandler{
		limiter: make(chan int, 1),
		timeout: time.NewTimer(0),
		done:    make(chan struct{}, 1),
	}
	handler.pong = sendPong
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

func (p *PongHandler) SendWithFlags(ctx context.Context, flags int64) error {
	if ctx != nil {
		ready := ctx.Value(&ContextKeyReady)
		if ready != nil {
			pongLog(flags, false)
			ready.(chan struct{}) <- struct{}{}
			return nil
		}
	}
	return p.sendImpl(flags, nil)
}

func (p *PongHandler) Send() error {
	return p.sendImpl(0, nil)
}

func (p *PongHandler) SendToLink(link *worker.Link) error {
	if link.IsControl() {
		return p.sendImpl(protocol.PONG_FOR_CTRL, link)
	} else {
		return p.sendImpl(protocol.PONG_FOR_DATA, link)
	}
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

func (p *PongHandler) sendImpl(flags int64, link *worker.Link) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	retry := 0
	// No retry and multiple PONGs avoidance if the link is specified, which is triggered by the worker and will not duplicate.
	if link == nil {
		select {
		case retry = <-p.limiter:
			// Quota avaiable or abort.
		default:
			return nil
		}
	}

	// Guard for session
	if lambdaLife.GetSession() == nil {
		// Abandon
		return nil
	}
	pongLog(flags, link != nil)
	if err := p.pong(link, flags); err != nil {
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
				p.sendImpl(flags, link)
			case <-p.done:
				return
			}
		}()
	}

	return nil
}

func pongLog(flags int64, forLink bool) {
	var claim string
	if flags > 0 {
		// These two claims are exclusive because backing only mode will enable reclaimation claim and disable fast recovery.
		if flags&protocol.PONG_RECOVERY > 0 {
			claim = " with fast recovery requested."
		} else if flags&protocol.PONG_RECLAIMED > 0 {
			claim = " with claiming the node has experienced reclaimation."
		}
	} else if forLink {
		claim = " for link."
	}
	log.Debug("PONG%s", claim)
}

func sendPong(link *worker.Link, flags int64) error {
	store.Server.AddResponsesWithPreparer(protocol.CMD_PONG, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		rsp.Attempts = 1
		// CMD
		w.AppendBulkString(rsp.Cmd)
		// WorkerID + StoreID
		// fmt.Printf("store id:%d, worker id:%d, sent: %d\n", store.Store.Id(), store.Server.Id(), int64(store.Store.Id())+int64(store.Server.Id())<<32)
		w.AppendInt(int64(store.Store.Id()) + int64(store.Server.Id())<<32)
		// Sid
		w.AppendBulkString(lambdaLife.GetSession().Sid)
		// Flags
		w.AppendInt(flags)
	}, link)
	// return rsp.Flush()
	return nil
}
