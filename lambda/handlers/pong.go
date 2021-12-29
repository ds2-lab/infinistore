package handlers

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

var (
	DefaultPongTimeout = 500 * time.Millisecond
	DefaultAttempts    = 0 // Disable retrial for backend link intergrated retrial and reconnection.
	NoTimeout          = false

	Pong = NewPongHandler()

	errPongTimeout = errors.New("pong timeout")
)

type pong func(*worker.Link, int64) error

type fail func(*worker.Link, error)

type PongHandler struct {
	// Pong limiter prevent pong being sent duplicatedly on launching lambda while a ping arrives
	// at the same time.
	limiter   chan int
	mu        sync.Mutex
	pong      pong            // For test
	fail      fail            // For test
	requested promise.Promise // For ctrl link only
}

func NewPongHandler() *PongHandler {
	handler := &PongHandler{
		limiter:   make(chan int, 1),
		requested: promise.Resolved(),
	}
	handler.pong = sendPong
	handler.fail = setFailure
	return handler
}

func (p *PongHandler) Issue(retry bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	attempts := 0
	if retry {
		attempts = DefaultAttempts - 1
	}
	select {
	case p.limiter <- attempts:
		return true
	default:
		// if limiter is full, move on
		return false
	}
}

// Send Send ack(pong) on control link, must call Issue(bool) first. Pong will keep retrying until maximum attempts reaches or is cancelled.
func (p *PongHandler) Send() error {
	return p.sendImpl(protocol.PONG_FOR_CTRL, nil, false)
}

// Send Send ack(pong) with flags on control link, must call Issue(bool) first. Pong will keep retrying until maximum attempts reaches or is cancelled.
func (p *PongHandler) SendWithFlags(flags int64) error {
	return p.sendImpl(flags, nil, false)
}

// Send Send heartbeat on specified link.
func (p *PongHandler) SendToLink(link *worker.Link, flags int64) error {
	// if link.IsControl() {
	// 	return p.sendImpl(protocol.PONG_FOR_CTRL, link, false)
	// } else {
	// 	return p.sendImpl(protocol.PONG_FOR_DATA, link, false)
	// }
	return p.sendImpl(flags, link, false)
}

// Cancel Flag expected request is received and cancel pong retrial.
func (p *PongHandler) Cancel() {
	p.mu.Lock()
	p.requested.Resolve()
	// cancel limiter
	select {
	case <-p.limiter:
		// Quota avaiable or abort.
	default:
	}
	p.mu.Unlock()
}

// IsCancelled If the expected request has been received and pong has benn cancelled.
func (p *PongHandler) IsCancelled() bool {
	return p.requested.IsResolved()
}

func (p *PongHandler) sendImpl(flags int64, link *worker.Link, retrial bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	timeoutRequired := !NoTimeout && (link == nil || link.IsControl())

	attempts := 0
	// No retrial and multi-PONGs avoidance if the link is specified, which is triggered by the worker and will not duplicate.
	if timeoutRequired {
		select {
		case attempts = <-p.limiter:
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

	if timeoutRequired {
		// Refresh promise
		p.requested.Resolve()
		p.requested.Reset()
	}

	// Do send
	pongLog(flags, link)
	if err := p.pong(link, flags); err != nil {
		log.Error("Error on PONG flush: %v", err)
		return err
	}

	if !timeoutRequired {
		return nil
	} else if attempts > 0 {
		// To keep a ealier pong will always send first, occupy the limiter now.
		p.limiter <- attempts - 1

		// Set timeout
		p.requested.SetTimeout(DefaultPongTimeout)

		// Monitor and wait
		go func() {
			if p.requested.Timeout() != nil {
				log.Warn("PONG timeout, retry")
				p.sendImpl(flags, link, true)
			}
		}()
	} else {
		// For ack/pong, link will be disconnected if no attempt left.
		p.requested.SetTimeout(DefaultPongTimeout)

		// Monitor and wait
		go func() {
			if p.requested.Timeout() != nil {
				log.Warn("PONG timeout, disconnect")
				p.fail(link, &PongError{error: errPongTimeout, flags: flags})
			}
		}()
	}

	return nil
}

func pongLog(flags int64, link *worker.Link) {
	var claim string
	if flags > 0 {
		// These two claims are exclusive because backing only mode will enable reclaimation claim and disable fast recovery.
		if flags&protocol.PONG_RECOVERY > 0 {
			claim = " with fast recovery requested."
		} else if flags&protocol.PONG_RECLAIMED > 0 {
			claim = " with claiming the node has experienced reclaimation."
		}
	} else if link != nil {
		claim = fmt.Sprintf(" for link: %v", link)
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

func setFailure(link *worker.Link, err error) {
	store.Server.SetFailure(link, err)
}
