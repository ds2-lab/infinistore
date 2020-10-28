package worker

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/redeo"
)

const (
	LinkUninitialized = 0
	LinkInitialized   = 1
)

var (
	ctxKeyLink = struct{}{}
)

// Wrapper for redeo client that support response buffering if connection is unavailable
type Link struct {
	id int
	*redeo.Client
	ctrl bool
	buff chan interface{}
	mu   sync.RWMutex
	once int32
}

func LinkFromClient(client *redeo.Client) *Link {
	if client == nil {
		return nil
	}

	if link, ok := client.Context().Value(ctxKeyLink).(*Link); ok {
		return link
	}

	return nil
}

func NewLink(ctrl bool) *Link {
	return &Link{
		ctrl: ctrl,
		buff: make(chan interface{}, 1),
	}
}

func (ln *Link) Initialize() bool {
	return atomic.CompareAndSwapInt32(&ln.once, LinkUninitialized, LinkInitialized)
}

func (ln *Link) ID() int {
	return ln.id
}

func (ln *Link) IsControl() bool {
	return ln.ctrl
}

func (ln *Link) Reset(conn net.Conn) {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	if conn == nil {
		ln.Client = nil
		return
	}

	ln.Client = redeo.NewClient(conn)
	ln.Client.SetContext(context.WithValue(ln.Client.Context(), ctxKeyLink, ln))
	ln.Client.SetContext(context.WithValue(ln.Client.Context(), ctxKeyLink, ln))
	// Move cached responses
	if len(ln.buff) > 0 {
		go ln.migrate()
	}
}

// Add asynchronize response, error if the client is closed.
func (ln *Link) AddResponses(rsp interface{}) error {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	if atomic.LoadInt32(&ln.once) == LinkUninitialized {
		rsp.(Response).abandon(ErrWorkerClosed)
		return ErrWorkerClosed
	} else if ln.Client == nil {
		ln.buff <- rsp
	} else if err := ln.Client.AddResponses(rsp); err != nil {
		ln.buff <- rsp
	}
	return nil
}

func (ln *Link) Close() {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	if ln.Client != nil {
		conn := ln.Client.Conn()
		// Don't use conn.Close(), it will stuck and wait.
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetLinger(0) // The operating system discards any unsent or unacknowledged data.
		}
		conn.Close()
		ln.Client.Close()
		ln.Client = nil
	}
	// Drain responses
	if len(ln.buff) > 0 {
		for rsp := range ln.buff {
			rsp.(Response).abandon(ErrWorkerClosed)
			if len(ln.buff) == 0 {
				break
			}
		}
	}
	atomic.StoreInt32(&ln.once, LinkUninitialized)
}

func (ln *Link) migrate() {
	// Lock, so no response can be added to the link before migrated.
	ln.mu.Lock()
	defer ln.mu.Unlock()

	for rsp := range ln.buff {
		ln.Client.AddResponses(rsp)
		if len(ln.buff) == 0 {
			break
		}
	}
}
