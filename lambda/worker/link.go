package worker

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/mason-leap-lab/infinicache/common/util/promise"
	"github.com/mason-leap-lab/redeo"
)

const (
	LinkUninitialized = 0
	LinkInitialized   = 1
	LinkClosed        = 2
)

var (
	ctxKeyLink    = struct{}{}
	ErrLinkClosed = errors.New("link closed")
)

// Wrapper for redeo client that support response buffering if connection is unavailable
type Link struct {
	*redeo.Client
	id        int
	addr      string
	ctrl      bool
	buff      []interface{}
	lastError error
	mu        sync.RWMutex
	once      int32
	token     *struct{}
	registry  interface{}
	acked     promise.Promise
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
		ctrl:  ctrl,
		buff:  make([]interface{}, 0, 1),
		acked: promise.Resolved(),
	}
}

func (ln *Link) Initialize() bool {
	atomic.CompareAndSwapInt32(&ln.once, LinkClosed, LinkUninitialized)
	return atomic.CompareAndSwapInt32(&ln.once, LinkUninitialized, LinkInitialized)
}

func (ln *Link) ID() int {
	return ln.id
}

func (ln *Link) String() string {
	if ln.ctrl {
		return ln.addr + "(c)"
	} else {
		return ln.addr + "(d)"
	}
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

	atomic.StoreInt32(&ln.once, LinkInitialized)
	ln.lastError = nil
	ln.Client = redeo.NewClient(conn)
	ln.Client.SetContext(context.WithValue(ln.Client.Context(), ctxKeyLink, ln))
	// Move cached responses, ln.buff can be nil
	if len(ln.buff) > 0 {
		go ln.migrate()
	}
}

// Add asynchronize response, error if the client is closed.
func (ln *Link) AddResponses(rsp interface{}) error {
	if atomic.LoadInt32(&ln.once) == LinkClosed {
		rsp.(Response).abandon(ErrLinkClosed)
		return ErrLinkClosed
	}

	ln.mu.Lock()
	defer ln.mu.Unlock()

	if atomic.LoadInt32(&ln.once) == LinkClosed {
		rsp.(Response).abandon(ErrLinkClosed)
		return ErrLinkClosed
	} else if ln.Client == nil {
		// Client Uninitialized
		ln.buff = append(ln.buff, rsp)
	} else if err := ln.Client.AddResponses(rsp); err != nil {
		// Client closed
		ln.buff = append(ln.buff, rsp)
	}
	return nil
}

func (ln *Link) GrantToken(token *struct{}) {
	ln.token = token
}

func (ln *Link) RevokeToken() *struct{} {
	token := ln.token
	if token == nil {
		return nil
	} else if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ln.token)), unsafe.Pointer(token), unsafe.Pointer(nil)) {
		return token
	} else {
		return nil
	}
}

func (ln *Link) Invalidate(err error) {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	// Set error first, so worker may read it later when it detect the failure.
	ln.lastError = err
	ln.close(true) // Force close.
}

func (ln *Link) IsClosed() bool {
	return atomic.LoadInt32(&ln.once) == LinkClosed
}

func (ln *Link) Close() {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	atomic.StoreInt32(&ln.once, LinkClosed)
	ln.close(false)
	// Drain responses, ln.buff can be nil
	if len(ln.buff) > 0 {
		for _, rsp := range ln.buff {
			rsp.(Response).abandon(ErrLinkClosed)
		}
	}
}

func (ln *Link) close(force bool) {
	if ln.Client != nil {
		conn := ln.Client.Conn()
		if force {
			// Don't use conn.Conn.Close(), it will stuck and wait for response. Too slow
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetLinger(0) // The operating system discards any unsent or unacknowledged data.
			}
		}
		conn.Close()
		ln.Client.Close()
		ln.Client = nil
	}
	ln.acked.Resolve()
}

func (ln *Link) migrate() {
	// Lock, so no response can be added to the link before migrated.
	ln.mu.Lock()
	defer ln.mu.Unlock()

	for _, rsp := range ln.buff {
		ln.Client.AddResponses(rsp)
	}
	ln.buff = ln.buff[:0]
}
