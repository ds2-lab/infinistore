package lambdastore

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/zhangjyr/hashmap"
)

const (
	LinkBucketSize       = 10
	UnlimitedActiveLinks = 0
	ActiveLinks          = 1
)

var (
	ErrLinkManagerReset = errors.New("link manager reset")
)

type manageableLink interface {
	SendRequest(*types.Request, ...interface{}) error
	Close() error
}

type LinkManager struct {
	seq           uint32
	instance      *Instance
	ctrlLink      *Connection
	lastValidCtrl *Connection

	// Available links, expandable
	availables *AvailableLinks

	dataLinks *hashmap.HashMap
	pendings  *list.List

	log logger.ILogger
	mu  sync.Mutex
}

func NewLinkManager(ins *Instance) *LinkManager {
	return &LinkManager{
		instance:   ins,
		availables: newAvailableLinks(),
		dataLinks:  hashmap.New(100),
		pendings:   list.New(),
		log:        ins.log,
	}
}

func (m *LinkManager) GetControl() *Connection {
	return m.ctrlLink
}

func (m *LinkManager) SetControl(link *Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldConn := m.ctrlLink

	// Set instance, order matters here.
	link.BindInstance(m.instance)
	m.ctrlLink = link
	m.log = link.log

	// Clean up if worker(lambda) changed. To deal temprorary invalidated control, we check lastValidCtrl instead of oldConn
	if m.lastValidCtrl != nil && link.workerId != m.lastValidCtrl.workerId {
		m.resetLocked(m.lastValidCtrl)
	}
	if oldConn != nil {
		oldConn.Close()
	}
	m.lastValidCtrl = link

	// Check pendings
	for i := m.pendings.Front(); i != nil; i = m.pendings.Front() {
		link := m.pendings.Remove(i).(*Connection)
		if !m.addDataLinkLocked(link) {
			// Still failed? Abandon.
			link.Close()
		}
	}
}

func (m *LinkManager) InvalidateControl(link manageableLink) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if link != m.ctrlLink {
		return
	}

	m.ctrlLink = nil
}

func (m *LinkManager) AddDataLink(link *Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.addDataLinkLocked(link)
}

func (m *LinkManager) addDataLinkLocked(link *Connection) bool {
	link.BindInstance(m.instance)
	// Datalinks can come earlier than ctrl link.
	if m.ctrlLink == nil || m.ctrlLink.workerId != link.workerId {
		m.pendings.PushBack(link)
		return false
	}

	link.Id = atomic.AddUint32(&m.seq, 1)
	m.dataLinks.Insert(link.Id, link)
	m.availables.AddAvailable(link) // No limit control here
	m.log.Debug("Data link added:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
	return true
}

func (m *LinkManager) RemoveDataLink(link *Connection) {
	m.dataLinks.Del(link.Id)
	m.log.Debug("Data link removed:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
}

func (m *LinkManager) GetAvailableForRequest() chan<- *types.Request {
	return m.availables.GetRequestPipe()
}

func (m *LinkManager) GetLastRequestError() error {
	return m.availables.lastError
}

func (m *LinkManager) FlagAvailableForRequest(link *Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	added := m.availables.AddAvailable(link)
	if !added {
		m.log.Debug("Data link available: %d, all: : %d", m.availables.Len(), m.dataLinks.Len())
	} else {
		m.RemoveDataLink(link)
		link.Close()
	}
	return added
}

func (m *LinkManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctrlLink == nil {
		return
	}
	m.ctrlLink.ClearResponses()
	m.resetLocked(m.ctrlLink)
}

func (m *LinkManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctrlLink == nil {
		return
	}
	m.resetLocked(m.ctrlLink)
	m.ctrlLink.Close()
	m.ctrlLink = nil
	m.lastValidCtrl = nil
}

func (m *LinkManager) resetLocked(legacy *Connection) {
	if legacy == nil {
		return
	}

	m.availables.Reset()
	for keyVal := range m.dataLinks.Iter() {
		conn := keyVal.Value.(*Connection)
		// Filter out unrelated connections
		if conn.workerId == legacy.workerId {
			conn.Close()
			m.dataLinks.Del(keyVal.Key)
		}
	}
}

type LinkBucket struct {
	links chan manageableLink
	next  *LinkBucket
}

func (b *LinkBucket) Reset() {
	for {
		select {
		case <-b.links:
		default:
			return
		}
	}
}

type AvailableLinks struct {
	top    *LinkBucket
	bottom *LinkBucket
	tail   *LinkBucket
	total  int
	limit  int

	// Offer pipe support
	lastPipe  chan *types.Request
	lastLink  manageableLink
	lastError error

	mu sync.Mutex
}

func newAvailableLinks() *AvailableLinks {
	links := &AvailableLinks{
		top:   &LinkBucket{links: make(chan manageableLink, LinkBucketSize)},
		limit: ActiveLinks,
	}
	links.bottom = links.top
	links.tail = links.top
	return links
}

func (l *AvailableLinks) Len() int {
	return l.total
}

func (l *AvailableLinks) SetLimit(limit int) int {
	old := l.limit
	l.limit = limit
	return old
}

func (l *AvailableLinks) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Clean pipe
	if l.lastPipe != nil {
		if l.lastLink == nil {
			// Pipe is waiting for available link
			top := l.top
			l.top = l.nextBottomLocked(false)
			top.next = nil
			close(top.links) // Close channel to allow pipe routing continue. Pipe will close itself.
			return           // Nothing left.
		} else {
			// Pipe is waiting for request, close it by force. Continue to check links left.
			close(l.lastPipe)
			l.total--
		}
	}

	if len(l.top.links) == 0 {
		return
	}

	// Drain top
	for l.top != l.bottom {
		l.top.Reset()
		l.top = l.recycleBucketLocked(l.top)
	}
	l.top.Reset()

	// Reset availables
	l.total = 0
}

func (l *AvailableLinks) AddAvailable(link manageableLink) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.limit > 0 && l.total >= l.limit {
		return false
	}

	for {
		select {
		case l.bottom.links <- link:
			l.total++
			return true
		default:
			l.nextBottomLocked(true)
		}
	}
}

func (l *AvailableLinks) GetRequestPipe() chan<- *types.Request {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lastPipe != nil {
		return l.lastPipe
	}

	l.lastError = nil
	l.lastPipe = make(chan *types.Request)

	// Pipe go routing is not in the lock
	go func(pipe chan *types.Request) {
		// Wait for available connection
	Finding:
		for {
			select {
			case l.lastLink = <-l.top.links:
				break Finding
			default:
				if l.top != l.bottom {
					// Looking for next bucket
					l.recycleTopBucket()
				} else {
					// Wait for next available connection
					l.lastLink = <-l.top.links
					break Finding
				}
			}
		}
		// AvailableLinks is cleaning up.
		if l.lastLink == nil {
			select {
			case <-pipe:
				l.lastError = ErrLinkManagerReset
			default:
			}
			l.lastPipe = nil
			close(pipe)
			return
		}

		// Consume request
		req := <-pipe
		if req != nil {
			l.total-- // In case cleaning up, Reset will reset the availables.
			l.lastError = l.lastLink.SendRequest(req)
		} else {
			// Or pipe is closed
			l.lastError = ErrLinkManagerReset
		}
		l.lastLink = nil
		l.lastPipe = nil
	}(l.lastPipe)

	return l.lastPipe
}

func (l *AvailableLinks) nextBottomLocked(check bool) *LinkBucket {
	if check && len(l.bottom.links) < LinkBucketSize {
		return l.bottom
	}

	if l.bottom == l.tail {
		l.tail.next = &LinkBucket{links: make(chan manageableLink, LinkBucketSize)}
		l.tail = l.tail.next
	}

	l.bottom = l.bottom.next
	return l.bottom
}

func (l *AvailableLinks) recycleTopBucket() (*LinkBucket, bool) {
	// Ensure top != bottom, and so does top != tail
	if l.top == l.bottom || len(l.top.links) > 0 {
		return l.top, false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.top = l.recycleBucketLocked(l.top)
	return l.top, true
}

func (l *AvailableLinks) recycleBucketLocked(bucket *LinkBucket) *LinkBucket {
	l.tail.next = bucket // Append old top to tail
	l.tail = bucket      // Update tail to old top
	next := bucket.next
	l.tail.next = nil
	return next
}
