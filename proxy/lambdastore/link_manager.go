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

func (m *LinkManager) GetLastControl() *Connection {
	return m.lastValidCtrl
}

func (m *LinkManager) SetControl(link *Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log.Debug("Set control %v -> %v", m.ctrlLink, link)

	oldConn := m.ctrlLink

	// Set instance, order matters here.
	link.BindInstance(m.instance)
	m.ctrlLink = link

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
		if !m.addDataLinkLocked(link, false) {
			// Still failed? Abandon.
			link.Close()
		}
	}
	m.log.Debug("Set control done")
}

func (m *LinkManager) InvalidateControl(link manageableLink) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if link != m.ctrlLink {
		return
	}

	m.ctrlLink = nil
	m.log.Debug("Invalidated control %v", link)
}

func (m *LinkManager) AddDataLink(link *Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.addDataLinkLocked(link, true)
}

func (m *LinkManager) addDataLinkLocked(link *Connection, cache bool) bool {
	// Datalinks can come earlier than ctrl link.
	if m.ctrlLink == nil || m.ctrlLink.workerId != link.workerId {
		if cache {
			m.pendings.PushBack(link)
		}
		return false
	}

	link.Id = atomic.AddUint32(&m.seq, 1)
	link.BindInstance(m.instance)
	m.dataLinks.Insert(link.Id, link)
	m.availables.AddAvailable(link, true) // No limit control here
	m.log.Debug("Data link added:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
	return true
}

func (m *LinkManager) RemoveDataLink(link *Connection) {
	m.dataLinks.Del(link.Id)
	m.log.Debug("Data link removed:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
	// Data link being removed can be in avaiables already, we ignore this by considering following cases:
	// 1. New datalink will not be disconnected before first use.
	// 2. Reuse datalink will be closed (beyond ActiveLinks) and will not be added to availables.
	// 3. On function exists, all data links will be removed, and then resetLocked will be called sometime.
}

func (m *LinkManager) GetAvailableForRequest() *AvailableLink {
	return m.availables.GetRequestPipe()
}

func (m *LinkManager) FlagAvailableForRequest(link *Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	added := m.availables.AddAvailable(link, false)
	if added {
		m.log.Debug("Data link available: %d, all: %d", m.availables.Len(), m.dataLinks.Len())
	} else {
		m.RemoveDataLink(link)
		// Directly close connection for grace close. The link will close afterward.
		link.Conn.Close()
	}
	return added
}

func (m *LinkManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctrlLink != nil {
		m.ctrlLink.ClearResponses()
	}
	m.resetLocked(m.lastValidCtrl)
}

func (m *LinkManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctrlLink != nil {
		m.ctrlLink.Close()
		m.ctrlLink = nil
	}
	m.resetLocked(m.lastValidCtrl)
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
	m.log.Debug("Data link reset, availables: %d, all: %d", m.availables.Len(), m.dataLinks.Len())
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

type AvailableLink struct {
	link      manageableLink
	pipe      chan *types.Request
	err       error
	requested sync.WaitGroup
}

func (l *AvailableLink) Request() chan<- *types.Request {
	return l.pipe
}

func (l *AvailableLink) Error() error {
	l.requested.Wait()
	return l.err
}

type AvailableLinks struct {
	top    *LinkBucket
	bottom *LinkBucket
	tail   *LinkBucket
	total  int32
	limit  int

	// Offer pipe support
	linkRequest *AvailableLink

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
	return int(atomic.LoadInt32(&l.total))
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
	if l.linkRequest != nil {
		if l.linkRequest.link == nil {
			// Pipe is waiting for available link
			top := l.top
			l.top = l.nextBottomLocked(false)
			top.next = nil
			close(top.links) // Close channel to allow pipe routing continue. Pipe will close itself.
			l.linkRequest = nil
			return // Nothing left.
		} else {
			// Pipe is waiting for request, close it by force. Continue to check links left.
			// Drain the pipe and update counter here to keep the last statement (l.total = 0) safe
			select {
			case <-l.linkRequest.pipe:
			default:
			}
			close(l.linkRequest.pipe)
			atomic.AddInt32(&l.total, -1)
		}
		l.linkRequest = nil
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
	atomic.StoreInt32(&l.total, 0)
}

func (l *AvailableLinks) AddAvailable(link manageableLink, nolimit bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !nolimit && l.limit > 0 && int(atomic.LoadInt32(&l.total)) >= l.limit {
		return false
	}

	for {
		select {
		case l.bottom.links <- link:
			atomic.AddInt32(&l.total, 1)
			return true
		default:
			l.nextBottomLocked(true)
		}
	}
}

func (l *AvailableLinks) GetRequestPipe() *AvailableLink {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.linkRequest != nil {
		return l.linkRequest
	}

	l.linkRequest = &AvailableLink{pipe: make(chan *types.Request)}
	// Pipe go routing is not in the lock
	go func(al *AvailableLink) {
		// Wait for available connection
	Finding:
		for {
			select {
			case al.link = <-l.top.links:
				break Finding
			default:
				if l.top != l.bottom {
					// Looking for next bucket
					l.recycleTopBucket()
				} else {
					// Wait for next available connection
					al.link = <-l.top.links
					break Finding
				}
			}
		}
		if al.link == nil {
			// AvailableLinks is cleaning up and top.links is closed.
			// This setion is triggered by Reset() and locked by Reset()
			select {
			case <-al.pipe:
				al.err = ErrLinkManagerReset
			default:
			}
			close(al.pipe)
			return
		}

		// Consume request
		al.requested.Add(1)
		defer al.requested.Done()
		req := <-al.pipe
		if req == nil {
			// Pipe is closed
			// This setion is triggered by Reset() and locked by Reset()
			al.err = ErrLinkManagerReset
			return
		}

		// Link request is fulfilled
		l.mu.Lock()
		l.linkRequest = nil
		l.mu.Unlock()

		// Consume link
		atomic.AddInt32(&l.total, -1)
		al.err = al.link.SendRequest(req)
	}(l.linkRequest)

	return l.linkRequest
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
