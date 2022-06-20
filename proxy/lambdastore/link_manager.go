package lambdastore

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	LinkBucketSize       = 10
	UnlimitedActiveLinks = 0
	ActiveLinks          = 1
)

var (
	ErrLinkRequestClosed  = &LambdaError{error: errors.New("link request closed")}
	ErrLinkRequestTimeout = &LambdaError{error: errors.New("link request timeout"), typ: LambdaErrorTimeout}
	ErrLinkManagerReset   = &LambdaError{error: errors.New("link manager reset")}
	ErrNilLink            = &LambdaError{error: errors.New("unexpected nil link")}

	// Keep following variables as false. They are only for unit tests.
	UnitTestMTC1 = false
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

	dataLinks hashmap.HashMap
	pendings  *list.List

	log logger.ILogger
	mu  sync.Mutex
}

func NewLinkManager(ins *Instance) *LinkManager {
	lm := &LinkManager{
		instance:   ins,
		availables: newAvailableLinks(),
		dataLinks:  hashmap.NewMap(100),
		pendings:   list.New(),
		log:        ins.log,
	}
	lm.availables.log = ins.log
	return lm
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

func (m *LinkManager) SetMaxActiveDataLinks(num int) {
	m.availables.SetLimit(num)
}

func (m *LinkManager) DataLinks() *AvailableLinks {
	return m.availables
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
	m.dataLinks.Store(link.Id, link)
	m.availables.AddAvailable(link, true) // No limit control here
	m.log.Debug("Data link added:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
	return true
}

func (m *LinkManager) RemoveDataLink(link *Connection) {
	m.dataLinks.Delete(link.Id)
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
		m.log.Debug("Data link reused:%v, availables: %d, all: %d", link, m.availables.Len(), m.dataLinks.Len())
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
	m.dataLinks.Range(func(key interface{}, value interface{}) bool {
		conn := value.(*Connection)
		// Filter out unrelated connections
		if conn.workerId == legacy.workerId {
			conn.Close()
			m.dataLinks.Delete(key)
		}
		return true
	})
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
	links *AvailableLinks
	elem  *list.Element

	link   manageableLink
	pipe   chan *types.Request
	err    error
	closed chan struct{}
}

func (l *AvailableLink) SetTimeout(d time.Duration) {
	go func() {
		<-time.After(d)
		if l.link == nil {
			if UnitTestMTC1 {
				time.Sleep(100 * time.Millisecond)
			}
			l.links.resetLinkRequest(l, ErrLinkRequestTimeout)
		}
	}()
}

func (l *AvailableLink) Request() chan<- *types.Request {
	return l.pipe
}

func (l *AvailableLink) Close() {
	l.links.resetLinkRequest(l, ErrLinkRequestClosed)
}

func (l *AvailableLink) Closed() <-chan struct{} {
	return l.closed
}

func (l *AvailableLink) Error() error {
	<-l.closed
	return l.err
}

type AvailableLinks struct {
	top    *LinkBucket
	bottom *LinkBucket
	tail   *LinkBucket
	total  int32
	limit  int

	// Offer pipe support
	linkRequests list.List

	log logger.ILogger
	mu  sync.Mutex
}

func newAvailableLinks() *AvailableLinks {
	links := &AvailableLinks{
		top:   &LinkBucket{links: make(chan manageableLink, LinkBucketSize)},
		limit: ActiveLinks,
		log:   logger.NilLogger,
	}
	links.bottom = links.top
	links.tail = links.top
	links.linkRequests.Init()
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

func (l *AvailableLinks) OffsetLimit(offset int, max int) int {
	old := l.limit
	new := l.limit + offset
	if new < 1 {
		l.limit = 1
	} else if new > max {
		l.limit = max
	} else {
		l.limit = new
	}
	return old
}

func (l *AvailableLinks) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Clean pipe
	for elem := l.linkRequests.Front(); elem != nil; elem = l.linkRequests.Front() {
		l.resetLinkRequestLocked(elem.Value.(*AvailableLink), ErrLinkManagerReset)
	}

	if len(l.top.links) == 0 {
		return
	}

	// Drain top
	for l.top != l.bottom {
		l.top.Reset()
		l.top = l.recycleTopLocked(l.top)
	}
	l.top.Reset()

	// Reset availables
	atomic.StoreInt32(&l.total, 0)
}

func (l *AvailableLinks) AddAvailable(link manageableLink, nolimit bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.addAvailableLocked(link, nolimit)
}

func (l *AvailableLinks) GetRequestPipe() *AvailableLink {
	l.mu.Lock()
	defer l.mu.Unlock()

	al := &AvailableLink{
		links:  l,
		pipe:   make(chan *types.Request),
		closed: make(chan struct{}),
	}
	al.elem = l.linkRequests.PushBack(al)
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
					// Reach end of bucket, looking for next bucket.
					l.recycleTopBucket()
				} else {
					// Wait for next available connection
					select {
					case <-al.closed:
						// al.link has not been set, it's safe to return
						return
					case al.link = <-l.top.links:
						break Finding
					}
				}
			}
		}
		if al.link == nil {
			al.links.resetLinkRequest(al, ErrNilLink)
			return
		}

		// Consume request. For support multi-source, the pipe will not be closed. Use select al.Closed() to unblock duplicated request.
		var req *types.Request
		select {
		case <-al.closed:
			// continue to handle link
		case req = <-al.pipe:
			// Link request is fulfilled

			if UnitTestMTC1 {
				time.Sleep(100 * time.Millisecond)
			}

			// Double check
			l.mu.Lock()
			if al.elem == nil {
				req = nil // reset req to flag link request is closed
			} else {
				// total := atomic.AddInt32(&l.total, -1)
				al.links.linkRequests.Remove(al.elem) // Ready to close
				al.elem = nil
			}
			l.mu.Unlock()
		}
		var availables int32
		if al.link != nil {
			// Update counter.
			// If resetLinkRequestLocked handled the link on ErrLinkManagerReset, link was reset.
			availables = atomic.AddInt32(&l.total, -1)
		}

		if req == nil {
			// handle link, RESET will remove all links anyway.
			if al.link != nil && al.err != ErrLinkManagerReset {
				// Reuse the link, addAvailableLocked will add l.total back.
				l.addAvailableLocked(al.link, true)
			}
			al.link = nil
			return
		}

		// Consume link
		al.err = al.link.SendRequest(req)
		close(al.closed) // close al only after got err.
		al.links.log.Debug("Data link consumed:%v, available: %d", al.link, availables)
	}(al)

	return al
}

func (l *AvailableLinks) addAvailableLocked(link manageableLink, nolimit bool) bool {
	if !nolimit && l.limit > 0 && int(atomic.LoadInt32(&l.total)) >= l.limit {
		// log.Printf("limited? %d, %d", l.limit, atomic.LoadInt32(&l.total))
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

	// Ensure top != bottom, and so does top != tail
	if l.top == l.bottom || len(l.top.links) > 0 {
		return l.top, false
	}

	l.top = l.recycleTopLocked(l.top)
	return l.top, true
}

func (l *AvailableLinks) recycleTopLocked(bucket *LinkBucket) *LinkBucket {
	l.tail.next = bucket // Append old top to tail
	l.tail = bucket      // Update tail to old top
	next := bucket.next
	l.tail.next = nil
	return next
}

func (l *AvailableLinks) resetLinkRequest(al *AvailableLink, err error) {
	if al == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.resetLinkRequestLocked(al, err)
}

func (l *AvailableLinks) resetLinkRequestLocked(al *AvailableLink, err error) {
	// al is not necessarily closed if request is sending.
	if al.elem == nil {
		return
	}
	l.linkRequests.Remove(al.elem)
	al.elem = nil

	// Do close
	al.err = err
	close(al.closed)

	// Link is set out of mutex, leave al to handle link.
	// An exception is ErrLinkManagerReset, it asks for immediate result.
	if al.link != nil && al.err == ErrLinkManagerReset {
		atomic.AddInt32(&l.total, -1)
		al.link = nil
	}
}
