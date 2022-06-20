package global

import (
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/util/hashmap"

	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	REQCNT_STATUS_RETURNED  uint64 = 0x0000000000000001
	REQCNT_STATUS_SUCCEED   uint64 = 0x0000000000010000
	REQCNT_STATUS_RECOVERED uint64 = 0x0000000100000000
	REQCNT_MASK_RETURNED    uint64 = 0x000000000000FFFF
	REQCNT_MASK_SUCCEED     uint64 = 0x00000000FFFF0000
	REQCNT_MASK_RECOVERED   uint64 = 0x0000FFFF00000000
	REQCNT_BITS_RETURNED    uint64 = 0
	REQCNT_BITS_SUCCEED     uint64 = 16
	REQCNT_BITS_RECOVERED   uint64 = 32
)

var (
	emptySlice = make([]*types.Request, 100) // Large enough to cover most cases.
	mu         sync.Mutex
)

func getEmptySlice(size int) []*types.Request {
	if size <= len(emptySlice) {
		return emptySlice[:size]
	}
	mu.Lock()
	defer mu.Unlock()
	if size > len(emptySlice) {
		emptySlice = make([]*types.Request, len(emptySlice)*((size-1)/len(emptySlice)+1))
	}
	return emptySlice[:size]
}

type RequestCoordinator struct {
	pool     *sync.Pool
	registry hashmap.HashMap
}

func NewRequestCoordinator(size int) *RequestCoordinator {
	return &RequestCoordinator{
		pool: &sync.Pool{
			New: newRequestCounter,
		},
		registry: hashmap.NewMapWithStringKey(size),
	}
}

func (c *RequestCoordinator) Len() int {
	return c.registry.Len()
}

func (c *RequestCoordinator) Register(reqId string, cmd string, d int, p int) *RequestCounter {
	prepared := c.pool.Get().(*RequestCounter)
	prepared.initialized.Add(1) // Block in case initalization is need.

	ret, ok := c.registry.LoadOrStore(reqId, prepared)
	// There is potential problem with this late initialization approach!
	// Since the counter will be used to coordinate async responses of the first batch of concurrent
	// chunk requests, it is ok here.
	// FIXME: Fix this if neccessary.
	counter := ret.(*RequestCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.reset(c, reqId, cmd, d, p)
		// Release initalization lock
		counter.initialized.Done()
	} else {
		// Release unused lock
		prepared.initialized.Done()
		c.pool.Put(prepared)

		// Wait for counter to be initalized.
		counter.initialized.Wait()
	}
	return counter.Load()
}

func (c *RequestCoordinator) RegisterControl(reqId string, ctrl *types.Control) {
	c.registry.Store(reqId, ctrl)
}

func (c *RequestCoordinator) Load(reqId string) interface{} {
	item, _ := c.registry.Load(reqId)
	if counter, ok := item.(*RequestCounter); ok {
		return counter.Load()
	} else {
		return item
	}
}

func (c *RequestCoordinator) Clear(reqId string) {
	c.registry.Delete(reqId)
}

func (c *RequestCoordinator) Recycle(item interface{}) bool {
	counter, ok := item.(*RequestCounter)
	if ok {
		copy(counter.Requests, getEmptySlice(len(counter.Requests))) // reset to nil and release memory
		c.pool.Put(counter)
	}

	return ok
}

// Counter for returned requests.
type RequestCounter struct {
	Cmd          string
	DataShards   int
	ParityShards int
	Requests     []*types.Request

	coordinator  *RequestCoordinator
	reqId        string
	status       uint64 // int32(succeed) + int32(returned)
	numToFulfill uint64
	initialized  sync.WaitGroup
	refs         int32
}

func newRequestCounter() interface{} {
	return &RequestCounter{}
}

func (counter *RequestCounter) reset(c *RequestCoordinator, reqId string, cmd string, d int, p int) {
	counter.Cmd = cmd
	counter.DataShards = d
	counter.ParityShards = p
	counter.coordinator = c
	counter.reqId = reqId
	counter.status = 0
	counter.numToFulfill = uint64(d)
	if Options.Evaluation && Options.NoFirstD {
		counter.numToFulfill = uint64(d + p)
	}
	l := int(d + p)
	if cap(counter.Requests) < l {
		counter.Requests = make([]*types.Request, l)
	} else if len(counter.Requests) != l {
		counter.Requests = counter.Requests[:l]
	}
}

func (c *RequestCounter) String() string {
	return c.reqId
}

func (c *RequestCounter) AddSucceeded(chunk int, recovered bool) (uint64, bool) {
	marked := false
	if c.Requests[chunk] != nil {
		marked = c.Requests[chunk].MarkReturned()
	}

	if !marked {
		return c.Status(), !marked
	} else if recovered {
		return atomic.AddUint64(&c.status, REQCNT_STATUS_RECOVERED|REQCNT_STATUS_SUCCEED|REQCNT_STATUS_RETURNED), !marked
	} else {
		status := atomic.AddUint64(&c.status, REQCNT_STATUS_SUCCEED|REQCNT_STATUS_RETURNED)
		return status, !marked
	}
}

func (c *RequestCounter) AddReturned(chunk int) (uint64, bool) {
	marked := false
	if c.Requests[chunk] != nil {
		marked = c.Requests[chunk].MarkReturned()
	}

	if !marked {
		return c.Status(), !marked
	} else {
		status := atomic.AddUint64(&c.status, REQCNT_STATUS_RETURNED)
		return status, !marked
	}
}

func (c *RequestCounter) Status() uint64 {
	return atomic.LoadUint64(&c.status)
}

func (c *RequestCounter) Returned() uint64 {
	return atomic.LoadUint64(&c.status) & REQCNT_MASK_RETURNED >> REQCNT_BITS_RETURNED
}

func (c *RequestCounter) Succeeded() uint64 {
	return (atomic.LoadUint64(&c.status) & REQCNT_MASK_SUCCEED) >> REQCNT_BITS_SUCCEED
}

func (c *RequestCounter) Recovered() uint64 {
	return (atomic.LoadUint64(&c.status) & REQCNT_MASK_RECOVERED) >> REQCNT_BITS_RECOVERED
}

func (c *RequestCounter) IsFulfilled(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsFulfilled(c.Status())
	}
	return (status[0]&REQCNT_MASK_SUCCEED)>>REQCNT_BITS_SUCCEED >= c.numToFulfill
}

func (c *RequestCounter) IsLate(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsLate(c.Status())
	}
	return (status[0]&REQCNT_MASK_SUCCEED)>>REQCNT_BITS_SUCCEED > c.numToFulfill
}

func (c *RequestCounter) IsAllReturned(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsAllReturned(c.Status())
	}
	return status[0]&REQCNT_MASK_RETURNED >= uint64(c.DataShards+c.ParityShards)
}

func (c *RequestCounter) Release() {
	c.coordinator.Clear(c.reqId)
}

func (c *RequestCounter) ReleaseIfAllReturned(status ...uint64) bool {
	if c.IsAllReturned(status...) {
		c.Release()
		return true
	} else {
		return false
	}
}

func (c *RequestCounter) Load() *RequestCounter {
	atomic.AddInt32(&c.refs, 1)
	return c
}

func (c *RequestCounter) MarkReturnd(id *types.Id) bool {
	_, ok := c.AddReturned(id.Chunk())
	return ok
}

func (c *RequestCounter) Close() {
	c.close()
}

func (c *RequestCounter) close() bool {
	cnt := atomic.AddInt32(&c.refs, -1)
	if cnt >= 0 && c.ReleaseIfAllReturned() && cnt == 0 {
		c.coordinator.Recycle(c)
		return true
	} else {
		return false
	}
}
