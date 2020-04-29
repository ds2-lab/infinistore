package global

import (
	"github.com/cornelk/hashmap"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	REQCNT_STATUS_SUCCEED  = 0x00010000
	REQCNT_STATUS_RETURNED = 0x00000001
	REQCNT_MASK_SUCCEED    = 0x11110000
	REQCNT_MASK_RETURNED   = 0x00001111
)

var (
	emptySlice = make([]*types.Request, 100)     // Large enough to cover most cases.
)

type RequestCoordinator struct {
	pool         *sync.Pool
	registry     *hashmap.HashMap
}

func NewRequestCoordinator(size uintptr) *RequestCoordinator {
	return &RequestCoordinator{
		pool: &sync.Pool{
			New: newRequestCounter,
		},
		registry: hashmap.New(size),
	}
}

func (c *RequestCoordinator) Register(reqId string, cmd string, d int64, p int64) *RequestCounter {
	prepared := c.pool.Get().(*RequestCounter)
	ret, ok := c.registry.GetOrInsert(reqId, prepared)
	// There is potential problem with this late initialization approach!
	// Since the counter will be used to coordinate async responses of the first batch of concurrent
	// chunk requests, it is ok here.
	// FIXME: Fix this if neccessary.
	counter := ret.(*RequestCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.Cmd = cmd
		counter.DataShards = d
		counter.ParityShards = p
		counter.coordinator = c
		counter.reqId = reqId
		counter.status = 0
		l := int(d + p)
		if cap(counter.Requests) < l {
			counter.Requests = make([]*types.Request, l)
		} else {
			if cap(emptySlice) < l {
				emptySlice = make([]*types.Request, cap(emptySlice) * 2)
			}
			if len(counter.Requests) != l {
				counter.Requests = counter.Requests[:l]
			}
			copy(counter.Requests, emptySlice[:l])
		}
	} else {
		c.pool.Put(prepared)
	}
	return counter
}

func (c *RequestCoordinator) Load(reqId string) (*RequestCounter, bool) {
	if counter, ok := c.registry.Get(reqId); ok {
		return counter.(*RequestCounter), ok
	} else {
		return nil, ok
	}
}

func (c *RequestCoordinator) Clear(counter *RequestCounter) {
	c.registry.Del(counter.reqId)
	c.pool.Put(counter)
}

// Counter for returned requests.
type RequestCounter struct {
	Cmd          string
	DataShards   int64
	ParityShards int64
	Requests     []*types.Request

	coordinator  *RequestCoordinator
	reqId        string
	status       int64       // int32(succeed) + int32(returned)
}

func newRequestCounter() interface{} {
	return &RequestCounter{}
}

func (c *RequestCounter) AddSucceeded(chunk int) int64 {
	status := atomic.AddInt64(&c.status, REQCNT_STATUS_SUCCEED + REQCNT_STATUS_RETURNED)
	if c.Requests[chunk] != nil {
		c.Requests[chunk].MarkReturned()
	}
	return status
}

func (c *RequestCounter) AddReturned(chunk int) int64 {
	status := atomic.AddInt64(&c.status, REQCNT_STATUS_RETURNED)
	if c.Requests[chunk] != nil {
		c.Requests[chunk].MarkReturned()
	}
	return status
}

func (c *RequestCounter) Status() int64 {
	return atomic.LoadInt64(&c.status)
}

func (c *RequestCounter) Succeeded() int64 {
	return atomic.LoadInt64(&c.status) & REQCNT_MASK_SUCCEED
}

func (c *RequestCounter) Returned() int64 {
	return atomic.LoadInt64(&c.status) & REQCNT_MASK_RETURNED
}

func (c *RequestCounter) IsFulfilled(status ...int64) bool {
	if len(status) == 0 {
		return c.IsFulfilled(c.Status())
	}
	return status[0] & REQCNT_MASK_SUCCEED >= c.DataShards
}

func (c *RequestCounter) IsLate(status ...int64) bool {
	if len(status) == 0 {
		return c.IsLate(c.Status())
	}
	return status[0] & REQCNT_MASK_SUCCEED > c.DataShards
}

func (c *RequestCounter) IsAllReturned(status ...int64) bool {
	if len(status) == 0 {
		return c.IsAllReturned(c.Status())
	}
	return status[0] & REQCNT_MASK_RETURNED >= c.DataShards + c.ParityShards
}

func (c *RequestCounter) Release() {
	c.coordinator.Clear(c)
}

func (c *RequestCounter) ReleaseIfAllReturned(status ...int64) {
	if c.IsAllReturned(status...) {
		c.Release()
	}
}
