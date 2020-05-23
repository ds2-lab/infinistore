package global

import (
	"github.com/cornelk/hashmap"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/proxy/types"
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
	prepared.initialized.Add(1)	// Block in case initalization is need.

	ret, ok := c.registry.GetOrInsert(reqId, prepared)
	counter := ret.(*RequestCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.Cmd = cmd
		counter.DataShards = d
		counter.ParityShards = p
		counter.returned = 0
		counter.numToFulfill = d
		if Options.Evaluation && Options.NoFirstD {
			counter.numToFulfill = d + p
		}
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
		// Release initalization lock
		counter.initialized.Done()
	} else {
		// Release unused lock
		prepared.initialized.Done()
		c.pool.Put(prepared)

		// Wait for counter to be initalized.
		counter.initialized.Wait()
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

func (c *RequestCoordinator) Clear(reqId string, counter *RequestCounter) {
	c.registry.Del(reqId)
	c.pool.Put(counter)
}

// Counter for returned requests.
type RequestCounter struct {
	Cmd          string
	DataShards   int64
	ParityShards int64
	Requests     []*types.Request

	returned     int64       // Returned counter from lambda.
	numToFulfill int64
	initialized  sync.WaitGroup
}

func newRequestCounter() interface{} {
	return &RequestCounter{}
}

func (c *RequestCounter) AddReturned(chunk int) int64 {
	returned := atomic.AddInt64(&c.returned, 1)
	if c.Requests[chunk] != nil {
		c.Requests[chunk].MarkReturned()
	}
	return returned
}

func (c *RequestCounter) Returned() int64 {
	return atomic.LoadInt64(&c.returned)
}

func (c *RequestCounter) IsFulfilled(returned int64) bool {
	return returned >= c.numToFulfill
}

func (c *RequestCounter) IsLate(returned int64) bool {
	return returned > c.numToFulfill
}

func (c *RequestCounter) IsAllReturned(returned int64) bool {
	return returned >= c.DataShards + c.ParityShards
}
