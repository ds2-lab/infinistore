package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangaoone/LambdaObjectstore/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"

	"github.com/cornelk/hashmap"
)

var (
	bucketPool = sync.Pool{
		New: func() interface{} {
			return &bucket{}
		},
	}
)

type bucket struct {
	log         logger.ILogger
	id          int
	m           hashmap.HashMap
	group       *Group
	initialized int32
	ready       chan struct{}

	start int64
}

func bucketStart(id int, ready chan struct{}) *bucket {
	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	bucket.start = time.Now().UnixNano()

	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		idx := i + ActiveInstance

		node := scheduler.GetForGroup(bucket.group, idx, "")
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		go func() {
			node.WarmUp()
			if atomic.AddInt32(&bucket.initialized, 1) == int32(bucket.group.Len()) {
				bucket.log.Info("[Proxy is ready]")
				close(ready)
			}
		}()

		// Begin handle requests
		go node.HandleRequests()

	}

	return bucket
}

func NewBucket(id int) *bucket {
	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	bucket.start = time.Now().UnixNano()

	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		bucket.log.Debug("i is %v", i)
		node := scheduler.GetForGroup(bucket.group, i, "")
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		go func() {
			node.WarmUp()
			if atomic.AddInt32(&bucket.initialized, 1) == int32(bucket.group.Len()) {
				bucket.log.Info("[Bucket %v is ready]", bucket.id)
			}
		}()

		// Begin handle requests
		go node.HandleRequests()

	}

	return bucket
}

func (b *bucket) Ready() chan struct{} {
	return b.ready
}

func (b *bucket) Size() int {
	return b.m.Len()
}

func (b *bucket) close() {
	bucketPool.Put(b)
}
