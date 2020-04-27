package server

import (
	"fmt"
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"

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
	//ready       chan struct{}
	ready sync.WaitGroup
}

func newBucket(id int, args ...interface{}) *bucket {

	// if this is the initial bucket
	//var ready chan struct{}

	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		node := scheduler.GetForGroup(bucket.group, i)
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Begin handle requests
		go node.HandleRequests()

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		bucket.ready.Add(1)

		go func() {
			node.WarmUp()
			bucket.ready.Done()
		}()
	}
	return bucket
}

func (b *bucket) waitReady() {
	b.ready.Wait()
	b.log.Info("[Bucket %v is ready]", b.id)
}

func (b *bucket) Size() int {
	return b.m.Len()
}

func (b *bucket) close() {
	bucketPool.Put(b)
}

func (b *bucket) append(group *Group) {
	for i := 0; i < len(group.All); i++ {
		b.group.All = append(b.group.All, group.All[i])
	}
	b.log.Debug(" scale out, current bucket group len is %v", len(b.group.All))
}
