package server

import (
	"errors"
	"fmt"
	"sync"

	"github.com/cornelk/hashmap"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	BUCKET_EXPIRE = 0
	BUCKET_COLD   = 1
	BUCKET_ACTIVE = 2
)

var (
	bucketPool = sync.Pool{
		New: func() interface{} {
			return &Bucket{
				log: &logger.ColorLogger{
					Level: global.Log.GetLevel(),
					Color: true,
				},
			}
		},
	}
)

type Bucket struct {
	log         logger.ILogger
	id          int
	m           *hashmap.HashMap
	initialized int32
	ready       sync.WaitGroup

	// group management
	group     *Group
	instances []*GroupInstance
	// pointer on group
	start int
	end   int

	//state
	state int
}

func newBucket(id int, group *Group, num int, args ...interface{}) (bucket *Bucket, err error) {
	bucket = bucketPool.Get().(*Bucket)

	bucket.id = id
	bucket.log.(*logger.ColorLogger).Prefix = fmt.Sprintf("Bucket %d", id)
	bucket.log.(*logger.ColorLogger).Color = !global.Options.NoColor
	bucket.m = hashmap.New(1000) // estimate each bucket will hold 1000 objects
	bucket.group = group

	// expand
	if len(args) > 0 {
		bucket.end = config.NumLambdaClusters
	} else {
		bucket.end, err = group.Expand(num)
		if err != nil {
			bucket.Close()
			bucket = nil
			return
		}
	}

	bucket.start = bucket.end - num
	bucket.initInstance(bucket.start, num)
	bucket.state = BUCKET_ACTIVE
	return
}

func (b *Bucket) initInstance(from, length int) {
	for i := from; i < from+length; i++ {
		node := scheduler.GetForGroup(b.group, i)
		node.Meta.Capacity = global.Options.GetInstanceCapacity()
		node.Meta.IncreaseSize(config.InstanceOverhead)
		// assign bucket id to new instance
		node.BucketId = int64(b.id)
		// init instance chunk counter
		node.ChunkCounter = 0

		b.log.Debug("[adding lambda instance %v]", node.Name())

		// Begin handle requests
		go node.HandleRequests()

		// Initialize instance, Bucket is not necessary if the start time of the instance is acceptable.
		// b.ready.Add(1)
		//
		// go func() {
		// 	node.WarmUp()
		// 	b.ready.Done()
		// }()
	}
	//b.log.Debug("start name is %v, end %v, base is %v", b.group.All[b.start].Name(), b.group.All[from+len-1].Name(), b.group.base)
	b.instances = b.group.SubGroup(b.start, from+length)
	b.log.Debug("len is %v, start is %v, end is %v", len(b.instances), b.instances[0].Name(), b.instances[len(b.instances)-1].Name())
}

func (b *Bucket) waitReady() {
	b.ready.Wait()
	b.log.Info("[Bucket %v is ready]", b.id)
}

func (b *Bucket) Close() {
	b.m = nil
	b.instances = nil
	b.group = nil
	bucketPool.Put(b)
}

func (b *Bucket) scale(num int) (err error) {
	if !b.group.IsBoundary(b.end) {
		return errors.New("scale out failed, not in current bucket")
	}
	// expand
	b.end, err = b.group.Expand(num)
	if err != nil {
		return
	}
	b.initInstance(b.end-num, num)

	return nil
}

func (b *Bucket) activeInstances(activeNum int) []*GroupInstance {
	if activeNum > b.end-b.start {
		return b.instances
	}
	return b.group.All[b.end-activeNum : b.end]
}

func (b *Bucket) Len() int {
	return len(b.instances)
}

func (b *Bucket) InstanceStats(idx int) types.InstanceStats {
	return b.instances[idx].LambdaDeployment.(*lambdastore.Instance)
}

func (b *Bucket) AllInstancesStats() types.Iterator {
	all := b.instances
	return types.NewStatsIterator(all, len(all))
}

func (b *Bucket) InstanceStatsFromIterator(iter types.Iterator) (int, types.InstanceStats) {
	i, val := iter.Value()

	var ins *GroupInstance
	switch item := val.(type) {
	case []*GroupInstance:
		ins = item[i]
	case *GroupInstance:
		ins = item
	}

	if ins == nil {
		return i, nil
	} else {
		return i, ins.LambdaDeployment.(*lambdastore.Instance)
	}
}
