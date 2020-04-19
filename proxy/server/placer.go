package server

import (
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

const (
	INIT_CAPACITY = 10000
)

type LruPlacerMeta struct {
	once      *sync.Once
	bucketIdx int
	confirmed bool
	ts        int64
}

func newLruPlacerMeta(numChunks int) *LruPlacerMeta {
	return &LruPlacerMeta{
		confirmed: false,
		bucketIdx: -1,
	}

}

type LruPlacer struct {
	log       logger.ILogger
	metaStore *MetaStore
	group     *Group
	mu        sync.RWMutex
}

func NewLruPlacer(store *MetaStore, group *Group) *LruPlacer {
	lruPlacer := &LruPlacer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		metaStore: store,
		group:     group,
	}
	return lruPlacer
}

func (l *LruPlacer) AvgSize() int {
	sum := 0
	for i := 0; i < l.group.Len(); i++ {
		sum += int(l.group.Instance(i).Meta.Size())
	}
	return sum / l.group.Len()
}

func (l *LruPlacer) NewMeta(key string, sliceSize int, numChunks int, chunkId int, lambdaId int, chunkSize int64) *Meta {
	l.log.Debug("key and chunkId is %v,%v", key, chunkId)
	meta := NewMeta(key, numChunks, chunkSize)
	l.group.InitMeta(meta, sliceSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	//l.TouchObject(meta)
	return meta
}

func (l *LruPlacer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool) {

	//lambdaId from client
	//chunkId := newMeta.lastChunk
	//lambdaId := newMeta.Placement[chunkId]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()
	// redirect lambda destination

	if l.AvgSize() > InstanceCapacity*Threshold {
		//TODO:  lambda instances scale out
	}

	if meta.Deleted {
		meta.placerMeta = nil
		meta.Deleted = false
	}

	if meta.placerMeta == nil {
		meta.placerMeta = newLruPlacerMeta(len(meta.Placement))
	}

	// Check availability
	l.mu.Lock()
	defer l.mu.Unlock()

	// load balancer, not trigger evict
	//if instance.Meta.Size()+uint64(meta.ChunkSize) < instance.Meta.Capacity {
	//	meta.Placement[chunk] = int(instance.Id())
	//	l.cache.Add(meta, nil)
	//	instance.Meta.IncreaseSize(meta.ChunkSize)
	//	l.Adapt(meta.Placement[chunk])

	//l.log.Debug("return at balancer, Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
	//instance, size, instance.Meta.Capacity, chunk, key, meta.ChunkSize)

	//return meta, got, nil
	//}

	return meta, got
}

func (l *LruPlacer) NextAvailable(meta *Meta) {
	//if atomic.LoadInt32(&meta.placerMeta.confirmed) == 1 {
	//	l.log.Debug("other goroutine already evicted")
	//	// return existed meta value to proxy
	//	// TODO
	//	return
	//}
	//atomic.AddInt32(&meta.placerMeta.confirmed, 1)

	l.log.Debug("in NextAvailable %v", meta.placerMeta.confirmed)
	// confirm == true, other goroutine already update the placement
	if meta.placerMeta.confirmed == true {
		l.log.Debug("already updated %v", meta.Placement)
		return
	}

	for {
		// get oldest meta
		// meta size smaller than evict size, enough
		l.log.Debug("meta smaller or equal than evict")
		break
	}
	//for i := 0; i < len(evict.(*Meta).Placement); i++ {
	//	delta := meta.ChunkSize - evictSize
	//	if l.group.Instance(evict.(*Meta).Placement[i]).IncreaseSize(delta) < InstanceCapacity*Threshold {
	//		meta.placerMeta.numConfirmed += 1
	//	} else {
	//
	//	}
	//}
	//if meta.placerMeta.numConfirmed == meta.NumChunks {
	//	enough = true
	//}
	//}
	//l.log.Debug("after infinite loop")
	//l.log.Debug("evict list is %v", l.dumpList(evictList))

	// copy last evict obj placement to meta
	//meta.Placement = evictList[len(evictList)-1].Placement
	//l.log.Debug("meta placement updated %v", meta.Placement)
	//update instance size
	//l.updateInstanceSize(evictList, meta)

	meta.placerMeta.confirmed = true

}

func (l *LruPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	if meta.Deleted {
		return meta, ok
	}
	// Ensure availability
	l.mu.Lock()
	defer l.mu.Unlock()

	// Object may be evicted just before locking.
	if meta.Deleted {
		return meta, ok
	}
	return meta, ok
}

func (l *LruPlacer) updateInstanceSize(evictList []*Meta, m *Meta) {
	last := evictList[len(evictList)-1]
	l.log.Debug("last in evict list is %v", last.Key)
	for i := 0; i < len(last.Placement); i++ {
		l.group.Instance(last.Placement[i]).Meta.IncreaseSize(m.ChunkSize - last.ChunkSize)
	}
	l.log.Debug("len of evict list is", len(evictList))
	if len(evictList) > 1 {
		l.log.Debug("evict list length larger than 1")
		for j := 0; j < len(evictList)-1; j++ {
			e := evictList[j]
			//delta := m.ChunkSize - e.ChunkSize
			for i := 0; i < len(e.Placement); i++ {
				l.group.Instance(e.Placement[i]).Meta.DecreaseSize(e.ChunkSize)
			}
		}
		return
	}

	return
}
