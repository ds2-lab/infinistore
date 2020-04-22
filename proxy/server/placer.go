package server

import (
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

const (
	INIT_CAPACITY = 10000
)

type PlacerMeta struct {
	bucketIdx  int
	instanceId int32
	ts         int64
}

func newPlacerMeta() *PlacerMeta {
	return &PlacerMeta{
		bucketIdx:  -1,
		instanceId: 0,
	}

}

type Placer struct {
	proxy     *Proxy
	log       logger.ILogger
	metaStore *MetaStore
	//group     *Group
	//window *MovingWindow
	mu sync.RWMutex
}

func NewPlacer(store *MetaStore) *Placer {
	lruPlacer := &Placer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		metaStore: store,
	}
	return lruPlacer
}

func (l *Placer) AvgSize() int {
	sum := 0
	for i := 0; i < l.proxy.group.Len(); i++ {
		sum += int(l.proxy.group.Instance(i).Meta.Size())
	}
	return sum / l.proxy.group.Len()
}

func (l *Placer) NewMeta(key string, sliceSize int, numChunks int, chunkId int, lambdaId int, chunkSize int64) *Meta {
	l.log.Debug("key and chunkId is %v,%v", key, chunkId)
	meta := NewMeta(key, numChunks, chunkSize)
	//l.window.proxy.group.InitMeta(meta, sliceSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	return meta
}

func (l *Placer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool) {
	//lambdaId from client
	chunkId := newMeta.lastChunk
	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)

	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	// scaler check
	//if l.AvgSize() > InstanceCapacity*Threshold {
	//	l.log.Debug("large than instance average size")
	//	//TODO:  lambda instances scale out
	//}

	if meta.placerMeta == nil {
		meta.placerMeta = newPlacerMeta()
	}

	// Check availability
	l.log.Debug("chunk id is %v, instance Id is %v", chunkId, meta.placerMeta.instanceId)
	meta.Placement[chunkId] = int(meta.placerMeta.instanceId)

	if int(meta.placerMeta.instanceId) == meta.NumChunks-1 {
		l.log.Debug("before touch")
		l.proxy.movingWindow.touch(meta)
	}

	meta.placerMeta.instanceId += 1

	return meta, got
}

//func (l *Placer) NextAvailable(meta *Meta) {
//	//if atomic.LoadInt32(&meta.placerMeta.confirmed) == 1 {
//	//	l.log.Debug("other goroutine already evicted")
//	//	// return existed meta value to proxy
//	//	// TODO
//	//	return
//	//}
//	//atomic.AddInt32(&meta.placerMeta.confirmed, 1)
//
//	l.log.Debug("in NextAvailable %v", meta.placerMeta.confirmed)
//	// confirm == true, other goroutine already update the placement
//	if meta.placerMeta.confirmed == true {
//		l.log.Debug("already updated %v", meta.Placement)
//		return
//	}
//
//	for {
//		// get oldest meta
//		// meta size smaller than evict size, enough
//		l.log.Debug("meta smaller or equal than evict")
//		break
//	}
//	//for i := 0; i < len(evict.(*Meta).Placement); i++ {
//	//	delta := meta.ChunkSize - evictSize
//	//	if l.group.Instance(evict.(*Meta).Placement[i]).IncreaseSize(delta) < InstanceCapacity*Threshold {
//	//		meta.placerMeta.numConfirmed += 1
//	//	} else {
//	//
//	//	}
//	//}
//	//if meta.placerMeta.numConfirmed == meta.NumChunks {
//	//	enough = true
//	//}
//	//}
//	//l.log.Debug("after infinite loop")
//	//l.log.Debug("evict list is %v", l.dumpList(evictList))
//
//	// copy last evict obj placement to meta
//	//meta.Placement = evictList[len(evictList)-1].Placement
//	//l.log.Debug("meta placement updated %v", meta.Placement)
//	//update instance size
//	//l.updateInstanceSize(evictList, meta)
//
//	meta.placerMeta.confirmed = true
//
//}

func (l *Placer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}
	return meta, ok
}

//func (l *Placer) updateInstanceSize(evictList []*Meta, m *Meta) {
//	last := evictList[len(evictList)-1]
//	l.log.Debug("last in evict list is %v", last.Key)
//	for i := 0; i < len(last.Placement); i++ {
//		l.group.Instance(last.Placement[i]).Meta.IncreaseSize(m.ChunkSize - last.ChunkSize)
//	}
//	l.log.Debug("len of evict list is", len(evictList))
//	if len(evictList) > 1 {
//		l.log.Debug("evict list length larger than 1")
//		for j := 0; j < len(evictList)-1; j++ {
//			e := evictList[j]
//			//delta := m.ChunkSize - e.ChunkSize
//			for i := 0; i < len(e.Placement); i++ {
//				l.group.Instance(e.Placement[i]).Meta.DecreaseSize(e.ChunkSize)
//			}
//		}
//		return
//	}
//
//	return
//}
