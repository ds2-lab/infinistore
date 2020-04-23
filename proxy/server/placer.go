package server

import (
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

const (
	INIT_CAPACITY = 10000
)

type PlacerMeta struct {
	bucketIdx int
	counter   int32
}

func newPlacerMeta() *PlacerMeta {
	return &PlacerMeta{
		bucketIdx: -1,
		counter:   0,
	}

}

type Placer struct {
	proxy     *Proxy
	log       logger.ILogger
	metaStore *MetaStore
	scaling   bool
	mu        sync.RWMutex
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
	l.log.Debug("group len %v", l.proxy.group.Len())
	sum := 0
	for i := 0; i < l.proxy.group.Len(); i++ {
		sum += int(l.proxy.group.Instance(i).Meta.Size())
	}
	l.log.Debug("avg size is", sum/l.proxy.group.Len())
	return sum / l.proxy.group.Len()
	//for i := 0; i < l.proxy.group.Len(); i++ {
	//	l.log.Debug("instance and sizeinstance and size is %v %v", l.proxy.group.Instance(i).Name(), int(l.proxy.group.Instance(i).Meta.Size()))
	//}
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
	lambdaId := newMeta.Placement[chunkId]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)

	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	// scaler check
	//if l.AvgSize() > InstanceCapacity*Threshold && l.scaling == false {
	//	l.log.Debug("large than instance average size")
	//	l.scaling = true
	//	l.proxy.scaler.Signal <- struct{}{}
	//}

	//l.AvgSize()

	if meta.placerMeta == nil {
		meta.placerMeta = newPlacerMeta()
	}

	// Check placement
	offset := l.proxy.movingWindow.getCurrentBucket().pointer

	instanceId := l.proxy.movingWindow.getInstanceId(lambdaId) + offset
	l.log.Debug("chunk id is %v, instance Id is %v, offset is %v", chunkId, instanceId, offset)

	// place
	meta.Placement[chunkId] = instanceId
	l.updateInstanceSize(instanceId, meta.ChunkSize)

	// use last arrived chunk to touch meta
	l.touch(meta)

	l.log.Debug("placement is %v", meta.Placement)
	return meta, got
}

func (l *Placer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}
	// use last arrived chunk to touch meta
	l.touch(meta)
	return meta, ok
}

func (l *Placer) touch(meta *Meta) {
	if int(atomic.AddInt32(&meta.placerMeta.counter, 1)) == meta.NumChunks {
		l.log.Debug("before touch")
		l.proxy.movingWindow.touch(meta)
		meta.placerMeta.counter = 0
	}
}

func (l *Placer) updateInstanceSize(idx int, block int64) {
	l.proxy.group.Instance(idx).Meta.IncreaseSize(block)
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
