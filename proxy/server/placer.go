package server

import (
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
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
	pointer   int32
}

func NewPlacer(store *MetaStore) *Placer {
	lruPlacer := &Placer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		metaStore: store,
		scaling:   false,
		pointer:   0,
	}
	return lruPlacer
}

func (l *Placer) NewMeta(key string, size, dChunks, pChunks, chunkId, chunkSize int64, lambdaId, sliceSize int) *Meta {
	l.log.Debug("key and chunkId is %v,%v", key, chunkId)
	meta := NewMeta(key, size, dChunks, pChunks, chunkSize)
	//l.window.proxy.group.InitMeta(meta, sliceSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	return meta
}

func (l *Placer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool) {
	//lambdaId from client
	chunkId := newMeta.lastChunk
	//lambdaId := newMeta.Placement[chunkId]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)

	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	// scaler check
	if l.AvgSize() > config.InstanceCapacity*config.Threshold && l.scaling == false {
		l.log.Debug("large than instance average size")
		l.scaling = true
		l.proxy.movingWindow.scaler <- struct{}{}
	}

	if meta.placerMeta == nil {
		meta.placerMeta = newPlacerMeta()
	}

	// place
	instanceId := int(l.proxy.movingWindow.activeInstances(meta.NumChunks)[chunkId].LambdaDeployment.Id())
	meta.Placement[chunkId] = instanceId
	l.updateInstanceSize(instanceId, meta.ChunkSize)

	// get current pointer and instance ID
	l.log.Debug("chunk id is %v, instance Id is %v", chunkId, instanceId)
	// use last arrived chunk to touch meta
	l.touch(meta)

	//l.log.Debug("placement is %v", meta.Placement)
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

func (l *Placer) AvgSize() int {
	sum := 0
	pointer := int(atomic.LoadInt32(&l.pointer))

	// only check size on small set of instances
	for i := pointer; i < config.NumLambdaClusters; i++ {
		sum += int(l.proxy.movingWindow.group.Instance(i).Meta.Size())
	}

	return sum / config.NumLambdaClusters
}

func (l *Placer) updateInstanceSize(idx int, block int64) {
	l.proxy.movingWindow.group.All[idx].LambdaDeployment.(*lambdastore.Instance).IncreaseSize(block)
}
