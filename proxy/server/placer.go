package server

import (
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
	"github.com/wangaoone/LambdaObjectstore/proxy/lambdastore"
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
	from      int32
}

func NewPlacer(store *MetaStore) *Placer {
	lruPlacer := &Placer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		metaStore: store,
		//scaling:   false,
		from: 0,
	}
	return lruPlacer
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
	if l.AvgSize() > InstanceCapacity*Threshold && l.scaling == false {
		l.log.Debug("large than instance average size")
		l.scaling = true
		l.proxy.scaler.Signal <- struct{}{}
	}

	if meta.placerMeta == nil {
		meta.placerMeta = newPlacerMeta()
	}

	// Check placement
	//offset := l.proxy.movingWindow.getCurrentBucket().offset
	currentFrom := atomic.LoadInt32(&l.from)
	instanceId := l.proxy.movingWindow.getInstanceId(lambdaId, int(currentFrom))
	l.log.Debug("chunk id is %v, instance Id is %v", chunkId, instanceId)

	// place
	meta.Placement[chunkId] = instanceId
	l.updateInstanceSize(instanceId, meta.ChunkSize)

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
	//currentBucket := l.proxy.movingWindow.getCurrentBucket()
	currentFrom := int(atomic.LoadInt32(&l.from))

	// only check size on small set of instances
	//for i := currentBucket.from; i < len(currentBucket.group.All); i++ {
	for i := currentFrom; i < NumLambdaClusters; i++ {
		//sum += int(currentBucket.group.Instance(i).Meta.Size())
		sum += int(l.proxy.group.Instance(i).Meta.Size())
	}

	return sum / NumLambdaClusters
}

func (l *Placer) updateInstanceSize(idx int, block int64) {
	//l.temp(idx)
	l.proxy.group.All[idx].LambdaDeployment.(*lambdastore.Instance).IncreaseSize(block)
}

func (l *Placer) temp(idx int) {
	for i := range l.proxy.group.All {
		l.log.Debug("name %v, size %v", l.proxy.group.All[i].Name(), l.proxy.group.All[i].LambdaDeployment.(*lambdastore.Instance).Size())
	}
}
