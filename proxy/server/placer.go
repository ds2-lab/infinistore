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
	instances []*GroupInstance

	// scaling?
	isScale bool
}

func newPlacerMeta() *PlacerMeta {
	return &PlacerMeta{
		counter: 0,
		isScale: false,
	}

}

type Placer struct {
	proxy     *Proxy
	log       logger.ILogger
	metaStore *MetaStore
	scaling   bool
	mu        sync.RWMutex
	pointer   int32

	instances []*GroupInstance
}

func NewPlacer(store *MetaStore) *Placer {
	lruPlacer := &Placer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  !global.Options.NoColor,
		},
		metaStore: store,
		scaling:   false,
		pointer:   0,
	}
	return lruPlacer
}

func (l *Placer) NewMeta(key string, size, dChunks, pChunks, chunkId, chunkSize int64, lambdaId, sliceSize int) *Meta {
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

	if meta.placerMeta == nil {
		meta.placerMeta = newPlacerMeta()

	}

	meta.placerMeta.instances = l.instances
	meta.placerMeta.bucketIdx = l.proxy.movingWindow.getCurrentBucket().id
	targetInstance := meta.placerMeta.instances[chunkId].LambdaDeployment

	// scaler check
	if (targetInstance.(*lambdastore.Instance).ChunkCounter >= config.MaxChunk ||
		l.AvgSize(l.instances) > config.InstanceCapacity*config.Threshold) && l.scaling == false && meta.placerMeta.isScale == false {
		l.log.Debug("large than instance average size")
		l.scaling = true
		meta.placerMeta.isScale = true
		l.proxy.movingWindow.scaler <- struct{}{}
	}

	// place
	instanceId := int(targetInstance.Id())
	meta.Placement[chunkId] = instanceId

	// update instance chunk counter
	targetInstance.(*lambdastore.Instance).ChunkCounter += 1

	l.updateInstanceSize(instanceId, meta.ChunkSize)

	// get current pointer and instance ID
	l.log.Debug("chunk id is %v, instance Id is %v", chunkId, instanceId)
	// use last arrived chunk to touch meta
	//l.touch(meta)

	return meta, got
}

func (l *Placer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	l.log.Debug("chunk is %v, ok is %v, meta is %v, placement is %v", chunk, ok, meta.Key, meta.Placement)

	return meta, ok
}

func (l *Placer) touch(meta *Meta) {
	if int(atomic.AddInt32(&meta.placerMeta.counter, 1)) == meta.NumChunks {
		l.log.Debug("before touch")
		l.proxy.movingWindow.touch(meta)
		meta.placerMeta.counter = 0
	}
}

func (l *Placer) AvgSize(instances []*GroupInstance) int {
	sum := 0

	// only check size on small set of instances
	for _, ins := range instances {
		sum += int(ins.LambdaDeployment.(*lambdastore.Instance).Meta.Size())
	}

	return sum / len(instances)
}

func (l *Placer) updateInstanceSize(idx int, block int64) {
	l.proxy.movingWindow.group.All[idx].LambdaDeployment.(*lambdastore.Instance).IncreaseSize(block)
}
