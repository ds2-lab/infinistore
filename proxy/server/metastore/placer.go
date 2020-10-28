package metastore

import (
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

type InstanceManager interface {
	// Request available instances with minimum number required.
	GetActiveInstances(int) []*lambdastore.Instance
	// Trigger event with code and parameter
	Trigger(int, ...interface{})
}

type Placer interface {
	// Parameters: key, size, dChunks, pChunks, chunkId, chunkSize, lambdaId, sliceSize
	NewMeta (string, int64, int64, int64, int64, int64, int, int) *Meta
	GetOrInsert(string, *Meta) (*Meta, bool, MetaPostProcess)
	Get(string, int) (*Meta, bool)
}

type DefaultPlacer struct {
	metaStore *MetaStore
	cluster   InstanceManager
	log       logger.ILogger
}

func NewDefaultPlacer(store *MetaStore, cluster InstanceManager) *DefaultPlacer {
	placer := &DefaultPlacer{
		metaStore: store,
		cluster:   cluster,
		log:       global.GetLogger("DefaultPlacer: "),
	}
	return placer
}

func (l *DefaultPlacer) NewMeta(key string, size, dChunks, pChunks, chunkId, chunkSize int64, lambdaId, sliceSize int) *Meta {
	meta := NewMeta(key, size, dChunks, pChunks, chunkSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	return meta
}

func (l *DefaultPlacer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool, MetaPostProcess) {
	//lambdaId from client
	chunkId := newMeta.lastChunk
	//lambdaId := newMeta.Placement[chunkId]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}

	instances := l.cluster.GetActiveInstances(len(meta.Placement))
	instance := instances[int(chunkId) % len(instances)]
	meta.Placement[chunkId] = int(instance.Id())

	instance.ChunkCounter += 1                     // TODO: Use atomic operation
	size := instance.IncreaseSize(meta.ChunkSize)
	instance.KeyMap = append(instance.KeyMap, key) // TODO: Use atomic operation
	l.log.Debug("Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
									instance.Id(), size, instance.Meta.Capacity, chunkId, key, meta.ChunkSize)

	// Check if scaling is reqired.
	// TODO: It is the responsibility of the cluster to handle duplicated events.
	if instance.ChunkCounter >= global.Options.GetInstanceChunkThreshold() ||
		size >= global.Options.GetInstanceThreshold() {
		l.log.Debug("Insuffcient storage reported %d", instance.Id())
		l.cluster.Trigger(EventInsufficientStorage, instance)
	}

	return meta, got, nil
}

func (l *DefaultPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}

	// TODO: Do some statistic

	return meta, ok
}

// TODO counter can be put in meta itself.
// func (l *DefaultPlacer) Touch(meta *Meta) {
// 	if int(atomic.AddInt32(&meta.placerMeta.counter, 1)) == meta.NumChunks {
// 		l.log.Debug("before touch")
// 		l.cluster.Touch(meta)
// 		meta.placerMeta.counter = 0
// 	}
// }
