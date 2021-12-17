package metastore

import (
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type ClusterManager interface {
	// GetActiveInstances Request available instances with minimum number required.
	GetActiveInstances(int) lambdastore.InstanceEnumerator

	// GetSlice Get slice implementation if support
	GetSlice(int) Slice

	// Trigger Trigger event with code and parameter
	Trigger(int, ...interface{})
}

type InstanceManager interface {
	lambdastore.InstanceManager
	ClusterManager
}

type Placer interface {
	// Parameters: key, size, dChunks, pChunks, chunkId, chunkSize, lambdaId, sliceSize
	NewMeta(string, string, int, int, int, int64, uint64, int) *Meta
	InsertAndPlace(string, *Meta, types.Command) (*Meta, MetaPostProcess, error)
	Place(*Meta, int, types.Command) (*lambdastore.Instance, MetaPostProcess, error)
	Get(string, int) (*Meta, bool)
}

type MetaInitializer func(meta *Meta)

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

func (l *DefaultPlacer) NewMeta(key string, size string, dChunks, pChunks, chunkId int, chunkSize int64, lambdaId uint64, sliceSize int) *Meta {
	meta := NewMeta(key, size, dChunks, pChunks, chunkSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	return meta
}

func (l *DefaultPlacer) InsertAndPlace(key string, newMeta *Meta, cmd types.Command) (*Meta, MetaPostProcess, error) {
	chunkId := newMeta.lastChunk

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}
	cmd.GetRequest().Info = meta

	instance, post, err := l.Place(meta, chunkId, cmd)
	if err != nil {
		meta.Placement[chunkId] = InvalidPlacement
		return meta, post, err
	}

	meta.Placement[chunkId] = instance.Id()
	return meta, post, nil
}

func (l *DefaultPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}

	// TODO: Do some statistic

	return meta, ok
}

func (l *DefaultPlacer) Place(meta *Meta, chunkId int, cmd types.Command) (*lambdastore.Instance, MetaPostProcess, error) {
	test := chunkId
	instances := l.cluster.GetActiveInstances(len(meta.Placement))
	for {
		// Not test is 0 based.
		if test >= instances.Len() {
			// Rotation safe: because rotation will not affect the number of active instances.
			instances = l.cluster.GetActiveInstances((test/len(meta.Placement) + 1) * len(meta.Placement)) // Force scale to ceil(test/meta.chunks)

			// If failed to get required number of instances, reset "test" and wish luck.
			if test >= instances.Len() {
				test = chunkId
			}

			// continue and test agian
			continue
		}

		ins := instances.Instance(test)
		cmd.GetRequest().InsId = ins.Id()
		if l.testChunk(ins, uint64(meta.ChunkSize)) {
			// Recheck capacity. Capacity can be calibrated and miss post-check.
			if l.testChunk(ins, 0) {
				l.log.Info("Insuffcient storage reported %d: %d of %d, trigger scaling...", ins.Id(), ins.Meta.Size(), ins.Meta.EffectiveCapacity())
				l.cluster.Trigger(EventInsufficientStorage, &types.ScaleEvent{BaseInstance: ins, Retire: true, Reason: "capacity watermark exceeded"})
			}
			// Try next group
			test += len(meta.Placement)
		} else if ins.IsBusy() {
			// Try next group
			test += len(meta.Placement)
		} else if err := ins.DispatchWithOptions(cmd, lambdastore.BUSY_CHECK); err == lambdastore.ErrInstanceBusy {
			// Try next group
			test += len(meta.Placement)
		} else if err != nil {
			return nil, nil, err
		} else {
			// Placed successfully
			key := meta.ChunkKey(chunkId)
			numChunks, size := ins.AddChunk(key, meta.ChunkSize)
			l.log.Debug("Lambda %d size updated: %d of %d (key:%s, Δ:%d, chunks:%d).",
				ins.Id(), size, ins.Meta.EffectiveCapacity(), key, meta.ChunkSize, numChunks)

			// Check if scaling is reqired.
			// NOTE: It is the responsibility of the cluster to handle duplicated events.
			if l.testChunk(ins, 0) {
				l.log.Info("Insuffcient storage reported %d: %d of %d, trigger scaling...", ins.Id(), size, ins.Meta.EffectiveCapacity())
				l.cluster.Trigger(EventInsufficientStorage, &types.ScaleEvent{BaseInstance: ins, Retire: true, Reason: "capacity watermark exceeded"})
			}

			return ins, nil, nil
		}
	}
}

func (l *DefaultPlacer) testChunk(ins *lambdastore.Instance, inc uint64) bool {
	numChunk := 0
	if inc > 0 {
		numChunk = 1
	}
	return ins.NumChunks()+numChunk > global.Options.GetInstanceChunkThreshold() || ins.Meta.ModifiedOccupancy(inc) > config.Threshold
}
