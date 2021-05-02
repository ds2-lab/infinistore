package metastore

import (
	"errors"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	INIT_LRU_CAPACITY = 10000
)

var (
	ErrPlacementConflict = errors.New("conflict on placing")
)

type LRUPlacerMeta struct {
	// Object management properties
	pos          [2]int // Positions on both primary and secondary array.
	visited      bool
	visitedAt    time.Time
	confirmed    []bool
	numConfirmed int
	swapMap      Placement // For decision from LRU
	evicts       *Meta
	// suggestMap Placement // For decision from balancer
	once   *sync.Once
	action MetaDoPostProcess
}

func newLRUPlacerMeta(numChunks int) *LRUPlacerMeta {
	return &LRUPlacerMeta{
		confirmed: make([]bool, numChunks),
	}
}

func (pm *LRUPlacerMeta) postProcess(action MetaDoPostProcess) {
	if pm.once == nil {
		return
	}
	pm.action = action
	pm.once.Do(pm.doPostProcess)
}

func (pm *LRUPlacerMeta) doPostProcess() {
	pm.action(pm.evicts)
	pm.evicts = nil
}

func (pm *LRUPlacerMeta) confirm(chunk int) {
	if !pm.confirmed[chunk] {
		pm.confirmed[chunk] = true
		pm.numConfirmed++
	}
}

func (pm *LRUPlacerMeta) allConfirmed() bool {
	return pm.numConfirmed == len(pm.confirmed)
}

// Placer implements a Clock LRU for object eviction. Because objects (or metas) are constantly added to the system
// in partial (chunks), a object (newer) can be appended to the list normally, while a later chunk of the object requires an
// eviction of another object (older). In this case, we evict the older in whole based on Clock LRU algorithm, and set the
// original position of the newer to nil, which a compact operation is needed later. We use a secondary array for online compact.
type LRUPlacer struct {
	log         logger.ILogger
	store       *MetaStore
	cluster     InstanceManager
	objects     [2][]*Meta // We use a secondary array for online compact, the first element is reserved.
	cursor      int        // Cursor of primary array that indicate where the LRU checks.
	cursorBound int        // The largest index the cursor can reach in current iteration.
	primary     int
	secondary   int
	mu          sync.RWMutex
}

func NewLRUPlacer(store *MetaStore, cluster InstanceManager) *LRUPlacer {
	placer := &LRUPlacer{
		log:       global.GetLogger("LRUPlacer: "),
		store:     store,
		cluster:   cluster,
		secondary: 1,
	}
	placer.objects[0] = make([]*Meta, 1, INIT_LRU_CAPACITY)
	return placer
}

func (p *LRUPlacer) NewMeta(key string, size int64, dChunks, pChunks, chunk int, chunkSize int64, lambdaId uint64, sliceSize int) *Meta {
	meta := NewMeta(key, size, dChunks, pChunks, chunkSize)
	if meta.slice == nil {
		meta.slice = p.cluster.GetSlice(sliceSize)
	} else {
		meta.slice.Reset(sliceSize)
	}
	meta.Placement[chunk] = lambdaId
	meta.lastChunk = chunk
	return meta
}

func (p *LRUPlacer) InsertAndPlace(key string, newMeta *Meta, cmd types.Command) (*Meta, MetaPostProcess, error) {
	chunkId := newMeta.lastChunk

	meta, got, _ := p.store.GetOrInsert(key, newMeta)
	if got {
		// Only copy placement assignment if the chunk has not been confirm.
		if meta.placerMeta == nil || !meta.placerMeta.(*LRUPlacerMeta).confirmed[chunkId] {
			meta.Placement[chunkId] = newMeta.Placement[chunkId]
		}
		newMeta.close()
	}
	cmd.GetRequest().Info = meta

	_, post, err := p.Place(meta, chunkId, cmd)
	return meta, post, err
}

// For test and simulation
func (p *LRUPlacer) Insert(key string, newMeta *Meta) (*Meta, MetaPostProcess, error) {
	chunkId := newMeta.lastChunk

	meta, got, _ := p.store.GetOrInsert(key, newMeta)
	if got {
		meta.Placement[chunkId] = newMeta.Placement[chunkId]
		newMeta.close()
	}

	_, post, err := p.FindPlacement(meta, chunkId)
	return meta, post, err
}

func (p *LRUPlacer) Place(meta *Meta, chunkId int, cmd types.Command) (*lambdastore.Instance, MetaPostProcess, error) {
	p.log.Debug("Placing %s", meta.ChunkKey(chunkId))
	instance, post, err := p.FindPlacement(meta, chunkId)
	if err != nil {
		return instance, post, err
	}

	cmd.GetRequest().InsId = instance.Id()
	err = instance.Dispatch(cmd)
	return instance, post, err
}

// NewMeta will remap idx according to following logic:
// 0. If an LRU relocation is present, remap according to "chunk" in relocation array.
// 1. Base on the size of slice, remap to a instance in the group.
// 2. If target instance is full, request an LRU relocation and restart from 0.
// 3. If no Balancer relocation is available, request one.
// 4. Remap to smaller "Size" of instance between target instance and remapped instance according to "chunk" in
//    relocation array.
func (p *LRUPlacer) FindPlacement(meta *Meta, chunkId int) (*lambdastore.Instance, MetaPostProcess, error) {
	p.log.Debug("Finding placement %s", meta.ChunkKey(chunkId))

	meta.mu.Lock()
	defer meta.mu.Unlock()

	// Added by Tianium: 20210427
	// Check if it is "evicted". Initialize on demand recovery by replacing.
	// Eviction flag(Deleted) will not be removed until all chunks have been confirmed.
	if meta.Deleted && meta.placerMeta != nil && meta.placerMeta.(*LRUPlacerMeta).allConfirmed() {
		meta.placerMeta = nil
		if meta.slice != nil {
			meta.slice.Reset(meta.slice.Size())
		}
	}

	// Initialize placerMeta if not.
	if meta.placerMeta == nil {
		meta.placerMeta = newLRUPlacerMeta(len(meta.Placement))
	}
	placerMeta := meta.placerMeta.(*LRUPlacerMeta)

	// Usually this should always be false for SET operation, flag RESET if true.
	if placerMeta.confirmed[chunkId] {
		meta.Reset = true
		// No size update is required, reserved on setting.
		return p.cluster.Instance(meta.Placement[chunkId]), nil, nil
	}

	// Check if a replacement decision has been made.
	if !IsPlacementEmpty(placerMeta.swapMap) {
		meta.Placement[chunkId] = placerMeta.swapMap[chunkId]
		placerMeta.confirm(chunkId)
		// Added by Tianium: 20210427
		// Remove eviction flag on all confirmed
		if placerMeta.allConfirmed() {
			meta.Deleted = false
		}

		// No size update is required, reserved on eviction.
		return p.cluster.Instance(meta.Placement[chunkId]), nil, nil
	}

	// assigned := meta.slice.GetIndex(lambdaId)
	assigned := meta.Placement[chunkId]
	if meta.slice != nil {
		assigned = meta.slice.GetIndex(meta.Placement[chunkId])
	}
	instance := p.cluster.Instance(assigned)
	confirmed := false
	if instance.Meta.Size()+uint64(meta.ChunkSize) < instance.Meta.Capacity {
		size := instance.Meta.IncreaseSize(meta.ChunkSize)
		if size < instance.Meta.Capacity {
			meta.Placement[chunkId] = assigned
			placerMeta.confirm(chunkId)
			confirmed = true

			// We can add size to instance safely, the allocated space is reserved for this chunk even set operation may fail.
			// This allow the client to reset the chunk without affecting the placement.
			p.log.Debug("Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
				assigned, size, instance.Meta.Capacity, chunkId, meta.Key, meta.ChunkSize)

			// Added by Tianium: 20210427
			// Remove eviction flag on all confirmed
			if placerMeta.allConfirmed() {
				meta.Deleted = false
			}

			// LOCK FREE: Regardless the value of p.primary, if meta has been in the placer already, either position should be non-zero.
			if placerMeta.pos[p.primary] > 0 || placerMeta.pos[p.secondary] > 0 {
				return instance, nil, nil
			} // else: No return until meta has been added to the placer.
		} else {
			// Roll back and continue
			instance.Meta.DecreaseSize(meta.ChunkSize)
		}
	}

	// Lock the placer
	p.mu.Lock()

	// Commented by Tianium: 20210427, because
	// 1. Unconfirmed object will not be evicted
	// 2. On re-placing, the object is flaged as deleted until all chunks are confirmed.
	// Start of commented
	// Double check if it is evicted.
	// if meta.Deleted {
	// 	return nil, nil, ErrPlacementConflict
	// }
	// End of commented

	// If confirmed and we get here, meta is not in the placer.
	if confirmed {
		p.AddObject(meta)
		p.mu.Unlock()
		return instance, nil, nil
	}

	// Try find a replacement
	// p.log.Warn("lambda %d overweight triggered by %d@%s, meta: %v", assigned, chunk, meta.Key, meta)
	// p.log.Info(p.dumpLRUPlacer())
	numScaned := 0
	for candidate, found := p.NextAvailableObject(meta, nil); !found; candidate, found = p.NextAvailableObject(meta, candidate) {
		// p.log.Warn("lambda %d overweight triggered by %d@%s, meta: %v", assigned, chunk, meta.Key, meta)
		// p.log.Info(p.dumpLRUPlacer())
		if numScaned > 0 && candidate != nil {
			p.evictMeta(meta, candidate, false)
			break
		}
		numScaned++
	}
	// p.log.Debug("meta key is: %s, chunk is %d, evicted, evicted key: %s, placement: %v", meta.Key, chunk, meta.placerMeta.evicts.Key, meta.placerMeta.evicts.Placement)

	p.mu.Unlock()

	for i, tbe := range placerMeta.swapMap { // To be evicted
		instance := p.cluster.Instance(tbe)
		if !placerMeta.confirmed[i] {
			// Confirmed chunk will not move

			// The size can be replaced safely, too.
			size := instance.Meta.IncreaseSize(meta.ChunkSize - placerMeta.evicts.ChunkSize)
			p.log.Debug("Lambda %d size updated: %d of %d (key:%d@%s, evict:%d@%s, Δ:%d).",
				tbe, size, instance.Meta.Capacity, i, meta.Key, i, placerMeta.evicts.Key,
				meta.ChunkSize-placerMeta.evicts.ChunkSize)
		} else {
			size := instance.Meta.DecreaseSize(placerMeta.evicts.ChunkSize)
			p.log.Debug("Lambda %d size updated: %d of %d (evict:%d@%s, Δ:%d).",
				tbe, size, instance.Meta.Capacity, i, placerMeta.evicts.Key,
				-placerMeta.evicts.ChunkSize)
		}
	}

	meta.Placement[chunkId] = placerMeta.swapMap[chunkId]
	placerMeta.confirm(chunkId)
	// Added by Tianium: 20210427
	// Remove eviction flag on all confirmed
	if placerMeta.allConfirmed() {
		meta.Deleted = false
	}
	placerMeta.once = &sync.Once{}
	return p.cluster.Instance(meta.Placement[chunkId]), placerMeta.postProcess, nil
}

func (p *LRUPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := p.store.Get(key)
	if !ok {
		return nil, ok
	}

	// If deleted, skip checks below.
	if meta.Deleted {
		return meta, ok
	}

	// Assertion: If normal, all chunks must be confirmed.
	if meta.placerMeta == nil || !meta.placerMeta.(*LRUPlacerMeta).confirmed[chunk] {
		p.log.Warn("Detected unconfirmed chunk: %s, evicted: %v", meta.ChunkKey(chunk), meta.Deleted)
		return nil, false
	}

	p.TouchObject(meta)
	return meta, ok
}

// Object management implementation: Clock LRU
func (p *LRUPlacer) AddObject(meta *Meta) {
	placerMeta := meta.placerMeta.(*LRUPlacerMeta)
	placerMeta.pos[p.primary] = len(p.objects[p.primary])
	placerMeta.visited = true
	placerMeta.visitedAt = time.Now()

	p.objects[p.primary] = append(p.objects[p.primary], meta)
}

func (p *LRUPlacer) TouchObject(meta *Meta) {
	meta.placerMeta.(*LRUPlacerMeta).visited = true
	meta.placerMeta.(*LRUPlacerMeta).visitedAt = time.Now()
}

func (p *LRUPlacer) NextAvailableObject(meta *Meta, candidate *Meta) (*Meta, bool) {
	// Position 0 is reserved, cursor iterates from 1
	if p.cursor == 0 {
		if p.objects[p.secondary] == nil || cap(p.objects[p.secondary]) < len(p.objects[p.primary]) {
			p.objects[p.secondary] = make([]*Meta, 1, 2*len(p.objects[p.primary]))
		} else {
			p.objects[p.secondary] = p.objects[p.secondary][:1] // Alwarys append from the 2nd position.
		}
		p.cursorBound = len(p.objects[p.primary]) // CursorBound is fixed once start iteration.
		p.cursor = 1
	}

	found := false
	for _, m := range p.objects[p.primary][p.cursor:p.cursorBound] {
		if m == nil {
			// skip empty slot.
			p.cursor++
			continue
		}

		mPlacerMeta := m.placerMeta.(*LRUPlacerMeta)
		if m == meta {
			// Ignore meta itself
		} else if mPlacerMeta.visited && mPlacerMeta.allConfirmed() {
			// Only switch to unvisited for complete object.
			mPlacerMeta.visited = false
		} else if !mPlacerMeta.visited && meta.ChunkSize <= m.ChunkSize {
			p.evictMeta(meta, m, false)
			m = meta
			mPlacerMeta = m.placerMeta.(*LRUPlacerMeta)
			found = true

		} else if !mPlacerMeta.visited && (candidate == nil || m.ChunkSize > candidate.ChunkSize) {
			// Candidate is not large enough, but largest we have seen so far.
			candidate = m
		}

		// Add current object to the secondary array for compact purpose.
		mPlacerMeta.pos[p.secondary] = len(p.objects[p.secondary])
		p.objects[p.secondary] = append(p.objects[p.secondary], m)
		p.cursor++

		if found {
			break
		}
	}

	// Reach end of the primary array(cursorBound), reset cursor and switch arraies.
	if !found {
		// Add new objects to the secondary array for compact purpose.
		if len(p.objects[p.primary]) > p.cursor {
			for _, m := range p.objects[p.primary][p.cursor:] {
				if m == nil {
					continue
				}

				m.placerMeta.(*LRUPlacerMeta).pos[p.secondary] = len(p.objects[p.secondary])
				p.objects[p.secondary] = append(p.objects[p.secondary], m)
			}
		}
		// Reset cursor and switch
		p.cursor = 0
		p.primary, p.secondary = p.secondary, p.primary
	}

	return candidate, found
}

func (p *LRUPlacer) evictMeta(meta *Meta, candidate *Meta, resetSecondary bool) {
	// Found candidate and candidate is large enough to be freed for space.
	candidate.Deleted = true

	// Don't reset placerMeta here, reset on recover object.
	// m.placerMeta = nil
	metaPlacerMeta := meta.placerMeta.(*LRUPlacerMeta)
	metaPlacerMeta.swapMap = copyPlacement(metaPlacerMeta.swapMap, candidate.Placement)
	metaPlacerMeta.evicts = candidate
	metaPlacerMeta.visited = true
	metaPlacerMeta.visitedAt = time.Now()

	candidatePlacerMeta := candidate.placerMeta.(*LRUPlacerMeta)
	p.objects[p.primary][metaPlacerMeta.pos[p.primary]] = nil // unset old position
	cursorPrimary := candidatePlacerMeta.pos[p.primary]
	metaPlacerMeta.pos[p.primary] = cursorPrimary
	p.objects[p.primary][cursorPrimary] = meta // replace
	if resetSecondary {
		p.objects[p.secondary][metaPlacerMeta.pos[p.secondary]] = nil // unset old position
		cursorSecondary := candidatePlacerMeta.pos[p.secondary]
		metaPlacerMeta.pos[p.secondary] = cursorSecondary
		p.objects[p.secondary][cursorSecondary] = meta // replace
	}
}

// func (p *LRUPlacer) dumpLRUPlacer(args ...bool) string {
// 	if len(args) > 0 && args[0] {
// 		return p.dump(p.objects[p.secondary])
// 	} else {
// 		return p.dump(p.objects[p.primary])
// 	}
// }

// func (p *LRUPlacer) dump(metas []*Meta) string {
// 	if metas == nil || len(metas) < 1 {
// 		return ""
// 	}

// 	elem := make([]string, len(metas)-1)
// 	for i, meta := range metas[1:] {
// 		if meta == nil {
// 			elem[i] = "nil"
// 			continue
// 		}

// 		visited := 0
// 		if meta.placerMeta.(*LRUPlacerMeta).visited {
// 			visited = 1
// 		}
// 		elem[i] = fmt.Sprintf("%s-%d", meta.Key, visited)
// 	}
// 	return strings.Join(elem, ",")
// }
