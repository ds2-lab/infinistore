package metastore

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
)

const (
	INIT_LRU_CAPACITY = 10000
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

func (pm *LRUPlacerMeta) confirm(chunk int64) {
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

func (p *LRUPlacer) NewMeta(key string, size, dChunks, pChunks, chunk, chunkSize int64, lambdaId, sliceSize int) *Meta {
	meta := NewMeta(key, size, dChunks, pChunks, chunkSize)
	// p.group.InitMeta(meta, sliceSize)   // Slice size is removed, we may added back later.
	meta.Placement[chunk] = lambdaId
	meta.lastChunk = chunk
	return meta
}

// NewMeta will remap idx according to following logic:
// 0. If an LRU relocation is present, remap according to "chunk" in relocation array.
// 1. Base on the size of slice, remap to a instance in the group.
// 2. If target instance is full, request an LRU relocation and restart from 0.
// 3. If no Balancer relocation is available, request one.
// 4. Remap to smaller "Size" of instance between target instance and remapped instance according to "chunk" in
//    relocation array.
func (p *LRUPlacer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool, MetaPostProcess) {
	chunk := newMeta.lastChunk
	lambdaId := newMeta.Placement[chunk]

	meta, got, _ := p.store.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()

	// Check if it is "evicted".
	if meta.Deleted {
		meta.placerMeta = nil
		meta.Deleted = false
	}

	// Initialize placerMeta if not.
	if meta.placerMeta == nil {
		meta.placerMeta = newLRUPlacerMeta(len(meta.Placement))
	}
	placerMeta := meta.placerMeta.(*LRUPlacerMeta)

	// Usually this should always be false for SET operation, flag RESET if true.
	if placerMeta.confirmed[chunk] {
		meta.Reset = true
		// No size update is required, reserved on setting.
		return meta, got, nil
	}

	// Check availability
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check if it is evicted.
	if meta.Deleted {
		return meta, got, nil
	}

	// Check if a replacement decision has been made.
	if !IsPlacementEmpty(placerMeta.swapMap) {
		meta.Placement[chunk] = placerMeta.swapMap[chunk]
		placerMeta.confirm(chunk)

		// No size update is required, reserved on eviction.
		return meta, got, nil
	}

	// assigned := meta.slice.GetIndex(lambdaId)
	assigned := lambdaId
	instances := p.cluster.GetActiveInstances(len(meta.Placement))
	instance := instances[assigned%len(instances)]
	if instance.Meta.Size()+uint64(meta.ChunkSize) < instance.Meta.Capacity {
		meta.Placement[chunk] = assigned
		placerMeta.confirm(chunk)
		// If the object has not seen.
		if placerMeta.pos[p.primary] == 0 {
			p.AddObject(meta)
		}
		// We can add size to instance safely, the allocated space is reserved for this chunk even set operation may fail.
		// This allow the client to reset the chunk without affecting the placement.
		size := instance.Meta.IncreaseSize(meta.ChunkSize)
		p.log.Debug("Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
			assigned, size, instance.Meta.Capacity, chunk, key, meta.ChunkSize)
		return meta, got, nil
	}

	// Try find a replacement
	// p.log.Warn("lambda %d overweight triggered by %d@%s, meta: %v", assigned, chunk, meta.Key, meta)
	// p.log.Info(p.dumpLRUPlacer())
	for !p.NextAvailableObject(meta) {
		// p.log.Warn("lambda %d overweight triggered by %d@%s, meta: %v", assigned, chunk, meta.Key, meta)
		// p.log.Info(p.dumpLRUPlacer())
	}
	// p.log.Debug("meta key is: %s, chunk is %d, evicted, evicted key: %s, placement: %v", meta.Key, chunk, meta.placerMeta.evicts.Key, meta.placerMeta.evicts.Placement)

	for i, tbe := range placerMeta.swapMap { // To be evicted
		instance := instances[tbe]
		if !placerMeta.confirmed[i] {
			// Confirmed chunk will not move

			// The size can be replaced safely, too.
			size := instance.Meta.IncreaseSize(meta.ChunkSize - placerMeta.evicts.ChunkSize)
			p.log.Debug("Lambda %d size updated: %d of %d (key:%d@%s, evict:%d@%s, Δ:%d).",
				tbe, size, instance.Meta.Capacity, i, key, i, placerMeta.evicts.Key,
				meta.ChunkSize-placerMeta.evicts.ChunkSize)
		} else {
			size := instance.Meta.DecreaseSize(placerMeta.evicts.ChunkSize)
			p.log.Debug("Lambda %d size updated: %d of %d (evict:%d@%s, Δ:%d).",
				tbe, size, instance.Meta.Capacity, i, placerMeta.evicts.Key,
				-placerMeta.evicts.ChunkSize)
		}
	}

	meta.Placement[chunk] = placerMeta.swapMap[chunk]
	placerMeta.confirm(chunk)
	placerMeta.once = &sync.Once{}
	return meta, got, placerMeta.postProcess
}

func (p *LRUPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := p.store.Get(key)
	if !ok {
		return nil, ok
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	if meta.Deleted {
		return meta, ok
	}

	if meta.placerMeta == nil || !meta.placerMeta.(*LRUPlacerMeta).confirmed[chunk] {
		return nil, false
	}

	// Ensure availability
	p.mu.Lock()
	defer p.mu.Unlock()

	// Object may be evicted just before locking.
	if meta.Deleted {
		return meta, ok
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
	placerMeta := meta.placerMeta.(*LRUPlacerMeta)
	placerMeta.visited = true
	placerMeta.visitedAt = time.Now()
}

func (p *LRUPlacer) NextAvailableObject(meta *Meta) bool {
	// Position 0 is reserved, cursor iterates from 1
	if p.cursor == 0 {
		if p.objects[p.secondary] == nil || cap(p.objects[p.secondary]) < len(p.objects[p.primary]) {
			p.objects[p.secondary] = make([]*Meta, 1, 2*len(p.objects[p.primary]))
		} else {
			p.objects[p.secondary] = p.objects[p.secondary][:1] // Alwarys append from the 2nd position.
		}
		p.cursorBound = len(p.objects[p.primary]) // CusorBound is fixed once start iteration.
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
		} else if !mPlacerMeta.visited {
			// Found candidate
			m.Deleted = true
			// Don't reset placerMeta here, reset on recover object.
			// m.placerMeta = nil
			metaPlacerMeta := meta.placerMeta.(*LRUPlacerMeta)
			metaPlacerMeta.swapMap = copyPlacement(metaPlacerMeta.swapMap, m.Placement)
			metaPlacerMeta.evicts = m

			p.objects[p.primary][metaPlacerMeta.pos[p.primary]] = nil // unset old position
			metaPlacerMeta.pos[p.primary] = p.cursor
			p.objects[p.primary][p.cursor] = meta // replace
			m = meta
			mPlacerMeta = m.placerMeta.(*LRUPlacerMeta)

			mPlacerMeta.visited = true
			mPlacerMeta.visitedAt = time.Now()
			found = true
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

	return found
}

func (p *LRUPlacer) dumpLRUPlacer(args ...bool) string {
	if len(args) > 0 && args[0] {
		return p.dump(p.objects[p.secondary])
	} else {
		return p.dump(p.objects[p.primary])
	}
}

func (p *LRUPlacer) dump(metas []*Meta) string {
	if metas == nil || len(metas) < 1 {
		return ""
	}

	elem := make([]string, len(metas)-1)
	for i, meta := range metas[1:] {
		if meta == nil {
			elem[i] = "nil"
			continue
		}

		visited := 0
		if meta.placerMeta.(*LRUPlacerMeta).visited {
			visited = 1
		}
		elem[i] = fmt.Sprintf("%s-%d", meta.Key, visited)
	}
	return strings.Join(elem, ",")
}
