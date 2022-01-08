package storage

import (
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

type ChunkQueue struct {
	queue []*types.Chunk
	lru   bool
}

func (h *ChunkQueue) Len() int {
	return len(h.queue)
}

func (h *ChunkQueue) Less(i, j int) bool {
	// log.Printf("Less %d, %d (%v, %v) of %d", i, j, h[i], h[j], len(h))
	// Change LRU to Largest Chunk Size. Larger chunk will be evicted first.
	return h.lru == (h.queue[i].Size > h.queue[j].Size)
}

func (h ChunkQueue) Swap(i, j int) {
	// log.Printf("Swap %d, %d (%v, %v) of %d", i, j, h[i], h[j], len(h))
	h.queue[i].BuffIdx, h.queue[j].BuffIdx = h.queue[j].BuffIdx, h.queue[i].BuffIdx
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
}

func (h *ChunkQueue) Push(x interface{}) {
	x.(*types.Chunk).BuffIdx = len(h.queue) + 1 // Start with 1
	h.queue = append(h.queue, x.(*types.Chunk))
}

func (h *ChunkQueue) Pop() interface{} {
	old := h.queue
	n := len(old)
	ret := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.queue = old[0 : n-1]
	return ret
}

func (h ChunkQueue) Peek() *types.Chunk {
	if len(h.queue) == 0 {
		return nil
	}
	return h.queue[0]
}

func (h ChunkQueue) Chunk(idx int) *types.Chunk {
	if idx < len(h.queue) {
		return h.queue[idx]
	}
	return nil
}
