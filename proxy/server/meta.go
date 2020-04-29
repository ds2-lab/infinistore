package server

import (
	"fmt"
	"sync"
)

var (
	metaPool = sync.Pool{
		New: func() interface{} {
			return &Meta{}
		},
	}
	emptyPlacement = make(Placement, 100)
)

type Meta struct {
	Key          string
	Size         int64
	DChunks      int64
	PChunks      int64
	NumChunks    int
	Placement
	ChunkSize    int64
	Reset        bool
	Deleted      bool

	Balanced    int32
	slice       Slice
	placerMeta  *PlacerMeta
	lastChunk   int64
	mu          sync.Mutex
}

func NewMeta(key string, size, d, p, chunkSize int64) *Meta {
	meta := metaPool.Get().(*Meta)
	meta.Key = key
	meta.Size = size
	meta.DChunks = d
	meta.PChunks = p
	meta.NumChunks = int(d + p)
	meta.Placement = initPlacement(meta.Placement, meta.NumChunks)
	meta.ChunkSize = chunkSize
	meta.Deleted = false
	meta.Balanced = 0

	if meta.slice.initialized {
		meta.slice = Slice{}
	}
	meta.placerMeta = nil

	return meta
}

func (m *Meta) close() {
	metaPool.Put(m)
}

func (m *Meta) ChunkKey(chunkId int) string {
	return fmt.Sprintf("%d@%s", chunkId, m.Key)
}

type Placement []int

func initPlacement(p Placement, sz int) Placement {
	if p == nil && sz == 0 {
		return p
	} else if p == nil || cap(p) < sz {
		return make(Placement, sz)
	} else {
		p = p[:sz]
		if sz > 0 {
			copy(p, emptyPlacement[:sz])
		}
		return p
	}
}

func copyPlacement(p Placement, src Placement) Placement {
	sz := len(src)
	if p == nil || cap(p) < sz {
		p = make(Placement, sz)
	}
	copy(p, src)
	return p
}

func IsPlacementEmpty(p Placement) bool {
	return p == nil || len(p) == 0
}
