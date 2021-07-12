package metastore

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

	InvalidPlacement = ^uint64(0)
)

type SliceInitializer func(int) (int, int)

type Slice interface {
	Size() int
	Reset(int)
	GetIndex(uint64) uint64
}

type Meta struct {
	Key       string
	Size      string
	DChunks   int
	PChunks   int
	NumChunks int
	Placement
	ChunkSize int64
	// Flag object has been reset by same value
	Reset bool
	// Flag object has been deleted
	Deleted bool

	slice      Slice
	placerMeta interface{}
	lastChunk  int
	mu         sync.Mutex
}

func NewEmptyMeta() *Meta {
	meta := metaPool.Get().(*Meta)
	meta.Key = ""
	meta.Size = "0"
	meta.DChunks = 0
	meta.PChunks = 0
	meta.NumChunks = 0
	meta.Placement = nil
	meta.ChunkSize = 0
	meta.Deleted = false
	meta.Reset = false
	meta.placerMeta = nil

	return meta
}

func NewMeta(key string, size string, d, p int, chunkSize int64) *Meta {
	meta := metaPool.Get().(*Meta)
	meta.Key = key
	meta.Size = size
	meta.DChunks = d
	meta.PChunks = p
	meta.NumChunks = int(d + p)
	meta.Placement = initPlacement(meta.Placement, meta.NumChunks)
	meta.ChunkSize = chunkSize
	meta.Deleted = false
	meta.Reset = false
	meta.placerMeta = nil

	return meta
}

func (m *Meta) close() {
	metaPool.Put(m)
}

func (m *Meta) ChunkKey(chunkId int) string {
	return fmt.Sprintf("%d@%s", chunkId, m.Key)
}

func (m *Meta) GetPlace(chunkId int) uint64 {
	return m.Placement[chunkId]
}

type Placement []uint64

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
