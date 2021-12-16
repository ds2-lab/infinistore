package lambdastore

import (
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/kelindar/binary"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/config"
)

// type ChuckMeta {
// 	Key string
// 	Size uint64
// 	Hit bool
// 	Accessed time.Time
//
// 	prev *ChunkMeta
// 	next *ChunkMeta
// }

// FULL = (Updates - SnapshotUpdates + SnapshotSize) / Bandwidth + (Term - SnapShotTerm + 1) * RTT
// INCREMENTAL = (Updates - LastUpdates) / Bandwidth + (Seq - LastSeq) * RTT
// FULL < INCREMENTAL
type Meta struct {
	// Sequence of the last confirmed log. Logs store by sequence.
	Term uint64

	// Total transmission size for restoring all confirmed logs.
	Updates uint64

	// Rank for lambda to decide if a fast recovery is required.
	DiffRank float64

	// Hash of the last confirmed log.
	Hash string

	// Sequence of snapshot.
	SnapshotTerm uint64

	// Total transmission size for restoring all confirmed logs from start to SnapShotSeq.
	SnapshotUpdates uint64

	// Total size of snapshot for transmission.
	SnapshotSize uint64

	// Flag shows that if meta is out of sync with the corresponding lambda.
	Stale bool

	// Capacity of the instance.
	Capacity uint64

	effective    uint64
	size         uint64 // Size of the instance.
	mem          uint64 // Lambda memory waterline
	sizeModified uint64 // Size of object stored including buffered.
	// chunks map[string]*ChuckMeta
	// head ChuckMeta
	// anchor *ChuckMeta
	numChunks int32
}

func (m *Meta) ResetCapacity(capacity uint64, effective uint64) {
	m.Capacity = capacity
	if effective == uint64(0) {
		m.effective = capacity - m.ReservedCapacity()
	} else {
		m.effective = effective
	}
}

func (m *Meta) EffectiveCapacity() uint64 {
	return m.effective
}

func (m *Meta) ModifiedOccupancy(adjustment uint64) float64 {
	return float64(m.Size()+adjustment) / float64(m.EffectiveCapacity())
}

func (m *Meta) ReservedCapacity() uint64 {
	return config.InstanceOverhead
}

func (m *Meta) Size() uint64 {
	return atomic.LoadUint64(&m.size)
}

func (m *Meta) NumChunks() int {
	return int(atomic.LoadInt32(&m.numChunks))
}

func (m *Meta) IncreaseSize(inc int64) uint64 {
	return atomic.AddUint64(&m.size, uint64(inc))
}

func (m *Meta) DecreaseSize(dec int64) uint64 {
	return atomic.AddUint64(&m.size, ^uint64(dec-1))
}

func (m *Meta) AddChunk(key string, sz int64) (num int, size uint64) {
	num = int(atomic.AddInt32(&m.numChunks, 1))
	size = m.IncreaseSize(sz)
	return
}

func (m *Meta) RemoveChunk(key string, sz int64) (num int, size uint64) {
	size = m.DecreaseSize(sz)
	num = int(atomic.AddInt32(&m.numChunks, -1))
	return
}

func (m *Meta) FromProtocolMeta(meta *protocol.Meta) (bool, error) {
	if meta.Term < m.Term {
		if meta.Term == 1 {
			return false, ErrInstanceReclaimed
		} else {
			return false, nil
		}
	}

	m.Term = meta.Term
	m.Updates = meta.Updates
	m.DiffRank = meta.DiffRank
	m.Hash = meta.Hash
	m.SnapshotTerm = meta.SnapshotTerm
	m.SnapshotUpdates = meta.SnapshotUpdates
	m.SnapshotSize = meta.SnapshotSize
	return true, nil
}

func (m *Meta) ToProtocolMeta(id uint64) *protocol.Meta {
	return &protocol.Meta{
		Id:              id,
		Term:            m.Term,
		Updates:         m.Updates,
		DiffRank:        m.DiffRank,
		Hash:            m.Hash,
		SnapshotTerm:    m.SnapshotTerm,
		SnapshotUpdates: m.SnapshotUpdates,
		SnapshotSize:    m.SnapshotSize,
	}
}

func (m *Meta) ToBackupPayload(id uint64, key int, total int, maxChunkSize uint64) ([]byte, error) {
	meta := m.ToProtocolMeta(id)
	tips := &url.Values{}
	tips.Set(protocol.TIP_BACKUP_KEY, strconv.Itoa(key))
	tips.Set(protocol.TIP_BACKUP_TOTAL, strconv.Itoa(total))
	tips.Set(protocol.TIP_MAX_CHUNK, strconv.FormatUint(maxChunkSize, 10))
	meta.Tip = tips.Encode()

	return binary.Marshal(meta)
}

func (m *Meta) ToDelegatePayload(id uint64, key int, total int, maxChunkSize uint64) (*protocol.Meta, []byte, error) {
	meta := m.ToProtocolMeta(id)
	tips := &url.Values{}
	tips.Set(protocol.TIP_DELEGATE_KEY, strconv.Itoa(key))
	tips.Set(protocol.TIP_DELEGATE_TOTAL, strconv.Itoa(total))
	tips.Set(protocol.TIP_MAX_CHUNK, strconv.FormatUint(maxChunkSize, 10))
	meta.Tip = tips.Encode()

	payload, err := binary.Marshal(meta)

	return meta, payload, err
}
