package metastore

import (
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	safesync "github.com/mason-leap-lab/infinicache/common/sync"
)

const (
	// MetaFlagValid flags that the meta is valid.
	// If the meta is invalid because of a former creation, some chunks may be created.
	// A new PUT should skip current version and advance to the next version.
	MetaFlagValid = int32(0x01)
	// MetaFlagCreated flags that the object is successfully created
	MetaFlagCreated = int32(0x02)
	// MetaFlagDeleted flags that the object is deleted
	MetaFlagDeleted = int32(0x04)

	replacerDelimiter = "=at."
	invalidVersion    = 0
)

var (
	RegDelimiter = regexp.MustCompile(`@`)
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

func santicizeKey(key string) string {
	return RegDelimiter.ReplaceAllString(key, replacerDelimiter)
}

func metaKeyByVersion(key string, ver int) string {
	return fmt.Sprintf("%s@v%d", key, ver)
}

// MetaKeyByVersion is the static version of Meta.KeyByVersion. Key will be santicized first.
func MetaKeyByVersion(key string, ver int) string {
	return metaKeyByVersion(santicizeKey(key), ver)
}

type Meta struct {
	key     string
	Size    int64
	DChunks int
	PChunks int
	Placement
	ChunkSize int64

	// Versioning parameters
	// Version
	version int
	// Timestamp of the version created.
	versionTs int64
	// Last Version
	lastVersion int
	// The requestId of initiate request
	initiator string
	// Flags to indicate if the status of object
	// 0x00: invalid
	// 0x01: valid and creating (initial state)
	// 0x03: created
	// 0x05: deleted
	// 0x01 -> 0x00 if creation failed or timeout.
	// 0x01 -> 0x03 if creation succeeded.
	// 0x03 -> 0x05 if deleted.
	flags int32

	slice      Slice
	deadline   int64 // Deadline for the creation operation.
	placerMeta interface{}
	lastChunk  int
	confirmed  safesync.WaitGroup
	mu         sync.Mutex
}

// For testing purpose
func newEmptyMeta() *Meta {
	meta := metaPool.Get().(*Meta)
	meta.key = ""
	meta.Size = 0
	meta.DChunks = 0
	meta.PChunks = 0
	meta.Placement = nil
	meta.ChunkSize = 0

	meta.version = 0
	meta.versionTs = 0
	meta.lastVersion = 0
	meta.initiator = ""
	meta.flags = 0

	meta.deadline = 0
	meta.placerMeta = nil

	return meta
}

func NewMeta(reqId string, key string, size int64, d, p int, chunkSize int64) *Meta {
	meta := metaPool.Get().(*Meta)
	meta.key = santicizeKey(key)
	meta.Size = size
	meta.DChunks = d
	meta.PChunks = p
	meta.Placement = initPlacement(meta.Placement, meta.NumChunks())
	meta.ChunkSize = chunkSize

	meta.version = 1
	meta.versionTs = time.Now().Unix()
	meta.lastVersion = 0
	meta.initiator = reqId
	meta.flags = MetaFlagValid

	meta.deadline = 0
	meta.placerMeta = nil
	meta.confirmed.Add(1)

	return meta
}

// Revise prepares the new meta created from calling NewMeta with new version infomation
func (m *Meta) ReviseBy(newMeta *Meta) *Meta {
	meta := newMeta

	meta.version = m.version + 1
	meta.lastVersion = m.version
	// Here we assume that the operation on the old version has concluded.
	// Its caller's responsibility to ensure that.
	if meta.flags&MetaFlagValid == 0 {
		// Shortcut the lastVersion with the last valid version.
		meta.lastVersion = m.lastVersion
	}

	return meta
}

// ReviseFrom updates the meta with new version information according to the specified meta of latest version.
func (m *Meta) ReviseFrom(lastVer *Meta) *Meta {
	return lastVer.ReviseBy(m)
}

func (m *Meta) Key() string {
	return m.key
}

func (m *Meta) VersioningKey() string {
	return metaKeyByVersion(m.key, m.version)
}

func (m *Meta) KeyByVersion(ver int) string {
	return metaKeyByVersion(m.key, ver)
}

func (m *Meta) ChunkKey(chunkId int) string {
	return fmt.Sprintf("%d@%s@v%d", chunkId, m.key, m.version)
}

func (m *Meta) NumChunks() int {
	return m.DChunks + m.PChunks
}

func (m *Meta) GetPlace(chunkId int) uint64 {
	return m.Placement[chunkId]
}

func (m *Meta) Version() int {
	return m.version
}

func (m *Meta) HasHistory() bool {
	return m.lastVersion > invalidVersion
}

func (m *Meta) PreviousVersion() int {
	return m.lastVersion
}

func (m *Meta) Invalidate() {
	// Ensure the meta will be set to invalid only once.
	if atomic.CompareAndSwapInt32(&m.flags, MetaFlagValid, 0) {
		m.confirmed.Done()
	}
}

func (m *Meta) Validate() bool {
	if m.IsCreated() {
		return true
	} else if m.IsValid() && time.Now().Unix() > m.deadline {
		m.Invalidate()
		return false
	} else {
		return m.IsValid()
	}
}

func (m *Meta) IsValid() bool {
	return m.flags&MetaFlagValid > 0
}

func (m *Meta) ConfirmCreated() {
	// Ensure the meta will be set to created only once.
	if atomic.CompareAndSwapInt32(&m.flags, MetaFlagValid, MetaFlagCreated|MetaFlagValid) {
		m.confirmed.Done()
	}
}

func (m *Meta) IsCreated() bool {
	return m.flags&MetaFlagCreated > 0
}

func (m *Meta) Delete() {
	m.flags |= MetaFlagDeleted
}

func (m *Meta) IsDeleted() bool {
	return m.flags&MetaFlagDeleted > 0
}

func (m *Meta) SetTimout(timeout time.Duration) {
	m.deadline = m.versionTs + int64(timeout)
}

func (m *Meta) Wait() {
	m.confirmed.Wait()
}

func (m *Meta) close() {
	m.Invalidate()
	metaPool.Put(m)
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
	return len(p) == 0
}
