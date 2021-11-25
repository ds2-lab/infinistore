package types

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/redeo/resp"
)

const (
	OP_SET       = 0
	OP_GET       = 1
	OP_DEL       = 2
	OP_RECOVER   = 3
	OP_WARMUP    = 90
	OP_MIGRATION = 91
	OP_RECOVERY  = 92 // Recover repository
	OP_COMMIT    = 93 // Commit lineage

	CHUNK_AVAILABLE  = 0
	CHUNK_DELETED    = 1
	CHUNK_RECOVERING = 2
)

var (
	ErrProxyClosing = errors.New("proxy closed")
	ErrNotFound     = errors.New("key not found")
	ErrDeleted      = errors.New("key deleted")
	ErrIncomplete   = errors.New("key incomplete")
)

const ()

type Loggable interface {
	ConfigLogger(int, bool)
}

type CalibratePriority int

type StorageMeta interface {
	// Capacity is physical memory allowed.
	Capacity() uint64

	// System is real memory used.
	System() uint64

	// Waterline is max memory used.
	Waterline() uint64

	// Effectetive is dynamic capacity calculated.
	Effective() uint64

	// Reserved is reserved capacity configured.
	Reserved() uint64

	// Size is the size stored.
	Size() uint64

	// Calibrate adjusts capacity after each invocation.
	Calibrate()
}

type Storage interface {
	Id() uint64
	Get(string) (string, []byte, *OpRet)
	GetStream(string) (string, resp.AllReadCloser, *OpRet)
	Set(string, string, []byte) *OpRet
	SetStream(string, string, resp.AllReadCloser) *OpRet
	Del(string, string) *OpRet
	Len() int
	Keys() <-chan string
	Meta() StorageMeta
}

type PersistentStorage interface {
	Storage

	ConfigS3(string, string)
	SetRecovery(string, string, uint64) *OpRet
	StartTracker()
	StopTracker(interface{})
}

// For storage
type Chunk struct {
	Key  string
	Id   string // Obsoleted, chunk id of the object
	Body []byte
	Size uint64
	Term uint64 // Lineage term of last write operation.
	// Status of the chunk, can be one of CHUNK_AVAILABLE, CHUNK_DELETED, or CHUNK_RECOVERING
	// CHUNK_RECOVERING is only used for ensure atomicity, check Available to ensure recovery status.
	Status    uint32
	Available uint64         // Bytes available now. Used for recovering
	Notifier  sync.WaitGroup // See benchmarks in github.com/mason-leap-lab/infinicache/common/sync
	Accessed  time.Time
	Bucket    string
	Backup    bool
}

func NewChunk(key string, id string, body []byte) *Chunk {
	return &Chunk{
		Key:       key,
		Id:        id,
		Body:      body,
		Size:      uint64(len(body)),
		Available: uint64(len(body)),
		Accessed:  time.Now(),
	}
}

func (c *Chunk) Access() []byte {
	c.Accessed = time.Now()
	return c.Body
}

func (c *Chunk) Op() uint32 {
	if c.Body == nil && c.Size > 0 {
		return OP_DEL
	} else {
		return OP_SET
	}
}

func (c *Chunk) IsAvailable() bool {
	return atomic.LoadUint32(&c.Status) == CHUNK_AVAILABLE && atomic.LoadUint64(&c.Available) == c.Size
}

func (c *Chunk) IsDeleted() bool {
	return atomic.LoadUint32(&c.Status) == CHUNK_DELETED
}

func (c *Chunk) IsRecovering() bool {
	return atomic.LoadUint32(&c.Status) == CHUNK_RECOVERING || atomic.LoadUint64(&c.Available) < c.Size
}

func (c *Chunk) Delete() {
	atomic.StoreUint32(&c.Status, CHUNK_DELETED)
	c.Body = nil
}

func (c *Chunk) PrepareRecover() bool {
	c.Notifier.Add(1)
	if atomic.CompareAndSwapUint32(&c.Status, CHUNK_DELETED, CHUNK_RECOVERING) {
		return true
	}
	c.Notifier.Done()
	return false
}

func (c *Chunk) StartRecover() {
	c.Notifier.Add(1)
	atomic.StoreUint64(&c.Available, 0)
	if atomic.CompareAndSwapUint32(&c.Status, CHUNK_RECOVERING, CHUNK_AVAILABLE) {
		c.Notifier.Done() // Done CHUNK_RECOVERING
	}
}

func (c *Chunk) AddRecovered(bytes uint64, notify bool) {
	recovered := atomic.AddUint64(&c.Available, bytes)
	if notify && recovered == c.Size {
		c.Notifier.Done()
	}
}

func (c *Chunk) NotifyRecovered() {
	c.Notifier.Done()
}

func (c *Chunk) WaitRecovered() {
	c.Notifier.Wait()
}
