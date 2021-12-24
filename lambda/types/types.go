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
	CHUNK_INCOMPLETE = 3

	CHUNK_TOBEBUFFERED = -1
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
	SetRecovery(string, string, uint64, int) *OpRet
	StartTracker()
	StopTracker()
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
	BuffIdx   int    // Index in buffer queue
	Note      string // Reason for the status.
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

func (c *Chunk) IsIncomplete() bool {
	return atomic.LoadUint32(&c.Status) == CHUNK_INCOMPLETE
}

func (c *Chunk) IsBuffered(includeTBD bool) bool {
	return c.BuffIdx > 0 || (includeTBD && c.BuffIdx == CHUNK_TOBEBUFFERED)
}

func (c *Chunk) Delete(reason string) {
	atomic.StoreUint32(&c.Status, CHUNK_DELETED)
	c.Body = nil
	c.Note = reason
}

// PrepareRecover initiate chunk for recovery.
// Return true if chunk is ready for wait.
func (c *Chunk) PrepareRecover() bool {
	c.Notifier.Add(1)
	if atomic.CompareAndSwapUint32(&c.Status, CHUNK_DELETED, CHUNK_RECOVERING) {
		c.Note = ""
		return true
	}
	c.Notifier.Done()
	return false
}

// StartRecover reset states of the winning chunk, so it is ready to start recovery
func (c *Chunk) StartRecover() {
	c.Notifier.Add(1)
	atomic.StoreUint64(&c.Available, 0)
}

// AddRecovered tracks recovery progress.
func (c *Chunk) AddRecovered(bytes uint64) bool {
	recovered := atomic.AddUint64(&c.Available, bytes)
	if recovered == c.Size {
		// Done CHUNK_RECOVERING
		c.EndRecover(CHUNK_AVAILABLE)
		return true
	} else {
		return false
	}
}

// NotifyRecovered notified concurrent requests that recovery has ended, success or not.
func (c *Chunk) EndRecover(status uint32) {
	// If still recovering, set to incomplete
	if atomic.CompareAndSwapUint32(&c.Status, CHUNK_RECOVERING, status) {
		c.Notifier.Done() // Done StartRecover
	}
}

// NotifyRecovered notified concurrent requests that recovery has ended, success or not.
func (c *Chunk) NotifyRecovered() {
	// If still recovering, set to incomplete
	c.EndRecover(CHUNK_INCOMPLETE)
	c.Notifier.Done() // Done PrepareRecover
}

func (c *Chunk) WaitRecovered() {
	c.Notifier.Wait()
}
