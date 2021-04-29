package types

import (
	"errors"
	"sync"
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
)

var (
	ErrProxyClosing = errors.New("proxy closed")
	ErrNotFound     = errors.New("key not found")
)

type Loggable interface {
	ConfigLogger(int, bool)
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
	Key       string
	Id        string // Obsoleted, chunk id of the object
	Body      []byte
	Size      uint64
	Term      uint64 // Lineage term of last write operation.
	Deleted   bool
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
