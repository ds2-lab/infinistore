package types

import (
	"errors"
	"fmt"
	"github.com/mason-leap-lab/redeo/resp"
	"strconv"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

const (
	OP_SET         = 0
	OP_GET         = 1
	OP_DEL         = 2
	OP_WARMUP      = 90
	OP_MIGRATION   = 91

	CHUNK_OK       = 0
	CHUNK_RECOVERING = 1
	CHUNK_LOCK     = 2
)

var (
	ErrProxyClosing               = errors.New("Proxy closed.")
	ErrNotFound                   = errors.New("Key not found")
)

type Storage interface {
	Get(string) (string, []byte, error)
	GetStream(string) (string, resp.AllReadCloser, error)
	Set(string, string, []byte) error
	SetStream(string, string, resp.AllReadCloser) error
	Len() int
	Del(string,string) error
	Keys()  <-chan string
}

type Lineage interface {
	IsConsistent(*protocol.Meta) (bool, error)
	TrackLineage()
	Commit() error
	StopTracker() *protocol.Meta
	Recover(*protocol.Meta) (bool, chan error)
}

type LineageTerm struct {
	Size     uint64        // Storage footprint of storing current term.
	Ops      []LineageOp  // Operations of the term.

	// Fields below will be serialized.
	Term     uint64        // Term id, increase on write operation.
	Updates  uint64        // Storage footprint of storing all terms of lineage so far.
	RawOps   []byte        // Serialized "Ops"
	Hash     string        // Hash value for the term.
	DiffRank float64       // For snapshot only, this is supposed to be a moving value.
}

type LineageOp struct {
	Op       uint32     // Operation, can be "OP_SET" or "OP_DEL"
	Key      string     // Key of the object
	Id       string     // Chunk id of the object
	Size     uint64     // Size of the object
	Accessed time.Time
	// Ret      chan error
	Bucket   string
}

// For storage
type Chunk struct {
	Key      string
	Id       string     // Obsoleted, chunk id of the object
	Body     []byte
	Size     uint64
	Term     uint64     // Lineage term of last write operation.
	Recovering uint32   // Recovering
	Accessed time.Time
	Bucket   string
}

func NewChunk(id string, body []byte) *Chunk {
	return &Chunk{ Id: id, Body: body, Size: uint64(len(body)), Accessed: time.Now() }
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

// For data collection
type DataEntry struct {
	Op             int
	Status         string
	ReqId          string
	ChunkId        string
	DurationAppend time.Duration
	DurationFlush  time.Duration
	Duration       time.Duration
	Session        string
}

type ResponseError struct {
	error
	StatusCode int
}

func NewResponseError(status int, msg interface{}, args ...interface{}) *ResponseError {
	switch msg.(type) {
	case error:
		return &ResponseError{
			error: msg.(error),
			StatusCode: status,
		}
	default:
		return &ResponseError{
			error: errors.New(fmt.Sprintf(msg.(string), args...)),
			StatusCode: status,
		}
	}
}

func (e *ResponseError) Status() string {
	return strconv.Itoa(e.StatusCode)
}
