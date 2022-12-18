package types

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/mason-leap-lab/redeo/resp"
)

var ErrNoSpareDeployment = errors.New("no spare deployment")

type InstanceOccupancyMode int

const (
	InstanceOccupancyMain InstanceOccupancyMode = iota
	InstanceOccupancyModified
	InstanceOccupancyMax
	InstanceOccupancyDisabled
	InstanceOccupancyMod
)

func (iom InstanceOccupancyMode) String() string {
	switch iom {
	case InstanceOccupancyMain:
		return "main"
	case InstanceOccupancyModified:
		return "modified"
	case InstanceOccupancyMax:
		return "max"
	default:
		return "disabled"
	}
}

type Id struct {
	ReqId    string
	ChunkId  string
	oldChunk *string
	chunk    int
}

func (id *Id) Chunk() int {
	if id.oldChunk != &id.ChunkId {
		id.chunk, _ = strconv.Atoi(id.ChunkId)
		id.oldChunk = &id.ChunkId
	}
	return id.chunk
}

func (id *Id) String() string {
	return fmt.Sprintf("%s(%s)", id.ReqId, id.ChunkId)
}

type Conn interface {
	net.Conn
	Writer() *resp.RequestWriter
}

type Command interface {
	Name() string
	String() string
	GetInfo() interface{}
	GetRequest() *Request
	// MarkError Mark an failure attempt to send request, return attempts left.
	MarkError(error) int
	// LastError Get last failure attempt, return attempts left and last error.
	LastError() (int, error)
	// FailureError Get error for failure (no attempt left.)
	FailureError() error
	Flush() error
}

type LambdaDeployment interface {
	Name() string
	Id() uint64
}

type MigrationScheduler interface {
	StartMigrator(uint64) (string, error)
	GetDestination(uint64) (LambdaDeployment, error)
}

type ServerStats interface {
	PersistCacheLen() int
}

type MetaStoreStats interface {
	Len() int
}

type ClusterStats interface {
	InstanceLen() int
	InstanceStats(int) InstanceStats
	AllInstancesStats() Iterator
	InstanceStatsFromIterator(Iterator) (int, InstanceStats)
	MetaStats() MetaStoreStats
}

type GroupedClusterStats interface {
	ClusterLen() int
	ClusterStats(int) ClusterStats
	AllClustersStats() Iterator
	ClusterStatsFromIterator(Iterator) (int, ClusterStats)
	MetaStats() MetaStoreStats
}

type InstanceStats interface {
	Status() uint64
	Occupancy(InstanceOccupancyMode) float64
}

type ScaleEvent struct {
	// BaseInstance Instance that triggers the scaling event.
	BaseInstance interface{}

	// ScaleTarget The number of instances to scale.
	ScaleTarget int

	// Scaled A promise object that can be used to wait for the completion of the scaling event.
	Scaled promise.Promise

	// Retire If there is insufficient space in BaseInstance, set to true to retire it.
	Retire bool

	// Reason for logging.
	Reason string
}

func (evt *ScaleEvent) SetError(err error) {
	if evt.Scaled != nil {
		evt.Scaled.Resolve(nil, err)
	}
}

func (evt *ScaleEvent) SetScaled() {
	if evt.Scaled != nil {
		evt.Scaled.Resolve()
	}
}
