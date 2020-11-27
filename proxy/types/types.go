package types

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/mason-leap-lab/infinicache/common/util/promise"
	"github.com/mason-leap-lab/redeo/resp"
)

var ErrNoSpareDeployment = errors.New("no spare deployment")

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

type Conn interface {
	net.Conn
	Writer() *resp.RequestWriter
}

type Command interface {
	String() string
	GetRequest() *Request
	Retriable() bool
	Flush(time.Duration) error
}

type LambdaDeployment interface {
	Name() string
	Id() uint64
}

type MigrationScheduler interface {
	StartMigrator(uint64) (string, error)
	GetDestination(uint64) (LambdaDeployment, error)
}

type ClusterStats interface {
	InstanceLen() int
	InstanceStats(int) InstanceStats
	AllInstancesStats() Iterator
	InstanceStatsFromIterator(Iterator) (int, InstanceStats)
}

type GroupedClusterStats interface {
	ClusterLen() int
	ClusterStats(int) ClusterStats
	AllClustersStats() Iterator
	ClusterStatsFromIterator(Iterator) (int, ClusterStats)
}

type InstanceStats interface {
	Status() uint64
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
