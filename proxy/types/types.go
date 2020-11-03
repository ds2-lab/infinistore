package types

import (
	"errors"
	"net"
	"strconv"
	"time"

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
	Len() int
	InstanceStats(int) InstanceStats
	AllInstancesStats() Iterator
	InstanceStatsFromIterator(Iterator) (int, InstanceStats)
}

type GroupedClusterStats interface {
	Len() int
	ClusterStats(int) ClusterStats
	AllClustersStats() Iterator
	ClusterStatsFromIterator(Iterator) (int, ClusterStats)
	InstanceSum(int) int
}

type InstanceStats interface {
	Status() uint64
}
