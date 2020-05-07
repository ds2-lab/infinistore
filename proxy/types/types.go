package types

import (
	"errors"
	"strconv"
)

var ErrNoSpareDeployment = errors.New("No spare deployment")

type Id struct {
	ConnId   int
	ReqId    string
	ChunkId  string
	oldChunk *string
	chunk    int
}

type Command interface {
	String() string
	GetRequest() *Request
	Retriable() bool
	Flush() error
}

type LambdaDeployment interface {
	Name() string
	Id() uint64
	Reset(new LambdaDeployment, old LambdaDeployment)
}

type MigrationScheduler interface {
	StartMigrator(uint64) (string, error)
	GetDestination(uint64) (LambdaDeployment, error)
}

func (id *Id) Chunk() int {
	if id.oldChunk != &id.ChunkId {
		id.chunk, _ = strconv.Atoi(id.ChunkId)
		id.oldChunk = &id.ChunkId
	}
	return id.chunk
}
