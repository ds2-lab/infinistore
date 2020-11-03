package cluster

import (
	"errors"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
)

var (
	ErrUnsupported = errors.New("unsupported")
)

type Cluster interface {
	lambdastore.InstanceManager
	metastore.InstanceManager

	Start() error
	WaitReady()
	GetPlacer() metastore.Placer
	CollectData()
	Close()
}
