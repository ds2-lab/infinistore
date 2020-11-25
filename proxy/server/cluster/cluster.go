package cluster

import (
	"errors"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
)

var (
	ErrUnsupported = errors.New("unsupported")
	ErrClusterClosed = errors.New("err cluster closed")
)

type Cluster interface {
	lambdastore.InstanceManager
	lambdastore.Relocator
	metastore.ClusterManager

	Start() error
	WaitReady()
	GetPlacer() metastore.Placer
	CollectData()
	Close()
}
