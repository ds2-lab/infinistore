package lambdastore

import (
	"fmt"

	"github.com/mason-leap-lab/infinicache/common/logger"
)

type Deployment struct {
	name string // Name of the function deployment, which will not change
	id   uint64 // ID of the instance
	log  logger.ILogger

	Block int
}

func NewDeployment(name string, id uint64) *Deployment {
	return &Deployment{
		name: fmt.Sprintf("%s%d", name, id),
		id:   id,
		log:  logger.NilLogger,
	}
}

func (d *Deployment) Name() string {
	return d.name
}

func (d *Deployment) Id() uint64 {
	return d.id
}
