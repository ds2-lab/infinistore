package store

import (
	"github.com/mason-leap-lab/infinicache/common/logger"

	"github.com/mason-leap-lab/infinicache/lambda/storage"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

var (
	Store   types.Storage = (*storage.Storage)(nil)
	Persist types.PersistentStorage
	Lineage types.Lineage
	Log     = &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false}
)

func IsDebug() bool {
	return Log.Level == logger.LOG_LEVEL_ALL
}
