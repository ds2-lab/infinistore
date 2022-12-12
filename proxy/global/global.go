package global

import (
	"math/rand"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"

	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	// Clients        = make([]chan interface{}, 1024*1024)
	Options          CommandlineOptions
	DataCollected    sync.WaitGroup
	Log              logger.ILogger
	ReqCoordinator   = NewRequestCoordinator(1024)
	Migrator         types.MigrationScheduler
	BasePort         = 6378
	LambdaServePorts = 1
	BaseMigratorPort = 6400
	ServerIp         string
	LambdaFlags      uint64
)

func init() {
	rand.Seed(time.Now().UnixNano()) // Reseed random.
	Log = logger.NilLogger

	if ServerIp == "" {
		ip, err := GetPrivateIp()
		if err != nil {
			panic(err)
		}
		ServerIp = ip
	}

	LambdaFlags = config.LambdaFeatures
}

func IsWarmupWithFixedInterval() bool {
	return config.ProxyFeatures&config.FLAG_FIXED_INTERVAL_WARMUP > 0
}

func IsClientsideFirstDOptimization() bool {
	return config.ProxyFeatures&config.FLAG_CLIENTSIDE_FIRSTD_OPTIMIZATION > 0
}

func IsLocalCacheEnabled() bool {
	return config.ProxyFeatures&config.FLAG_ENABLE_LOCAL_CACHE > 0
}

func GetLogger(prefix string) logger.ILogger {
	return &logger.ColorLogger{
		Prefix: prefix,
		Level:  Log.GetLevel(),
		Color:  !Options.NoColor,
	}
}

func SetLoggerLevel(level int) {
	if color, ok := Log.(*logger.ColorLogger); ok {
		color.Level = level
	}
}
