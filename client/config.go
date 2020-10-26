package client

import (
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/config"
)

var (
	// MaxLambdaStores This setting will avoid network contention. Must equal or smaller than config.NumLambdaClusters
	MaxLambdaStores int = config.NumLambdaClusters

	// Timeout Operation timeout
	Timeout = 1 * time.Second
)
