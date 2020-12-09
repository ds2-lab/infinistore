package client

import (
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/config"
)

var (
	// MaxLambdaStores This setting will avoid network contention. Must equal or smaller than config.NumLambdaClusters
	MaxLambdaStores int = config.NumLambdaClusters

	// Timeout The timeout of a single operation
	Timeout = 10 * time.Second

	// Timeout The timeout for sending header fields, and reading response headers.
	HeaderTimeout = 1 * time.Second
)
