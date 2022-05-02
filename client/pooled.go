package client

import (
	"github.com/mason-leap-lab/infinicache/common/sync"
)

type PooledClient struct {
	// Concurrency supported
	Concurrency int

	// Default # of data shards
	NumDataShards int

	// Default # of parity shards
	NumParityShards int

	// Max goroutine used by Erasure Coding
	ECMaxGoroutine int

	addrs []string
	pool  *sync.Pool
}

func NewPooledClient(addrArr []string, options ...func(*PooledClient)) *PooledClient {
	cli := &PooledClient{
		Concurrency:     5,
		NumDataShards:   10,
		NumParityShards: 2,
		ECMaxGoroutine:  32,
		addrs:           addrArr,
	}
	if len(options) > 0 {
		for _, option := range options {
			option(cli)
		}
	}

	cli.pool = sync.InitPool(&sync.Pool{
		New: func() interface{} {
			c := NewClient(cli.NumDataShards, cli.NumParityShards, cli.ECMaxGoroutine)
			c.Dial(cli.addrs)
			return c
		},
		Finalize: func(c interface{}) {
			c.(*Client).Close()
		},
	}, cli.Concurrency, sync.PoolForPerformance)

	return cli
}

func (c *PooledClient) Get(key string) (ReadAllCloser, error) {
	cli := c.pool.Get().(*Client)
	defer c.pool.Put(cli)

	_, reader, err := cli.EcGet(key)
	return reader, err
}

func (c *PooledClient) Set(key string, val []byte) error {
	cli := c.pool.Get().(*Client)
	defer c.pool.Put(cli)

	_, err := cli.EcSet(key, val)
	return err
}

func (c *PooledClient) Close() {
	c.pool.Close()
}
