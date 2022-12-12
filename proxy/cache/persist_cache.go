package cache

import (
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/zhangjyr/hashmap"
)

type persistCache struct {
	hashmap *hashmap.HashMap // Should be fine if total key is under 1000.
	pool    sync.Pool        // For storing uninitialized chunks.
	log     logger.ILogger
}

func NewPersistCache() types.PersistCache {
	cache := &persistCache{hashmap: hashmap.New(1024)}
	cache.log = global.GetLogger("PersistCache ")
	return cache
}

func (c *persistCache) Len() int {
	return c.hashmap.Len()
}

func (c *persistCache) GetOrCreate(key string, size int64) (chunk types.PersistChunk, first bool) {
	prepared := c.pool.Get()
	if prepared == nil {
		prepared = newPersistChunk(c, key, size)
	} else {
		prepared.(*persistChunk).reset(key, size)
	}

	got, loaded := c.hashmap.GetOrInsert(key, prepared)
	for loaded && got.(*persistChunk).Error() != nil {
		// The chunk stored is in error state, we need to replace it with a new one.
		swapped := c.hashmap.Cas(key, got, prepared)
		if swapped {
			loaded = false
			got = prepared
		} else {
			// Call GetOrInsert again to get the latest chunk and prevent the chunk being removed.
			got, loaded = c.hashmap.GetOrInsert(key, prepared)
		}
	}

	// Recycle the chunk if it's not in the cache.
	if loaded {
		c.pool.Put(prepared)
	}

	if !loaded {
		c.log.Debug("%s: Created", got.(*persistChunk).Key())
	}
	return got.(*persistChunk), !loaded
}

func (c *persistCache) Get(key string) (chunk types.PersistChunk) {
	if got, exist := c.hashmap.Get(key); !exist {
		return nil
	} else {
		return got.(*persistChunk)
	}
}

func (c *persistCache) Restore() error {
	return types.ErrUnimplemented
}

func (c *persistCache) remove(key string) {
	c.hashmap.Del(key)
}
