package cache

import (
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type persistCache struct {
	hashmap hashmap.HashMap // Should be fine if total key is under 1000.
	pool    sync.Pool       // For storing uninitialized chunks.
	log     logger.ILogger
}

func NewPersistCache() types.PersistCache {
	cache := &persistCache{hashmap: hashmap.NewMapWithStringKey(1024)}
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

	got, loaded := c.hashmap.LoadOrStore(key, prepared)
	reported := false
	for loaded && got == nil {
		if !reported {
			c.log.Warn("Nil chunk found in GetOrCreate. Yield to other goroutines and load again.")
			reported = true
		}
		runtime.Gosched()
		// Unlikely to happen, but just in case.
		got, _ = c.hashmap.Load(key) // Don't overwrite "loaded"
	}
	for loaded && got.(*persistChunk).Error() != nil {
		// The chunk stored is in error state, we need to replace it with a new one.
		_, swapped := c.hashmap.CompareAndSwap(key, got, prepared)
		if swapped {
			loaded = false
			got = prepared
			break
		} else {
			got, loaded = c.hashmap.Load(key)
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
	if got, exist := c.hashmap.Load(key); !exist {
		return nil
	} else {
		return got.(*persistChunk)
	}
}

func (c *persistCache) Restore() error {
	return types.ErrUnimplemented
}

func (c *persistCache) Report() {
	c.log.Info("Total keys: %d", c.hashmap.Len())
	if c.hashmap.Len() == 0 {
		return
	}
	c.log.Warn("Remaining keys:%v", logger.NewFunc(func() string {
		var msg strings.Builder
		c.hashmap.Range(func(_, chunk interface{}) bool {
			msg.WriteString(chunk.(fmt.Stringer).String())
			msg.WriteString(":")
			msg.WriteString(fmt.Sprintf("%v", chunk.(*persistChunk).Error()))
			msg.WriteString(" ")
			return true
		})
		return msg.String()
	}))
}

func (c *persistCache) remove(key string, chunk types.PersistChunk) {
	existed, _ := c.hashmap.Load(key)
	if existed == nil || existed == chunk {
		c.hashmap.Delete(key)
	}
}
