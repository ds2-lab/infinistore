package metastore

import (
	"errors"

	"github.com/ds2-lab/infinistore/common/util/hashmap"
)

var (
	ErrConcurrentCreation = errors.New("concurrent creation")
)

type MetaPostProcess func(MetaDoPostProcess)

//type MetaDoPostProcess func(*Meta)
type MetaDoPostProcess func(*Meta)

type MetaStore struct {
	metaMap hashmap.HashMap
}

func New() *MetaStore {
	return &MetaStore{metaMap: hashmap.NewMapWithStringKey(1024)}
}

func NewWithCapacity(size int) *MetaStore {
	return &MetaStore{metaMap: hashmap.NewMapWithStringKey(size)}
}

// GetOrInsert get the meta if exists, otherwise insert a new one.
// The second return value is true if a existed meta is loaded.
func (ms *MetaStore) GetOrInsert(key string, insert *Meta) (*Meta, bool, error) {
	var latestMeta *Meta
	candidate := insert
	for {
		firstChunk := false
		if latestMeta == nil {
			m, ok := ms.metaMap.LoadOrStore(key, candidate)
			latestMeta, _ = m.(*Meta) // Update latestMeta
			firstChunk = !ok
		} else {
			m, ok := ms.metaMap.CompareAndSwap(key, latestMeta, candidate)
			latestMeta, _ = m.(*Meta) // Update latestMeta
			firstChunk = ok
		}

		// The candidate wins and makes the change.
		if firstChunk {
			ms.metaMap.Store(latestMeta.VersioningKey(), latestMeta) // Also, create a entry for the version key.
			return latestMeta, false, nil                            // Keep return consistent with the LoadOrStore call.
		}

		// The loaded meta was produced by the same request
		if latestMeta.initiator == candidate.initiator {
			// Leave caller to close the insert, so caller can still have access to the insert.
			if candidate != insert {
				candidate.close()
			}
			return latestMeta, true, nil
		}

		// Validate meta status first (so status can be concluded if PUT timeout),
		// And if another request is creating the object (valid but not yet created), return error.
		if latestMeta.Validate() && !latestMeta.IsCreated() {
			if candidate != insert {
				candidate.close()
			}
			return latestMeta, true, ErrConcurrentCreation
		}

		// Or(in case of invalidated, created, or deleted) we create a new version
		newVersion := latestMeta.ReviseBy(candidate)
		if candidate != insert {
			candidate.close()
		}
		candidate = newVersion

		// Try insert the new version again.
	}
}

func (ms *MetaStore) Get(key string) (*Meta, bool) {
	for {
		m, ok := ms.metaMap.Load(key)
		if !ok {
			return nil, ok
		}

		meta, _ := m.(*Meta)
		// Validate meta status first (so status can be concluded if PUT timeout),
		// And if the object is created (can be deleted), return the meta.
		if meta.Validate() && meta.IsCreated() {
			return meta, ok
		} else if meta.HasHistory() {
			// The request that created the meta has not succeeded (requesting or failed).
			// Load the previous version.
			key = meta.KeyByVersion(meta.PreviousVersion())
			// continue
		} else {
			// This is the first version
			return nil, false
		}
	}
}

func (ms *MetaStore) GetByVersion(key string, ver int) (*Meta, bool) {
	m, ok := ms.metaMap.Load(MetaKeyByVersion(key, ver))
	if !ok {
		return nil, ok
	}

	meta, _ := m.(*Meta)
	if meta.IsCreated() {
		return meta, ok
	} else {
		return nil, false
	}
}

func (ms *MetaStore) Len() int {
	return ms.metaMap.Len()
}
