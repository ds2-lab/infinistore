package metastore

import (
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
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

func (ms *MetaStore) GetOrInsert(key string, insert *Meta) (*Meta, bool, MetaPostProcess) {
	meta, ok := ms.metaMap.LoadOrStore(key, insert)
	return meta.(*Meta), ok, nil
}

func (ms *MetaStore) Get(key string) (*Meta, bool) {
	meta, ok := ms.metaMap.Load(key)
	if ok {
		return meta.(*Meta), ok
	} else {
		return nil, ok
	}
}

func (ms *MetaStore) Len() int {
	return ms.metaMap.Len()
}
