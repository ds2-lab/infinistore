package metastore

import (
	"github.com/zhangjyr/hashmap"
)

type MetaPostProcess func(MetaDoPostProcess)

//type MetaDoPostProcess func(*Meta)
type MetaDoPostProcess func(*Meta)

type MetaStore struct {
	metaMap *hashmap.HashMap
}

func New() *MetaStore {
	return &MetaStore{metaMap: hashmap.New(1024)}
}

func NewWithCapacity(size uintptr) *MetaStore {
	return &MetaStore{metaMap: hashmap.New(size)}
}

func (ms *MetaStore) GetOrInsert(key string, insert *Meta) (*Meta, bool, MetaPostProcess) {
	meta, ok := ms.metaMap.GetOrInsert(key, insert)
	return meta.(*Meta), ok, nil
}

func (ms *MetaStore) Get(key string) (*Meta, bool) {
	meta, ok := ms.metaMap.Get(key)
	if ok {
		return meta.(*Meta), ok
	} else {
		return nil, ok
	}
}

func (ms *MetaStore) Len() int {
	return ms.metaMap.Len()
}
