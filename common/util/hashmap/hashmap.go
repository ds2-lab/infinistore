package hashmap

import (
	"sync"

	hashmap "github.com/orcaman/concurrent-map"
)

type BaseHashMap interface {
	Delete(interface{})
	Load(interface{}) (interface{}, bool)
	LoadAndDelete(interface{}) (interface{}, bool)
	LoadOrStore(interface{}, interface{}) (interface{}, bool)
	Range(func(interface{}, interface{}) bool)
	Store(interface{}, interface{})
}

type HashMap interface {
	BaseHashMap
	Len() int
}

func NewMap(size int) HashMap {
	return &MapCounter{BaseHashMap: &sync.Map{}}
}

func NewMapWithStringKey(size int) HashMap {
	hashmap.SHARD_COUNT = 256
	return &MapCounter{BaseHashMap: &hashMap{backend: hashmap.New()}}
}

type hashMap struct {
	backend hashmap.ConcurrentMap
}

func (m *hashMap) Delete(key interface{}) {
	m.backend.Remove(key.(string))
}

func (m *hashMap) Load(key interface{}) (interface{}, bool) {
	return m.backend.Get(key.(string))
}

func (m *hashMap) LoadAndDelete(key interface{}) (retVal interface{}, retExists bool) {
	m.backend.RemoveCb(key.(string), func(key string, val interface{}, exists bool) bool {
		retVal = val
		retExists = exists
		return true
	})
	return
}

func (m *hashMap) LoadOrStore(key interface{}, value interface{}) (interface{}, bool) {
	set := m.backend.SetIfAbsent(key.(string), value)
	if set {
		return value, false
	} else {
		return m.Load(key.(string))
	}
}

func (m *hashMap) Range(cb func(interface{}, interface{}) bool) {
	breaked := false
	for item := range m.backend.IterBuffered() {
		if !breaked {
			breaked = !cb(item.Key, item.Val)
		}
		// iterate over all items to drain the channel
	}
}

func (m *hashMap) Store(key interface{}, val interface{}) {
	m.backend.Set(key.(string), val)
}
