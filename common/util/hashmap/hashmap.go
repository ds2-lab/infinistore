package hashmap

import (
	"sync"

	hashmap "github.com/orcaman/concurrent-map"
)

type BaseHashMap interface {
	Delete(interface{})
	Load(interface{}) (val interface{}, loaded bool)
	LoadAndDelete(interface{}) (val interface{}, exists bool)
	LoadOrStore(interface{}, interface{}) (val interface{}, loaded bool)
	CompareAndSwap(interface{}, interface{}, interface{}) (val interface{}, swapped bool)
	Range(func(interface{}, interface{}) (contd bool))
	Store(interface{}, interface{})
}

type HashMap interface {
	BaseHashMap
	Len() int
}

func NewMap(size int) HashMap {
	return &MapCounter{BaseHashMap: &syncMapWrapper{}}
}

type syncMapWrapper struct {
	sync.Map
}

// Just a dummy implementation to that will fail always.
func (m *syncMapWrapper) CompareAndSwap(key interface{}, old interface{}, new interface{}) (interface{}, bool) {
	v, _ := m.Load(key)
	return v, false
}

func NewMapWithStringKey(size int) HashMap {
	hashmap.SHARD_COUNT = 256
	return &MapCounter{BaseHashMap: &cMapWrapper{backend: hashmap.New()}}
}

type cMapWrapper struct {
	backend hashmap.ConcurrentMap
}

func (m *cMapWrapper) Delete(key interface{}) {
	m.backend.Remove(key.(string))
}

func (m *cMapWrapper) Load(key interface{}) (interface{}, bool) {
	return m.backend.Get(key.(string))
}

func (m *cMapWrapper) LoadAndDelete(key interface{}) (retVal interface{}, retExists bool) {
	m.backend.RemoveCb(key.(string), func(key string, val interface{}, exists bool) bool {
		retVal = val
		retExists = exists
		return true
	})
	return
}

func (m *cMapWrapper) LoadOrStore(key interface{}, value interface{}) (interface{}, bool) {
	set := m.backend.SetIfAbsent(key.(string), value)
	if set {
		return value, false
	} else {
		// To allow backend bugs be identified by the caller, return loaded value and true.
		// In case the backend's Load works incorrectly, the caller can detect it by checking the returned value.
		loaded, _ := m.Load(key)
		return loaded, true
	}
}

func (m *cMapWrapper) CompareAndSwap(key interface{}, oldVal interface{}, newVal interface{}) (interface{}, bool) {
	set := false
	m.backend.Upsert(key.(string), newVal, func(exist bool, valueInMap interface{}, newValue interface{}) interface{} {
		if valueInMap == oldVal {
			set = true
			return newValue
		} else {
			newVal = valueInMap
			return valueInMap
		}
	})
	return newVal, set
}

func (m *cMapWrapper) Range(cb func(interface{}, interface{}) bool) {
	breaked := false
	for item := range m.backend.IterBuffered() {
		if !breaked {
			breaked = !cb(item.Key, item.Val)
		}
		// iterate over all items to drain the channel
	}
}

func (m *cMapWrapper) Store(key interface{}, val interface{}) {
	m.backend.Set(key.(string), val)
}
