package hashmap

import "sync/atomic"

type MapCounter struct {
	BaseHashMap
	size int64
}

func (m *MapCounter) Delete(key interface{}) {
	m.LoadAndDelete(key)
}

func (m *MapCounter) LoadAndDelete(key interface{}) (retVal interface{}, retExists bool) {
	retVal, retExists = m.BaseHashMap.LoadAndDelete(key)
	if retExists {
		atomic.AddInt64(&m.size, -1)
	}
	return
}

func (m *MapCounter) LoadOrStore(key interface{}, value interface{}) (interface{}, bool) {
	val, loaded := m.BaseHashMap.LoadOrStore(key, value)
	if !loaded {
		atomic.AddInt64(&m.size, 1)
	}
	return val, loaded
}

func (m *MapCounter) Store(key interface{}, val interface{}) {
	_, loaded := m.LoadOrStore(key, val)
	if loaded {
		m.BaseHashMap.Store(key, val)
	}
}

func (m *MapCounter) Len() int {
	return int(atomic.LoadInt64(&m.size))
}
