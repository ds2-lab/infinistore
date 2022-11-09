package metastore

import (
	"testing"
)

// var (
// 	emptyMeta Meta
// )

func (m *Meta) test() {
	// Do nothing
}

func BenchmarkNewMeta(b *testing.B) {
	var meta *Meta
	for i := 0; i < b.N; i++ {
		meta = &Meta{}
		meta.test()
	}
}

// func BenchmarkCopyMeta(b *testing.B) {
// 	meta := &Meta{}
// 	for i := 0; i < b.N; i++ {
// 		*meta = emptyMeta
// 		meta.test()
// 	}
// }

// func BenchmarkMetaPoolCopy(b *testing.B) {
// 	var meta *Meta
// 	for i := 0; i < b.N; i++ {
// 		meta = metaPool.Get().(*Meta)
// 		*meta = emptyMeta
// 		meta.test()
// 		meta.close()
// 	}
// }

func BenchmarkMetaPoolAssignment(b *testing.B) {
	var meta *Meta
	for i := 0; i < b.N; i++ {
		meta = newEmptyMeta()
		meta.test()
		meta.close()
	}
}
