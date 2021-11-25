package storage

import (
	"math"
	"runtime"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const CalibrateFactor = 0.9

const (
	CalibratePriorityNormal types.CalibratePriority = iota
	CalibratePriorityRecover
	CalibratePriorityMax = CalibratePriorityRecover
)

type StorageMeta struct {
	Cap      uint64 // Capacity of the lambda.
	Overhead uint64 // Minimum overhead reserved.
	Rsrved   uint64 // Other reserved storage capacity.

	overhead float64 // Total storage overhead/usable capacity.
	size     uint64  // Size of objects stored.
	bakSize  uint64  // Size of backup objects stored.
	memStat  runtime.MemStats
}

func (m *StorageMeta) Capacity() uint64 {
	return m.Cap
}

func (m *StorageMeta) System() uint64 {
	return m.memStat.Sys - m.memStat.HeapSys - m.memStat.GCSys + m.memStat.HeapInuse
}

func (m *StorageMeta) Waterline() uint64 {
	return m.memStat.Sys
}

func (m *StorageMeta) Effective() uint64 {
	return m.Cap - uint64(math.Ceil(m.overhead)) - m.Rsrved
}

func (m *StorageMeta) Reserved() uint64 {
	return m.Rsrved
}

func (m *StorageMeta) Size() uint64 {
	return atomic.LoadUint64(&m.size)
}

func (m *StorageMeta) BackupSize() uint64 {
	return m.bakSize
}

func (m *StorageMeta) IncreaseSize(inc uint64) uint64 {
	return atomic.AddUint64(&m.size, inc)
}

func (m *StorageMeta) DecreaseSize(dec uint64) uint64 {
	return atomic.AddUint64(&m.size, ^(dec - 1))
}

func (m *StorageMeta) Calibrate() {
	runtime.GC()
	runtime.ReadMemStats(&m.memStat)
	overhead := float64(m.memStat.Sys - m.size)
	if overhead > m.overhead {
		m.overhead = overhead*CalibrateFactor + m.overhead*(1-CalibrateFactor)
	} else {
		m.overhead = overhead*(1-CalibrateFactor) + m.overhead*CalibrateFactor
	}
	if m.overhead < float64(m.Overhead) {
		m.overhead = float64(m.Overhead)
	}
}
