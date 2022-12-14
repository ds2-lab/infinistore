package atomic

import "sync/atomic"

type Int32 int32

func (i *Int32) Add(delta int32) (new int32) {
	return atomic.AddInt32((*int32)(i), delta)
}

func (i *Int32) CompareAndSwap(old, new int32) (swapped bool) {
	return atomic.CompareAndSwapInt32((*int32)(i), old, new)
}

func (i *Int32) Load() (val int32) {
	return atomic.LoadInt32((*int32)(i))
}

func (i *Int32) Store(val int32) {
	atomic.StoreInt32((*int32)(i), val)
}

func (i *Int32) Swap(new int32) (old int32) {
	return atomic.SwapInt32((*int32)(i), new)
}
