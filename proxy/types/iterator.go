package types

type Iterator interface {
	Len() int
	Next() bool
	Value() (int, interface{})
}

type StatsIterator struct {
	arr  interface{}
	len  int
	i    int
}

func NewStatsIterator(arr interface{}, len int) *StatsIterator {
	return &StatsIterator{ arr: arr, len: len, i: -1 }
}

func (iter *StatsIterator) Len() int {
	return iter.len
}

func (iter *StatsIterator) Next() bool {
	iter.i++
	return iter.i < iter.len
}

func (iter *StatsIterator) Value() (int, interface{}) {
	return iter.i, iter.arr
}
