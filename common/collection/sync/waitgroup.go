package syncCollection

import (
	"github.com/ds2-lab/infinistore/common/collection"
)

type WaitGroup interface {
	Add(int)
	Done()
	Wait()
}

type WaitGroupCollection interface {
	Len() int
	Item(int) WaitGroup
	Iterate(int) (int, WaitGroup, bool)
}

type waitGroupCollection struct {
	*collection.BaseCollection
}

func WaitGroupCollectionFrom(ac interface{}) (WaitGroupCollection, error) {
	if ac, ok := ac.(WaitGroupCollection); ok {
		return ac, nil
	}

	if ret, err := collection.From(&waitGroupCollection{}, ac); err != nil {
		return nil, err
	} else {
		return ret.(WaitGroupCollection), err
	}
}

func (c *waitGroupCollection) Item(i int) WaitGroup {
	return c.Index(i).Interface().(WaitGroup)
}

func (c *waitGroupCollection) Iterate(i int) (int, WaitGroup, bool) {
	if i >= c.Len() {
		return i, nil, false
	}

	return i, c.Item(i), true
}
