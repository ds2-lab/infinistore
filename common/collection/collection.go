package collection

import "reflect"

type Collection interface {
	Len() int
	Index(int) reflect.Value
}

type BaseCollection struct {
	c Collection
}

func (c *BaseCollection) Len() int {
	return c.c.Len()
}

func (c *BaseCollection) Index(i int) reflect.Value {
	return c.c.Index(i)
}

func From(c interface{}, ac interface{}) (interface{}, error) {
	ret := reflect.ValueOf(c)
	switch cc := ac.(type) {
	case Collection:
		ret.Elem().Field(0).Set(reflect.ValueOf(&BaseCollection{c: cc}))
	default:
		ret.Elem().Field(0).Set(reflect.ValueOf(&BaseCollection{c: reflect.ValueOf(ac)}))
	}
	return c, nil
}
