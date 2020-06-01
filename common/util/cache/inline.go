package cache

// Inline cache is a general variable cache for costy operation.
// Example usage:
// type TypeA struct {
//   variable InlineCache
// }
//
// func NewTypeA() *TypeA {
// 	typeA := &TypeA{}
// 	typeA.variable.Producer = typeA.costOperation
// 	return typeA
// }
//
// func (f *Foo) GetVariable(args ...interface{}) TypeB {
// 	return f.variable.Value(args...)
// }
//
// func (f *Foo) costOperation(cached interface{}, args ...interface{}) (ret *TypeB) {
// 	// ret = Compute with args...
// 	return
//

type InlineProducer func(interface{}, ...interface{}) (interface{}, error)

type InlineValidator func(interface{}) bool

type InlineCache struct{
	Producer   InlineProducer
	Validator  InlineValidator

	cached     interface{}
}

func (c *InlineCache) Value(args ...interface{}) interface{} {
	cached, _ := c.ValueWithError(args...)
	return cached
}

func (c *InlineCache) ValueWithError(args ...interface{}) (cached interface{}, err error) {
	if (c.Validator != nil && !c.Validator(c.cached)) ||
		(c.Validator == nil && c.cached == nil) {
		c.cached, err = c.Producer(c.cached, args...)
	}
	return c.cached, err
}

func (c *InlineCache) Invalidate() {
	c.cached = nil
}

func InlineProducer0(f func() interface{}) InlineProducer {
	return func(interface{}, ...interface{}) (interface{}, error) {
		return f(), nil
	}
}

func InlineProducer0E(f func() (interface{}, error)) InlineProducer {
	return func(interface{}, ...interface{}) (interface{}, error) {
		return f()
	}
}
