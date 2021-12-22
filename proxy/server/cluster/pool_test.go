package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pool", func() {
	var pool *Pool

	setup := func() {
		pool = NewPool(5, 5)
	}

	// It("should remove from actives after recycled.", func() {
	// 	setup()

	// 	Expect(pool.NumActives()).To(Equal(0))

	// 	group := NewGroup(5)
	// 	ins1 := pool.GetForGroup(group, DefaultGroupIndex(0))
	// 	pool.GetForGroup(group, DefaultGroupIndex(1))

	// 	Expect(pool.NumActives()).To(Equal(2))

	// 	pool.Recycle(ins1)

	// 	Expect(pool.NumActives()).To(Equal(1))

	// 	pool.GetForGroup(group, DefaultGroupIndex(2))
	// 	pool.GetForGroup(group, DefaultGroupIndex(3))

	// 	Expect(pool.NumActives()).To(Equal(3))
	// })

	It("should remain in actives after recycled.", func() {
		setup()

		Expect(pool.NumActives()).To(Equal(0))

		group := NewGroup(5)
		ins1 := pool.GetForGroup(group, DefaultGroupIndex(0))
		pool.GetForGroup(group, DefaultGroupIndex(1))

		Expect(pool.NumActives()).To(Equal(2))

		pool.Recycle(ins1)

		Expect(pool.NumActives()).To(Equal(2))

		pool.GetForGroup(group, DefaultGroupIndex(2))
		pool.GetForGroup(group, DefaultGroupIndex(3))

		Expect(pool.NumActives()).To(Equal(4))
	})

	It("should correctly clean actives including recycled instance.", func() {
		setup()

		Expect(pool.NumActives()).To(Equal(0))

		group := NewGroup(5)
		ins1 := pool.GetForGroup(group, DefaultGroupIndex(0))
		pool.GetForGroup(group, DefaultGroupIndex(1))

		Expect(pool.NumActives()).To(Equal(2))

		pool.Recycle(ins1)

		Expect(pool.NumActives()).To(Equal(2))

		pool.GetForGroup(group, DefaultGroupIndex(2))

		Expect(pool.NumActives()).To(Equal(3))

		Expect(func() { pool.Clear(group) }).ShouldNot(Panic())
	})

})
