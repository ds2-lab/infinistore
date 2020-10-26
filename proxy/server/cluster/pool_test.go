package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

//	"github.com/mason-leap-lab/infinicache/proxy"
)

var _ = Describe("Pool", func() {
	var pool *Pool

	setup := func() {
		pool = NewPool(2, 5)
	}

	It("should instance fetch return switched instance on migrate.", func() {
		setup()

		group := NewGroup(2)
		ins1 := pool.GetForGroup(group, DefaultGroupIndex(0))
		dpl2, _ := pool.ReserveForGroup(group, DefaultGroupIndex(0))
		ins11, _ := pool.Instance(dpl2.Id())

		Expect(ins11).To(Equal(ins1))
		Expect(ins11.Id()).To(Equal(uint64(1)))
		Expect(dpl2.Id()).To(Equal(uint64(0)))

		// fixed: delete keys in scheduler.actives wrongly
		ins12, _ := pool.Instance(ins1.Id())

		Expect(ins12).To(Equal(ins1))
		Expect(ins12.Id()).To(Equal(uint64(1)))
	})

})
