package lambdastore

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var _ = Describe("Instance", func() {
	It("should Status() output correctly.", func() {
		ins := &Instance{}
		Expect(ins.Status()).To(Equal(uint64(0x0000)))

		ins.status = INSTANCE_RUNNING
		Expect(ins.Status()).To(Equal(uint64(0x0001)))

		ins.recovering = 10
		Expect(ins.Status()).To(Equal(uint64(0x0101)))

		ins.recovering = 0
		ins.backing = 2
		Expect(ins.Status()).To(Equal(uint64(0x0201)))

		ins.recovering = 10
		Expect(ins.Status()).To(Equal(uint64(0x0301)))

		ins.phase = PHASE_BACKING_ONLY
		Expect(ins.Status()).To(Equal(uint64(0x1301)))

		ins.phase = PHASE_RECLAIMED
		Expect(ins.Status()).To(Equal(uint64(0x2301)))

		ins.phase = PHASE_EXPIRED
		Expect(ins.Status()).To(Equal(uint64(0x3301)))
	})

})
