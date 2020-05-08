package lambdastore

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"


	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var _ = Describe("Instance", func() {
	It("should Switch() change id and name.", func() {
		ins := NewInstanceFromDeployment(NewDeployment("Inst", 1, false))
		deploy := NewDeployment("Inst", 2, false)

		ins.Switch(deploy)

		Expect(ins.Id()).To(Equal(uint64(2)))
		Expect(ins.Name()).To(Equal("Inst2"))
		Expect(deploy.Id()).To(Equal(uint64(1)))
		Expect(deploy.Name()).To(Equal("Inst1"))
	})

	It("should Status() output correctly.", func() {
		ins := &Instance{}
		Expect(ins.Status()).To(Equal(uint64(0x0000)))

		ins.started = INSTANCE_STARTED
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
