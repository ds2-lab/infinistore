package metastore

import (
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Placer", func() {
	It("should test chunk detect oversize", func() {
		placer := &DefaultPlacer{}

		ins := &lambdastore.Instance{}
		ins.ResetCapacity(1536000000, 1232400000)
		ins.Meta.IncreaseSize(1190681458)

		Expect(placer.testChunk(ins, 0)).To(Equal(true))
	})
})
