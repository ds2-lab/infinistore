package cluster

import (
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var (
	NumFunctions = 12
)

var _ = Describe("MovingWindow", func() {
	It("should success to scale on demand if more active instances are requested", func() {
		cluster := NewMovingWindowWithOptions(NumFunctions)
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		instances := cluster.GetActiveInstances(NumFunctions * 3)
		Expect(len(instances)).To(Equal(NumFunctions * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})

	It("should success to scale on demand after rotated", func() {
		cluster := NewMovingWindowWithOptions(NumFunctions)
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		Expect(cluster.Rotate()).To(BeTrue())

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		instances := cluster.GetActiveInstances(NumFunctions * 3)
		Expect(len(instances)).To(Equal(NumFunctions * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})

	It("should concurrent scaling ok", func() {
		cluster := NewMovingWindowWithOptions(NumFunctions)
		lambdastore.CM = cluster
		cluster.Start()
		concurrency := 100

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		var done sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			done.Add(1)
			go func() {
				// defer GinkgoRecover()

				instances := cluster.GetActiveInstances(NumFunctions * 3)
				Expect(len(instances)).To(Equal(NumFunctions * 3))
				done.Done()
			}()
		}

		done.Wait()
		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})
})
