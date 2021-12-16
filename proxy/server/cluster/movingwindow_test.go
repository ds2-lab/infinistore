package cluster

import (
	"math/rand"
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
		MaxBackingNodes = 10
		cluster := NewMovingWindowWithOptions(NumFunctions)
		cluster.backupQueue = nil // disable candidateQueue
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		instances := cluster.GetActiveInstances(NumFunctions * 3)
		Expect(instances.Len()).To(Equal(NumFunctions * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})

	It("should success to scale on demand after rotated", func() {
		MaxBackingNodes = 10
		cluster := NewMovingWindowWithOptions(NumFunctions)
		cluster.backupQueue = nil // disable candidateQueue
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		_, _, err := cluster.Rotate()
		Expect(err).To(BeNil())

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		instances := cluster.GetActiveInstances(NumFunctions * 3)
		Expect(instances.Len()).To(Equal(NumFunctions * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})

	It("should concurrent scaling ok", func() {
		MaxBackingNodes = 10
		cluster := NewMovingWindowWithOptions(NumFunctions)
		cluster.backupQueue = nil // disable candidateQueue
		lambdastore.CM = cluster
		cluster.Start()
		concurrency := 1000

		Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 2))

		var done sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			done.Add(1)
			go func() {
				defer GinkgoRecover()

				random := rand.Intn(20) + 1
				cluster.GetActiveInstances(NumFunctions * random)
				// Expect(len(instances)).To(Equal(NumFunctions * random))
				done.Done()
			}()
		}

		done.Wait()
		// Expect(cluster.GetCurrentBucket().len()).To(Equal(NumFunctions * 3))

		cluster.Close()
	})
})
