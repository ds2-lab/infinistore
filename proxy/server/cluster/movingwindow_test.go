package cluster

import (
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var _ = Describe("MovingWindow", func() {
	It("should success to scale on demand if more active instances are requested", func() {
		cluster := NewMovingWindow()
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 2))

		instances := cluster.GetActiveInstances(config.NumLambdaClusters * 3)
		Expect(len(instances)).To(Equal(config.NumLambdaClusters * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 3))

		cluster.Close()
	})

	It("should success to scale on demand after rotated", func() {
		cluster := NewMovingWindow()
		lambdastore.CM = cluster
		cluster.Start()

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 2))

		Expect(cluster.Rotate()).To(BeTrue())

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 2))

		instances := cluster.GetActiveInstances(config.NumLambdaClusters * 3)
		Expect(len(instances)).To(Equal(config.NumLambdaClusters * 3))

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 3))

		cluster.Close()
	})

	It("should concurrent scaling ok", func() {
		cluster := NewMovingWindow()
		lambdastore.CM = cluster
		cluster.Start()
		concurrency := 100

		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 2))

		var done sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			done.Add(1)
			go func() {
				// defer GinkgoRecover()

				instances := cluster.GetActiveInstances(config.NumLambdaClusters * 3)
				Expect(len(instances)).To(Equal(config.NumLambdaClusters * 3))
				done.Done()
			}()
		}

		done.Wait()
		Expect(cluster.GetCurrentBucket().len()).To(Equal(config.NumLambdaClusters * 3))

		cluster.Close()
	})
})
