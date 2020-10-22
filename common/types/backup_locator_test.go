package types_test

import (
	"fmt"
	"log"
	"math"

	// "github.com/google/uuid"
	. "github.com/mason-leap-lab/infinicache/common/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BackupLocator", func() {
	locator := &BackupLocator{}

	It("should standard deviation of loads acorss backups around 10 percent", func() {
		n := 2400
		buckets := make([]int, 20)

		locator.Reset(len(buckets))
		for i := 0; i < n; i++ {
			bucket, _ := locator.Locate(fmt.Sprintf("key_%d", i))
			// bucket, _ := locator.Locate(uuid.New().String())
			buckets[bucket]++
		}

		sum := float64(0)
		sum2 := float64(0)
		max := float64(0)
		min := float64(n)
		for i := 0; i < len(buckets); i++ {
			sum += float64(buckets[i])
			sum2 += float64(buckets[i] * buckets[i])
			max = math.Max(max, float64(buckets[i]))
			min = math.Min(min, float64(buckets[i]))
		}

		l := float64(len(buckets))
		mean := sum / l
		variance := (l*sum2 - sum*sum) / l / (l - 1)
		log.Printf("mean: %f, max:%f, min:%f, sd: %f\n", mean, max, min, math.Sqrt(variance))
		Expect(math.Sqrt(variance)/mean < 0.10).To(Equal(true))
	})
})
