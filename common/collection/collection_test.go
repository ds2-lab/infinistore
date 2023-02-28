package collection_test

import (
	"sync"
	"testing"

	syncCollection "github.com/ds2-lab/infinistore/common/collection/sync"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTypes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Collection")
}

var _ = Describe("Collection", func() {
	It("should instancize from slice", func() {
		s := make([]*sync.WaitGroup, 1)
		s[0] = &sync.WaitGroup{}

		c, err := syncCollection.WaitGroupCollectionFrom(s)
		Expect(err).To(BeNil())
		Expect(c).To(Not(BeNil()))
		Expect(c.Len()).To(Equal(1))
		Expect(c.Item(0)).To(Not(BeNil()))
	})

	It("should instancize from self shortcuted", func() {
		s := make([]*sync.WaitGroup, 1)
		s[0] = &sync.WaitGroup{}

		c, _ := syncCollection.WaitGroupCollectionFrom(s)
		copy, err := syncCollection.WaitGroupCollectionFrom(c)
		Expect(err).To(BeNil())
		Expect(copy).To(Equal(c))
	})
})
