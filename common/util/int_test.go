package util

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WaitGroup", func() {
	It("should add1 increase itself", func() {
		var i Int

		Expect(i.Add(1)).To(Equal(1))
		Expect(i.Int()).To(Equal(1))
	})
})
