package sync_test

import (
	"runtime"
	"sync"
	"testing"

	csync "github.com/ds2-lab/infinistore/common/sync"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestWaitGroup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "WaitGroup")
}

var _ = Describe("WaitGroup", func() {
	isBlock := func(wg *csync.WaitGroup) bool {
		return wg.IsWaiting()
	}

	It("should non-block initially", func() {
		var wg csync.WaitGroup

		Expect(isBlock(&wg)).To(Equal(false))
	})

	It("should block after added", func() {
		var wg csync.WaitGroup

		wg.Add(1)
		Expect(isBlock(&wg)).To(Equal(true))
	})

	It("should non-block after done", func() {
		var wg csync.WaitGroup

		wg.Add(1)
		wg.Done()
		runtime.Gosched()
		Expect(isBlock(&wg)).To(Equal(false))
	})

	It("should add 2 times", func() {
		var wg csync.WaitGroup

		wg.Add(1)
		wg.Done()
		wg.Add(1)
		wg.Done()
		runtime.Gosched()
		Expect(isBlock(&wg)).To(Equal(false))
	})

	It("can be reused after wait fulfilled", func() {
		var wg csync.WaitGroup

		wg.Add(1)
		wg.Done()
		wg.Wait()

		wg.Add(2)
		wg.Done()
		runtime.Gosched()
		Expect(isBlock(&wg)).To(Equal(true))

		wg.Done()
		runtime.Gosched()
		Expect(isBlock(&wg)).To(Equal(false))
	})

	It("can done only without error", func() {
		var wg csync.WaitGroup

		wg.Done()
		Expect(isBlock(&wg)).To(Equal(false))
	})

	It("can add after done", func() {
		var wg csync.WaitGroup

		wg.Done()
		wg.Add(1)
		Expect(isBlock(&wg)).To(Equal(false))
	})
})

func BenchmarkGoWaitgroupAll(b *testing.B) {
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		wg.Done()
		wg.Wait()
	}
}

func BenchmarkWaitgroupAll(b *testing.B) {
	var wg csync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		wg.Done()
		wg.Wait()
	}
}

func BenchmarkGoWaitgroupNonWaiting(b *testing.B) {
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Done()
	for i := 0; i < b.N; i++ {
		wg.Wait()
	}
}

func BenchmarkWaitgroupNonWaiting(b *testing.B) {
	var wg csync.WaitGroup
	wg.Add(1)
	wg.Done()
	for i := 0; i < b.N; i++ {
		wg.Wait()
	}
}

func BenchmarkWaitgroupTestWaiting(b *testing.B) {
	var wg csync.WaitGroup
	var waiting bool
	wg.Add(1)
	for i := 0; i < b.N; i++ {
		waiting = wg.IsWaiting()
	}
	waiting = !waiting
}

func BenchmarkWaitgroupTestNonWaiting(b *testing.B) {
	var wg csync.WaitGroup
	var waiting bool
	wg.Add(1)
	wg.Done()
	for i := 0; i < b.N; i++ {
		waiting = wg.IsWaiting()
	}
	waiting = !waiting
}
