package lambdastore

import (
	"sync"
	"sync/atomic"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var (
	TestAddress = "connectiontest"
	TestID      = int32(0)
)

func getTestID() int {
	return int(atomic.AddInt32(&TestID, 1))
}

func shouldTimeout(test func(), expect bool) {
	timer := time.NewTimer(time.Second)
	timeout := false
	responeded := make(chan struct{})
	go func() {
		test()
		responeded <- struct{}{}
	}()
	select {
	case <-timer.C:
		timeout = true
	case <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}

	Expect(timeout).Should(Equal(expect))
}

var _ = Describe("Connection", func() {
	protocol.InitShortcut()

	It("should cascade Close() block free.", func() {
		var done sync.WaitGroup
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), 2)

		ctrlLink := NewConnection(shortcut.Conns[0].Server)
		dataLink := NewConnection(shortcut.Conns[1].Server)

		done.Add(2)
		go func() {
			ctrlLink.ServeLambda()
			done.Done()
		}()
		go func() {
			dataLink.ServeLambda()
			done.Done()
		}()

		added := ctrlLink.AddDataLink(dataLink)
		Expect(added).To(BeTrue())

		shouldTimeout(func() {
			ctrlLink.Close()
		}, false)

		shouldTimeout(func() {
			done.Wait()
		}, false)
	})
})
