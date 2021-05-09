package lambdastore

import (
	"sync"
	"sync/atomic"

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

var _ = Describe("Connection", func() {
	protocol.InitShortcut()

	It("should cascade Close() block free.", func() {
		var done sync.WaitGroup
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), 2)

		instance := NewInstance("test", 1)
		ctrlLink := NewConnection(shortcut.Conns[0].Server).BindInstance(instance)
		dataLink := NewConnection(shortcut.Conns[1].Server).BindInstance(instance)

		done.Add(2)
		go func() {
			ctrlLink.ServeLambda()
			done.Done()
		}()
		go func() {
			dataLink.ServeLambda()
			done.Done()
		}()

		instance.lm.SetControl(ctrlLink)
		added := instance.lm.AddDataLink(dataLink)
		Expect(added).To(BeTrue())

		shouldTimeout(func() {
			instance.lm.Close()
		}, false)

		shouldTimeout(func() {
			done.Wait()
		}, false)
	})
})
