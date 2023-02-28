package lambdastore

import (
	"sync/atomic"

	"github.com/ds2-lab/infinistore/common/net"
	. "github.com/onsi/ginkgo"
	// . "github.com/onsi/gomega"
	// . "github.com/ds2-lab/infinistore/proxy/lambdastore"
)

var (
	TestAddress = "connectiontest"
	TestID      = int32(0)
)

func getTestID() int {
	return int(atomic.AddInt32(&TestID, 1))
}

var _ = Describe("Connection", func() {
	net.InitShortcut()

	// It("should cascade Close() block free.", func() {
	// 	var done sync.WaitGroup
	// 	shortcut := net.Shortcut.Prepare(TestAddress, getTestID(), 2)
	// 	shortcut.Validate()

	// 	instance := NewInstance("test", 1)
	// 	ctrlLink := NewConnection(shortcut.Conns[0].Server).BindInstance(instance)
	// 	ctrlLink.workerId = 0
	// 	dataLink := NewConnection(shortcut.Conns[1].Server).BindInstance(instance)
	// 	dataLink.workerId = 0

	// 	done.Add(2)
	// 	go func() {
	// 		ctrlLink.ServeLambda()
	// 		done.Done()
	// 	}()
	// 	go func() {
	// 		dataLink.ServeLambda()
	// 		done.Done()
	// 	}()

	// 	instance.lm.SetControl(ctrlLink)
	// 	added := instance.lm.AddDataLink(dataLink)
	// 	Expect(added).To(BeTrue())

	// 	shouldTimeout(func() {
	// 		instance.lm.Close()
	// 	}, false)

	// 	shouldTimeout(func() {
	// 		done.Wait()
	// 	}, false)
	// })

	It("should close connection not timeout.", func() {
		shortcut := net.Shortcut.Prepare(TestAddress, getTestID(), 1)
		shortcut.Validate()

		instance := NewInstance("test", 1)
		dataLink := NewConnection(shortcut.Conns[0].Server).BindInstance(instance)
		dataLink.workerId = 0
		dataLink.control = false

		shouldTimeout(func() {
			dataLink.CloseWithReason("test", true)
		}, false)
	})
})
