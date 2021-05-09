package handlers

import (
	// "errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

type TestPongHandler struct {
	*PongHandler
}

func newTestPongHandler(override pong) *TestPongHandler {
	handler := &TestPongHandler{
		PongHandler: NewPongHandler(),
	}
	handler.pong = override
	return handler
}

func succeedPong(link *worker.Link, flags int64) error {
	return nil
}

func getTimeoutPong(handler *TestPongHandler, failure int) pong {
	attempt := 0
	return func(link *worker.Link, flags int64) error {
		failure := attempt < failure
		attempt++
		if !failure {
			go handler.Cancel()
		}
		return nil
	}
}

var _ = Describe("PongHandler", func() {

	It("should pong implementation be overwriten in test handler", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = succeedPong
		pong.Issue(false)
		Ω(pong.Send()).Should(Succeed())
	})

	It("should pong successed if failure <= 3", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, 3)
		pong.Issue(true)
		Ω(pong.Send()).Should(Succeed())

		// Deny more pongs
		Expect(pong.Issue(false)).To(Equal(false))

		// Wait for retry
		timeout := time.NewTimer(1 * time.Second)
		<-timeout.C
		Expect(pong.cancelled).To(Equal(true))

		// Should be ok to send more
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(Equal(true))
		pong.Send() // Drain pongs
	})

	It("should pong failed if failure > 3", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, 4)
		pong.Issue(true)
		Ω(pong.Send()).Should(Succeed())

		// Deny more pongs
		Expect(pong.Issue(true)).To(Equal(false))

		timeout := time.NewTimer(1 * time.Second)
		<-timeout.C
		Expect(pong.cancelled).To(Equal(false))

		// Should be ok to send more
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(Equal(true))
		pong.Send() // Drain pongs
	})

	It("should not stuck for concurrent request", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, 4)

		var allDone sync.WaitGroup
		for i := 0; i < 10; i++ {
			allDone.Add(1)
			go func() {
				pong.Issue(true)
				pong.Send()
				allDone.Done()
			}()
		}

		allDone.Wait()
		Expect(true).To(Equal(true))
		// Ω(pong.SendTo(nil)).ShouldNot(HaveOccurred())
	})
})
