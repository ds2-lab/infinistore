package handlers

import (
	// "errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/lambda/lifetime"
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
		fail := attempt < failure
		attempt++
		if !fail {
			go handler.Cancel()
		}
		return nil
	}
}

var _ = Describe("PongHandler", func() {
	log = &logger.ColorLogger{Level: logger.LOG_LEVEL_ALL, Color: true, Prefix: "Pong:"}
	lifetime.GetOrCreateSession()

	It("should pong implementation be overwriten in test handler", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(BeTrue())
		Ω(pong.Send()).Should(Succeed())
		pong.Cancel()
	})

	It("should pong deny duplicated request", func() {
		pong := newTestPongHandler(succeedPong)
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(BeTrue())

		// Deny more pongs
		Expect(pong.Issue(false)).To(BeFalse())

		Ω(pong.Send()).Should(Succeed())
		pong.Cancel()
	})

	It("should pong successed if failure < 3", func() {
		old := DefaultAttempts
		DefaultAttempts = 3
		defer func() { DefaultAttempts = old }()

		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, DefaultAttempts-1)
		pong.Issue(true)
		Ω(pong.Send()).Should(Succeed())

		// Deny more pongs before cancel
		Expect(pong.Issue(false)).To(Equal(false))

		// Wait for retry
		<-time.After(time.Duration(DefaultAttempts) * DefaultPongTimeout)

		Expect(pong.IsCancelled()).To(Equal(true))

		// Should be ok to send more
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(Equal(true))
		pong.Send() // Drain pongs
		pong.Cancel()
	})

	It("should pong failed if failure >= 3", func() {
		old, to := DefaultAttempts, NoTimeout
		DefaultAttempts = 3
		NoTimeout = true
		defer func() { DefaultAttempts, NoTimeout = old, to }()

		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, DefaultAttempts)
		pong.Issue(true)
		Ω(pong.Send()).Should(Succeed())

		// Deny more pongs
		Expect(pong.Issue(true)).To(Equal(false))

		<-time.After(time.Duration(DefaultAttempts) * DefaultPongTimeout)
		Expect(pong.IsCancelled()).To(Equal(false))

		// Should be ok to send more
		pong.pong = succeedPong
		Expect(pong.Issue(false)).To(Equal(true))
		pong.Send() // Drain pongs
		pong.Cancel()
	})

	It("should pong timeout", func() {
		to := NoTimeout
		NoTimeout = false
		defer func() { NoTimeout = to }()

		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, 1)
		pong.fail = func(_ *worker.Link, err error) {
			defer GinkgoRecover()

			errPong, ok := err.(*PongError)
			Expect(ok).To(BeTrue())
			Expect(errPong.error).To(Equal(errPongTimeout))
		}
		pong.Issue(false)
		Ω(pong.Send()).Should(Succeed())

		// Allow more pongs
		Expect(pong.Issue(false)).To(Equal(true))

		<-time.After(DefaultPongTimeout)
		Expect(pong.IsCancelled()).To(Equal(false))

		// Should be ok to send more
		pong.pong = succeedPong
		pong.Issue(false)
		pong.Send() // Drain pongs
		pong.Cancel()
	})

	It("should one cancel enough for all previous pongs", func() {
		to := NoTimeout
		NoTimeout = false
		defer func() { NoTimeout = to }()

		pong := newTestPongHandler(succeedPong)
		pong.pong = getTimeoutPong(pong, 1)
		pong.fail = func(_ *worker.Link, err error) {
			defer GinkgoRecover()

			Fail("should not fail")
		}
		Expect(pong.Issue(false)).To(Equal(true))
		Ω(pong.Send()).Should(Succeed())

		// Another pong
		Expect(pong.Issue(false)).To(Equal(true))
		Ω(pong.Send()).Should(Succeed())

		<-time.After(DefaultPongTimeout * 2)
		Expect(pong.IsCancelled()).To(Equal(true))
	})

	// It("should not stuck for concurrent request", func() {
	// 	pong := newTestPongHandler(succeedPong)
	// 	pong.pong = getTimeoutPong(pong, 4)

	// 	var allDone sync.WaitGroup
	// 	for i := 0; i < 10; i++ {
	// 		allDone.Add(1)
	// 		go func() {
	// 			pong.Issue(true)
	// 			pong.Send()
	// 			pong.Cancel()
	// 			allDone.Done()
	// 		}()
	// 	}

	// 	allDone.Wait()
	// 	Expect(true).To(Equal(true))
	// 	// Ω(pong.SendTo(nil)).ShouldNot(HaveOccurred())
	// })
})
