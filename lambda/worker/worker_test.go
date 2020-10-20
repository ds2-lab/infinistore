package worker

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	mock "github.com/jordwest/mock-conn"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	TestAddress        = "workertest"
	TestID             = int32(0)
	TestNumConnections = 2
)

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker")
}

func getTestID() int {
	return int(atomic.AddInt32(&TestID, 1))
}

type TestClient struct {
	Conn   net.Conn
	Writer *resp.RequestWriter
	Reader resp.ResponseReader
}

func NewTestClient(cn net.Conn) *TestClient {
	return &TestClient{
		Conn:   cn,
		Writer: resp.NewRequestWriter(cn),
		Reader: resp.NewResponseReader(cn),
	}
}

type TestHeartbeater struct {
	worker *Worker
}

func (hb *TestHeartbeater) SendToLink(link *redeo.Client) error {
	rsp, err := hb.worker.AddResponsesWithPreparer(func(w resp.ResponseWriter) {
		w.AppendBulkString("pong")
	}, link)
	if err != nil {
		return err
	} else {
		return rsp.Flush()
	}
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

func shouldCleanClosed(server *Worker) {
	Expect(server.closed).Should(Equal(WorkerClosed))
	Expect(server.numLinks).Should(Equal(int32(0)))
	shouldTimeout(func() {
		server.readyToClose.Wait()
	}, false)
}

var _ = Describe("Worker", func() {
	protocol.InitShortcut()
	server := NewWorker()
	server.SetHeartbeater(&TestHeartbeater{worker: server})

	It("should return no error on dry running", func() {
		Expect(server.closed).Should(Equal(WorkerClosed))

		// shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID, TestNumConnections)
		start, err := server.StartOrResume("", &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		server.Close()
		shouldCleanClosed(server)
	})

	It("should return error if no proxy address specified", func() {
		_, err := server.StartOrResume("")
		Expect(err).Should(Equal(ErrNoProxySpecified))
		shouldCleanClosed(server)
	})

	It("should worker connect successfully", func() {
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, err := server.StartOrResume(shortcut.Address, &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		for _, conn := range shortcut.Conns {
			client := NewTestClient(conn.Server)
			rsp, err := client.Reader.ReadBulkString()
			Expect(err).Should(BeNil())
			Expect(rsp).Should(Equal("pong"))
		}

		server.Close()
		shouldCleanClosed(server)
	})

	It("no ack should be return on disabled", func() {
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		server.SetManualAck(true)
		start, err := server.StartOrResume(shortcut.Address, &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		ctrlClient := NewTestClient(shortcut.Conns[0].Server)
		shouldTimeout(func() {
			ctrlClient.Reader.ReadBulkString()
		}, true)
		ctrlClient.Conn.Close()
		for _, conn := range shortcut.Conns[1:] {
			client := NewTestClient(conn.Server)
			rsp, err := client.Reader.ReadBulkString()
			Expect(err).Should(BeNil())
			Expect(rsp).Should(Equal("pong"))
		}

		server.SetManualAck(false)
		server.Close()
		shouldCleanClosed(server)
	})

	It("should reflect resuming of worker on calling StartOrResume", func() {
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, _ := server.StartOrResume(shortcut.Address, &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(server.closed).Should(Equal(WorkerRunning))
		// Drain the pongs
		for _, conn := range shortcut.Conns {
			client := NewTestClient(conn.Server)
			client.Reader.ReadBulkString()
		}

		start, err := server.StartOrResume(shortcut.Address, &WorkerOptions{DryRun: true})
		Expect(start).Should(BeFalse())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		server.Close()
		shouldCleanClosed(server)
	})

	It("should worker reconnect automatically", func() {
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, _ := server.StartOrResume(shortcut.Address, &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(server.closed).Should(Equal(WorkerRunning))
		// Drain the pongs
		for _, conn := range shortcut.Conns {
			client := NewTestClient(conn.Server)
			client.Reader.ReadBulkString()
		}

		// Prepare new shortcut connection for redial.
		old := shortcut.Conns[0]
		shortcut.Conns[0] = mock.NewConn()

		// Server should redail now.
		old.Close()

		client2 := NewTestClient(shortcut.Conns[0].Server)
		rsp2, err2 := client2.Reader.ReadBulkString() // skip command
		Expect(err2).Should(BeNil())
		Expect(rsp2).Should(Equal("pong"))

		server.Close()
		shouldCleanClosed(server)
	})
})
