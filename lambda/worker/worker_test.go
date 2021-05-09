package worker

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
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

type TestHeartbeater struct {
	worker *Worker
}

func (hb *TestHeartbeater) SendToLink(link *Link, flags int64) error {
	rsp, err := hb.worker.AddResponsesWithPreparer("pong", func(rsp *SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
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
	server := NewWorker(0)
	server.SetHeartbeater(&TestHeartbeater{worker: server})

	It("should return no error on dry running", func() {
		Expect(server.closed).Should(Equal(WorkerClosed))

		// shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID, TestNumConnections)
		start, err := server.StartOrResume(protocol.StrAddr(""), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		server.Close()
		shouldCleanClosed(server)
	})

	It("should return error if no proxy address specified", func() {
		_, err := server.StartOrResume(protocol.StrAddr(""))
		Expect(err).Should(Equal(ErrNoProxySpecified))
		shouldCleanClosed(server)
	})

	It("should worker connect successfully", func() {
		fmt.Println("should worker connect successfully...")
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, err := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))
		for _, conn := range shortcut.Conns {
			client := NewClient(conn.Server, true)
			rsp, err := client.Reader.ReadBulkString()
			Expect(err).Should(BeNil())
			Expect(rsp).Should(Equal("pong"))
		}

		server.Close()
		shouldCleanClosed(server)
	})

	It("no ack should be return on disabled", func() {
		fmt.Println("no ack should be return on disabled...")
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		server.SetManualAck(true)
		start, err := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		ctrlClient := NewClient(shortcut.Conns[0].Server, true)
		shouldTimeout(func() {
			ctrlClient.Reader.ReadBulkString()
		}, true)
		ctrlClient.Conn.Close()
		for _, conn := range shortcut.Conns[1:] {
			client := NewClient(conn.Server, true)
			rsp, err := client.Reader.ReadBulkString()
			Expect(err).Should(BeNil())
			Expect(rsp).Should(Equal("pong"))
		}

		server.SetManualAck(false)
		server.Close()
		shouldCleanClosed(server)
	})

	It("response should be cached if connection has not been established", func() {
		fmt.Println("response should be cached if connection has not been established...")
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		server.AddResponsesWithPreparer("pong", func(rsp *SimpleResponse, w resp.ResponseWriter) {
			w.AppendBulkString(rsp.Cmd)
		})
		server.SetManualAck(true)
		start, err := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))
		ctrlClient := NewClient(shortcut.Conns[0].Server, true)
		shouldTimeout(func() {
			ctrlClient.Reader.ReadBulkString()
		}, false)
		for _, conn := range shortcut.Conns[1:] {
			client := NewClient(conn.Server, true)
			rsp, err := client.Reader.ReadBulkString()
			Expect(err).Should(BeNil())
			Expect(rsp).Should(Equal("pong"))
		}

		server.SetManualAck(false)
		server.Close()
		shouldCleanClosed(server)
	})

	It("should reflect resuming of worker on calling StartOrResume", func() {
		fmt.Println("should reflect resuming of worker on calling StartOrResume...")
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, _ := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(server.closed).Should(Equal(WorkerRunning))
		// Drain the pongs
		for _, conn := range shortcut.Conns {
			client := NewClient(conn.Server, true)
			client.Reader.ReadBulkString()
		}

		start, err := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeFalse())
		Expect(err).Should(BeNil())
		Expect(server.closed).Should(Equal(WorkerRunning))

		server.Close()
		shouldCleanClosed(server)
	})

	It("should worker reconnect automatically", func() {
		fmt.Println("should worker reconnect automatically...")
		shortcut := protocol.Shortcut.Prepare(TestAddress, getTestID(), TestNumConnections)

		start, _ := server.StartOrResume(protocol.StrAddr(shortcut.Address), &WorkerOptions{DryRun: true})
		Expect(start).Should(BeTrue())
		Expect(server.closed).Should(Equal(WorkerRunning))
		// Drain the pongs
		for _, conn := range shortcut.Conns {
			client := NewClient(conn.Server, true)
			client.Reader.ReadBulkString()
		}

		// Prepare new shortcut connection for redial.
		old := shortcut.Conns[0]
		shortcut.Conns[0] = protocol.NewMockConn(shortcut.Address, 0)

		// Server should redail now.
		old.Close()

		client2 := NewClient(shortcut.Conns[0].Server, true)
		rsp2, err2 := client2.Reader.ReadBulkString() // skip command
		Expect(err2).Should(BeNil())
		Expect(rsp2).Should(Equal("pong"))

		server.Close()
		shouldCleanClosed(server)
	})
})
