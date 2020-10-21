package worker

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

const (
	WorkerRunning = int32(0)
	WorkerClosing = int32(1)
	WorkerClosed  = int32(2)
)

var (
	defaultOption WorkerOptions
	ctxKeyConn    = struct{}{}

	ErrNoProxySpecified   = errors.New("no proxy specified")
	MaxControlRequestSize = int64(100000) // 100KB, which can be transmitted in 10ms.
)

type Worker struct {
	*redeo.Server
	ctrlLink     *redeo.Client
	dataLink     *redeo.Client
	heartbeater  Heartbeater
	log          logger.ILogger
	mu           sync.Mutex
	closed       int32
	numLinks     int32
	manualAck    int32 // Normally, worker will acknowledge links by calling heartbeater automatically. ManualAck will override default behavior for ctrlLink.
	readyToClose sync.WaitGroup
	dryrun       bool
}

type WorkerOptions struct {
	DryRun bool
}

func NewWorker() *Worker {
	worker := &Worker{
		Server:      redeo.NewServer(nil),
		log:         &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Worker:"},
		ctrlLink:    new(redeo.Client),
		dataLink:    new(redeo.Client),
		heartbeater: new(DefaultHeartbeater),
		closed:      WorkerClosed,
	}
	worker.Server.HandleCallbackFunc(worker.responseHandler)
	return worker
}

func (wrk *Worker) SetHeartbeater(heartbeater Heartbeater) {
	wrk.heartbeater = heartbeater
}

func (wrk *Worker) StartOrResume(proxyAddr string, args ...*WorkerOptions) (isStart bool, err error) {
	opts := &defaultOption
	if len(args) > 0 {
		opts = args[0]
	}

	if len(proxyAddr) == 0 {
		if opts.DryRun {
			isStart = atomic.CompareAndSwapInt32(&wrk.closed, WorkerClosed, WorkerRunning)
			wrk.dryrun = true
		} else {
			err = ErrNoProxySpecified
		}
		return
	}

	isStart = atomic.CompareAndSwapInt32(&wrk.closed, WorkerClosed, WorkerRunning)
	wrk.dryrun = false

	if err = wrk.ensureConnection(wrk.ctrlLink, true, proxyAddr, opts); err != nil {
		return
	}
	if err = wrk.ensureConnection(wrk.dataLink, false, proxyAddr, opts); err != nil {
		return
	}
	return
}

func (wrk *Worker) Close(opts ...bool) {
	graceful := false
	if len(opts) > 0 {
		graceful = opts[0]
	}

	wrk.mu.Lock()
	defer wrk.mu.Unlock()

	if graceful && atomic.CompareAndSwapInt32(&wrk.closed, WorkerRunning, WorkerClosing) {
		// Graceful close is requested, wait for close.
		wrk.readyToClose.Wait()
		atomic.StoreInt32(&wrk.closed, WorkerClosed)
	} else if !atomic.CompareAndSwapInt32(&wrk.closed, WorkerRunning, WorkerClosed) {
		// Closed
		return
	}

	wrk.ctrlLink = wrk.resetLinkLocked(wrk.ctrlLink)
	wrk.dataLink = wrk.resetLinkLocked(wrk.dataLink)
	atomic.StoreInt32(&wrk.numLinks, 0)

	wrk.readyToClose.Wait()
}

// Add asynchronize response, error if the client is closed.
// The function will use the link specified by the second parameter, and use datalink automatically
// if payload is large enough.
// In case an error is returned on closing, the caller can safely ignore the error and call rsp.Flush() afterward
// without side affect.
func (wrk *Worker) AddResponses(rsp Response, datalinks ...interface{}) (err error) {
	if wrk.dryrun {
		return nil
	}

	var datalink *redeo.Client
	if len(datalinks) > 0 {
		switch dl := datalinks[0].(type) {
		case *redeo.Client:
			datalink = dl
		case bool:
			if dl {
				datalink = wrk.dataLink
			}
		}
	}

	// Default to use ctrlLink
	if datalink == nil {
		datalink = wrk.ctrlLink
	}

	// Stick to original link, proxy may decide which link to use.
	// // Only upgrade to datalink for responses aimed for ctrlLink.
	// // For others like using datalink or migration link, do nothing.
	// if datalink == wrk.ctrlLink && rsp.Size() > MaxControlRequestSize {
	// 	datalink = wrk.dataLink
	// }

	rsp.reset(rsp, datalink)
	if err = datalink.AddResponses(rsp); err != nil {
		rsp.close()
	}
	return err
}

func (wrk *Worker) AddResponsesWithPreparer(preparer Preparer, datalinks ...interface{}) (Response, error) {
	resp := &BaseResponse{preparer: preparer}
	return resp, wrk.AddResponses(resp, datalinks...)
}

func (wrk *Worker) SetManualAck(enable bool) {
	if enable {
		atomic.StoreInt32(&wrk.manualAck, int32(1))
	} else {
		atomic.StoreInt32(&wrk.manualAck, int32(0))
	}
}

func GetConnectionByLink(link *redeo.Client) net.Conn {
	if link == nil {
		return nil
	}

	if conn, ok := link.Context().Value(ctxKeyConn).(net.Conn); ok {
		return conn
	}

	return nil
}

func (wrk *Worker) ensureConnection(link *redeo.Client, isCtrl bool, proxyAddr string, opts *WorkerOptions) error {
	initialized := false
	wrk.mu.Lock()
	initialized = link.ID() != uint64(0)
	wrk.mu.Unlock()
	if initialized {
		return nil
	}

	id := atomic.AddInt32(&wrk.numLinks, 1)
	wrk.readyToClose.Add(1)
	go wrk.serve(int(id), link, isCtrl, proxyAddr, opts)
	return nil
}

func (wrk *Worker) serve(id int, link *redeo.Client, isCtrl bool, proxyAddr string, opts *WorkerOptions) {
	for {
		// Connect to proxy.
		var conn net.Conn
		wrk.log.Debug("Ready to connect %s", proxyAddr)
		if opts.DryRun {
			shortcuts, ok := protocol.Shortcut.Dial(proxyAddr)
			if !ok {
				wrk.log.Error("Oops, no shortcut connection available for dry running.")
				return
			}
			conn = shortcuts[id-1].Client
		} else {
			cn, err := net.Dial("tcp", proxyAddr)
			if err != nil {
				wrk.log.Error("Failed to connect proxy %s: %v", proxyAddr, err)
				return
			}
			conn = cn
		}

		wrk.log.Info("Connection(%v) to %v established.", id, conn.RemoteAddr())

		wrk.mu.Lock()
		// Recheck if server closed in mutex
		if atomic.LoadInt32(&wrk.closed) == WorkerClosed {
			conn.Close()
			wrk.mu.Unlock()
			return
		}
		*link = *redeo.NewClient(conn)
		link.SetContext(context.WithValue(link.Context(), ctxKeyConn, conn))
		wrk.mu.Unlock()

		// Send a heartbeat on the link immediately to confirm store information.
		// The heartbeat will be queued and send once worker started.
		// On error, the connection will be closed .
		if !isCtrl || atomic.LoadInt32(&wrk.manualAck) == 0 {
			go func(link *redeo.Client) {
				if err := wrk.heartbeater.SendToLink(link); err != nil {
					wrk.log.Warn("%v", err)
					link.Close()
				}
			}(link)
		}

		// Serve the client.
		err := wrk.Server.ServeClient(link, false) // Enable asych mode to allow sending request.
		conn.Close()
		// Check if worker is closed.
		switch atomic.LoadInt32(&wrk.closed) {
		case WorkerClosed:
			fallthrough
		case WorkerClosing:
			wrk.readyToClose.Done()
			wrk.log.Info("Connection(%v) closed.", id)
			return
		}

		if err != nil && err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			wrk.log.Warn("Connection(%v) closed: %v, reconnecting...", id, err)
		} else {
			// Closed by the proxy, stop worker.
			wrk.log.Info("Connection(%v) closed from proxy. Closing worker...", id)
			wrk.readyToClose.Done()
			wrk.Close()
			return
		}
	}
}

func (wrk *Worker) resetLinkLocked(link *redeo.Client) *redeo.Client {
	link.Close()
	if conn := GetConnectionByLink(link); conn != nil {
		conn.Close() // force disconnect
	}
	return new(redeo.Client)
}

// HandleCallback callback handler
func (wrk *Worker) responseHandler(w resp.ResponseWriter, r interface{}) {
	rsp := r.(Response)
	defer rsp.close()

	rsp.flush(w)
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
