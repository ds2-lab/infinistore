package worker

import (
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	ErrWorkerClosed     = errors.New("worker closed")
	ErrNoProxySpecified = errors.New("no proxy specified")
	// MaxControlRequestSize = int64(200000) // 200KB, which can be transmitted in 20ms.

	MaxAttempts           = 3
	RetrialDelayStartFrom = 20 * time.Millisecond
	RetrialBackoffFactor  = 2
)

type Worker struct {
	*redeo.Server
	id           int32
	ctrlLink     *Link
	dataLink     *Link
	heartbeater  Heartbeater
	log          logger.ILogger
	mu           sync.RWMutex
	closed       int32
	numLinks     int32
	manualAck    int32 // Normally, worker will acknowledge links by calling heartbeater automatically. ManualAck will override default behavior for ctrlLink.
	readyToClose sync.WaitGroup
	dryrun       bool
}

type WorkerOptions struct {
	DryRun bool
}

func NewWorker(lifeId int64) *Worker {
	rand.Seed(lifeId)
	worker := &Worker{
		id:          rand.Int31(),
		Server:      redeo.NewServer(nil),
		log:         &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Worker:"},
		ctrlLink:    NewLink(true),
		dataLink:    NewLink(false),
		heartbeater: new(DefaultHeartbeater),
		closed:      WorkerClosed,
	}
	worker.Server.HandleCallbackFunc(worker.responseHandler)
	return worker
}

func (wrk *Worker) Id() int32 {
	return wrk.id
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

	wrk.ensureConnection(wrk.ctrlLink, proxyAddr, opts)
	wrk.ensureConnection(wrk.dataLink, proxyAddr, opts)
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

	wrk.ctrlLink.Close()
	wrk.dataLink.Close()
	atomic.StoreInt32(&wrk.numLinks, 0)

	wrk.readyToClose.Wait()
	wrk.log.Info("Closed")
}

func (wrk *Worker) IsClosed() bool {
	wrk.mu.RLock()
	defer wrk.mu.RUnlock()

	return wrk.isClosedLocked()
}

func (wrk *Worker) isClosedLocked() bool {
	return atomic.LoadInt32(&wrk.closed) == WorkerClosed
}

// Add asynchronize response, error if the client is closed.
// The function will use the link specified by the second parameter, and use datalink automatically
// if payload is large enough.
// In case an error is returned on closing, the caller can safely ignore the error and call rsp.Flush() afterward
// without side affect.
func (wrk *Worker) AddResponses(rsp Response, links ...interface{}) (err error) {
	if wrk.dryrun {
		return nil
	}

	var link *Link
	wrk.mu.RLock()

	if wrk.isClosedLocked() {
		wrk.mu.RUnlock()
		return ErrWorkerClosed
	}

	if len(links) > 0 {
		switch dl := links[0].(type) {
		case *redeo.Client:
			link = LinkFromClient(dl)
		case bool:
			if dl {
				link = wrk.dataLink
			}
		}
	}

	// Default to use ctrlLink
	if link == nil {
		link = wrk.ctrlLink
	}

	// Stick to original link, proxy may decide which link to use.
	// // Only upgrade to datalink for responses aimed for ctrlLink.
	// // For others like using datalink or migration link, do nothing.
	// if datalink == wrk.ctrlLink && rsp.Size() > MaxControlRequestSize {
	// 	datalink = wrk.dataLink
	// }

	wrk.mu.RUnlock()

	// Link will only be binded once. We use binded link to add the response.
	rsp.bind(link)
	return rsp.getLink().AddResponses(rsp)
}

func (wrk *Worker) AddResponsesWithPreparer(cmd string, preparer Preparer, links ...interface{}) (Response, error) {
	resp := &SimpleResponse{BaseResponse{Cmd: cmd, preparer: preparer}}
	return resp, wrk.AddResponses(resp, links...)
}

func (wrk *Worker) SetManualAck(enable bool) {
	if enable {
		atomic.StoreInt32(&wrk.manualAck, int32(1))
	} else {
		atomic.StoreInt32(&wrk.manualAck, int32(0))
	}
}

func (wrk *Worker) ensureConnection(link *Link, proxyAddr string, opts *WorkerOptions) error {
	if !link.Initialize() {
		// Initilized.
		return nil
	}

	id := atomic.AddInt32(&wrk.numLinks, 1)
	wrk.readyToClose.Add(1)
	go wrk.serve(int(id), link, proxyAddr, opts)
	return nil
}

func (wrk *Worker) serve(id int, link *Link, proxyAddr string, opts *WorkerOptions) {
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
		link.Reset(conn)
		wrk.mu.Unlock()

		// Send a heartbeat on the link immediately to confirm store information.
		// The heartbeat will be queued and send once worker started.
		// On error, the connection will be closed .
		if !link.IsControl() || atomic.LoadInt32(&wrk.manualAck) == 0 {
			go func(client *redeo.Client) {
				if err := wrk.heartbeater.SendToLink(client); err != nil {
					wrk.log.Warn("%v", err)
					client.Close()
				}
			}(link.Client)
		}

		// Serve the client.
		err := wrk.Server.ServeClient(link.Client, false) // Enable asych mode to allow sending request.
		conn.Close()
		// Reset link and buffer possible incoming response
		link.Reset(nil)

		// Check if worker is closed.
		// Must Check first to avoid deadlock on calling Close()
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

// HandleCallback callback handler
func (wrk *Worker) responseHandler(w resp.ResponseWriter, r interface{}) {
	rsp := r.(Response)

	err := rsp.flush(w)
	if err != nil {
		left := rsp.markAttempt()
		retryIn := RetrialDelayStartFrom * time.Duration(math.Pow(float64(RetrialBackoffFactor), float64(MaxAttempts-left)))
		if left > 0 {
			wrk.log.Warn("Error on flush response(%v), retry in %v: %v", rsp, retryIn, err)
			go func() {
				time.Sleep(retryIn)
				wrk.AddResponses(rsp)
			}()
			return
		} else {
			wrk.log.Warn("Error on flush response(%v), abandon attempts: %v", rsp, err)
		}
	}

	rsp.close()
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
