package worker

import (
	"container/list"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"runtime"
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
	WorkerRunning        = int32(0)
	WorkerClosing        = int32(1)
	WorkerClosed         = int32(2)
	RetrialBackoffFactor = 2
	MinDataLinks         = 1
	MaxDataLinks         = 10
)

var (
	defaultOption WorkerOptions

	ErrWorkerClosed     = errors.New("worker closed")
	ErrNoProxySpecified = errors.New("no proxy specified")
	ErrInvalidShortcut  = errors.New("invalid shortcut connection")
	// MaxControlRequestSize = int64(200000) // 200KB, which can be transmitted in 20ms.

	RetrialDelayStartFrom = 20 * time.Millisecond
	RetrialMaxDelay       = 10 * time.Second
)

// Worker Lambda serve worker. A worker uses two types of links: control and data.
// Control link: Stable connection to serve control commands and small requests.
// Data link: Short lived (one time mostly) connection serve all requests, data link is established on demand via a dynamic connection system
// Dynamic connection system: Use token to control minimum active connections. on connecting, each connection consumes a token,
// 														and the token is returned on first request or disconnection, among which is first.
type Worker struct {
	*redeo.Server
	id           int32
	ctrlLink     *Link
	heartbeater  Heartbeater
	log          logger.ILogger
	mu           sync.RWMutex
	closed       int32
	numLinks     int32
	manualAck    int32 // Normally, worker will acknowledge links by calling heartbeater automatically. ManualAck will override default behavior for ctrlLink.
	readyToClose sync.WaitGroup
	dryrun       bool

	// Dynamic connection
	availableTokens chan *struct{}
	balance         int32
	startBalance    int32
	dataLinks       *list.List

	// Proxies container
	proxies []*HandlerProxy
}

type WorkerOptions struct {
	DryRun       bool
	MinDataLinks int
}

func NewWorker(lifeId int64) *Worker {
	rand.Seed(lifeId)
	worker := &Worker{
		id:       rand.Int31(),
		Server:   redeo.NewServer(nil),
		log:      &logger.ColorLogger{Level: logger.LOG_LEVEL_ALL, Color: false, Prefix: "Worker:"},
		ctrlLink: NewLink(true),
		// dataLink:    NewLink(false),
		heartbeater: new(DefaultHeartbeater),
		dataLinks:   list.New(),
		closed:      WorkerClosed,
		proxies:     make([]*HandlerProxy, 0, 10), // 10 for a initial size.
	}
	worker.Server.HandleFunc(protocol.CMD_ACK, worker.ackHandler)
	worker.Server.HandleCallbackFunc(worker.responseHandler)
	return worker
}

func (wrk *Worker) Id() int32 {
	return wrk.id
}

func (wrk *Worker) SetHeartbeater(heartbeater Heartbeater) {
	wrk.heartbeater = heartbeater
}

func (wrk *Worker) StartOrResume(proxyAddr net.Addr, args ...*WorkerOptions) (isStart bool, err error) {
	opts := &defaultOption
	if len(args) > 0 && args[0] != nil {
		opts = args[0]
	}
	if opts.MinDataLinks == 0 {
		opts.MinDataLinks = MinDataLinks
	}
	if opts.MinDataLinks < 0 {
		opts.MinDataLinks = 1
	}
	if opts.MinDataLinks > MaxDataLinks {
		opts.MinDataLinks = MaxDataLinks
	}

	if proxyAddr == nil {
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

	var started sync.WaitGroup
	wrk.ensureConnection(wrk.ctrlLink, proxyAddr, opts, &started)
	go wrk.reserveConnection(wrk.dataLinks, proxyAddr, opts, &started)
	started.Wait()
	return
}

func (wrk *Worker) Pause() {
	wrk.mu.Lock()
	defer wrk.mu.Unlock()

	wrk.clearDataLinksLocked()
}

func (wrk *Worker) Close() {
	wrk.CloseWithOptions(false)
}

func (wrk *Worker) CloseWithOptions(opts ...bool) {
	graceful := false
	if len(opts) > 0 {
		graceful = opts[0]
	}

	if !atomic.CompareAndSwapInt32(&wrk.closed, WorkerRunning, WorkerClosing) {
		return
	}
	if graceful {
		// Graceful close is requested, wait for close.
		wrk.readyToClose.Wait()
	}

	wrk.mu.Lock()

	atomic.StoreInt32(&wrk.closed, WorkerClosed)
	wrk.ctrlLink.Close()
	wrk.clearDataLinksLocked()
	atomic.StoreInt32(&wrk.numLinks, 0)

	wrk.mu.Unlock()

	wrk.readyToClose.Wait()
	wrk.log.Info("Closed")
}

func (wrk *Worker) IsClosed() bool {
	if wrk.isClosedLocked() {
		return true
	}

	wrk.mu.RLock()
	defer wrk.mu.RUnlock()

	return wrk.isClosedLocked()
}

func (wrk *Worker) isClosedLocked() bool {
	return atomic.LoadInt32(&wrk.closed) != WorkerRunning
}

func (wrk *Worker) Handler(fn redeo.HandlerFunc) redeo.HandlerFunc {
	handler := &HandlerProxy{worker: wrk, handle: fn}
	wrk.proxies = append(wrk.proxies, handler)
	return handler.HandlerFunc
}

func (wrk *Worker) StreamHandler(fn redeo.StreamHandlerFunc) redeo.StreamHandlerFunc {
	handler := &HandlerProxy{worker: wrk, streamHandle: fn}
	wrk.proxies = append(wrk.proxies, handler)
	return handler.StreamHandlerFunc
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

	// It's no harm to call locked version
	if wrk.isClosedLocked() {
		rsp.abandon(ErrWorkerClosed)
		return ErrWorkerClosed
	}

	// Select link to use by parameter.
	link := wrk.selectLink(links...)

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

func (wrk *Worker) SetFailure(link interface{}, err error) {
	wrk.selectLink(link).Invalidate(err)
}

func (wrk *Worker) ensureConnection(link *Link, proxyAddr net.Addr, opts *WorkerOptions, started *sync.WaitGroup) error {
	if !link.Initialize() {
		// Initilized.
		return nil
	}

	link.id = int(atomic.AddInt32(&wrk.numLinks, 1))
	started.Add(1)
	wrk.readyToClose.Add(1)
	go wrk.serve(link, proxyAddr, opts, started)
	return nil
}

func (wrk *Worker) serve(link *Link, proxyAddr net.Addr, opts *WorkerOptions, started *sync.WaitGroup) {
	var once sync.Once
	defer once.Do(started.Done)

	delay := RetrialDelayStartFrom
	link.addr = proxyAddr.String() // To be compatibile with shortcut QueueAddr, keep a copy of string address.
	hbFlags := protocol.PONG_FOR_CTRL
	if !link.IsControl() {
		hbFlags = protocol.PONG_FOR_DATA
	}
	for {
		// Connect to proxy.
		var conn net.Conn
		var remoteAddr string
		wrk.log.Debug("Ready to connect %v", link.addr)
		if opts.DryRun {
			shortcuts, ok := protocol.Shortcut.Dial(link.addr)
			if !ok {
				wrk.log.Error("Oops, no shortcut connection available for dry running, retry after %v", delay)
				delay = wrk.waitDelay(delay)
				continue
			}
			conn = shortcuts[0].Client
			remoteAddr = shortcuts[0].String()
		} else {
			cn, err := net.Dial("tcp", link.addr)
			if err != nil {
				wrk.log.Error("Failed to connect proxy %s, retry after %v: %v", proxyAddr, delay, err)
				delay = wrk.waitDelay(delay)
				continue
			}
			conn = cn
			remoteAddr = cn.RemoteAddr().String()
		}
		delay = RetrialDelayStartFrom

		wrk.log.Info("Connection(%v) to %v established.", link.ID(), remoteAddr)

		// Recheck if server closed in mutex
		if wrk.IsClosed() {
			conn.Close()
			wrk.readyToClose.Done()
			return
		}
		link.Reset(conn)

		// Send a heartbeat on the link immediately to confirm store information.
		// The heartbeat will be queued and send once worker started.
		// On error, the connection will be closed .
		if !link.IsControl() || atomic.LoadInt32(&wrk.manualAck) == 0 {
			go func(link *Link) {
				wrk.log.Debug("Invoke heartbeater(%v)", link.ID())
				if err := wrk.heartbeater.SendToLink(link, hbFlags); err != nil {
					wrk.log.Warn("Heartbeat(%v) err: %v", link.ID(), err)
					link.Client.Close()
				}
			}(link)
		}
		once.Do(started.Done)

		// Serve the client.
		err := wrk.Server.ServeClient(link.Client, false) // Enable asych mode to allow sending request.
		if link.lastError != nil {
			// override err if by purpose.
			err = link.lastError
			if hbErr, ok := err.(HeartbeatError); ok {
				hbFlags = hbErr.Flags()
			}
		}
		conn.Close()
		// Reset link and buffer possible incoming response
		link.Reset(nil)

		// Check if worker is closed.
		switch atomic.LoadInt32(&wrk.closed) {
		case WorkerClosed:
			fallthrough
		case WorkerClosing:
			wrk.log.Info("Connection(%v) closed.", link.ID())
			wrk.readyToClose.Done()
			return
		}

		if err != nil && err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			wrk.log.Warn("Connection(%v) closed: %v, reconnecting...", link.ID(), err)
		} else {
			// Closed by the proxy, stop worker.
			wrk.log.Info("Connection(%v) closed from proxy. Closing worker...", link.ID())
			wrk.readyToClose.Done() // Close() will wait for readyToClose
			wrk.Close()
			return
		}
	}
}

func (wrk *Worker) reserveConnection(links *list.List, proxyAddr net.Addr, opts *WorkerOptions, started *sync.WaitGroup) {
	wrk.availableTokens = make(chan *struct{}, MinDataLinks)
	// Fill tokens
	for i := 0; i < opts.MinDataLinks; i++ {
		wrk.availableTokens <- &struct{}{}
	}
	wrk.balance = int32(len(wrk.availableTokens))
	wrk.startBalance = wrk.balance
	ch := wrk.availableTokens
	for token := range ch {
		if link := wrk.reserveDataLink(nil, token); link == nil {
			continue
		} else if err := wrk.serveOnce(link, proxyAddr, opts); err != nil {
			// Failed to connect, exit.
			break
		} else {
			wrk.addDataLink(link)
		}
	}
}

func (wrk *Worker) serveOnce(link *Link, proxyAddr net.Addr, opts *WorkerOptions) error {
	// Connect to proxy.
	var conn net.Conn
	var remoteAddr string
	if opts.DryRun {
		proxyAddr.(*protocol.QueueAddr).Pop()
		wrk.log.Debug("Ready to connect %v", proxyAddr)
		link.addr = proxyAddr.String()
		shortcuts, ok := protocol.Shortcut.Dial(proxyAddr.String())
		if !ok {
			wrk.log.Error("Oops, no shortcut connection available for dry running")
			return ErrInvalidShortcut
		}
		conn = shortcuts[0].Client
		remoteAddr = shortcuts[0].String()
	} else {
		wrk.log.Debug("Ready to connect %v", proxyAddr)
		link.addr = proxyAddr.String()
		cn, err := net.Dial("tcp", proxyAddr.String())
		if err != nil {
			wrk.log.Error("Failed to connect proxy %s: %v", proxyAddr, err)
			return err
		}
		conn = cn
		remoteAddr = cn.RemoteAddr().String()
	}
	wrk.log.Info("Connection(%v) to %v established.", link.ID(), remoteAddr)

	// Recheck if server closed in mutex
	if wrk.IsClosed() {
		conn.Close()
		return ErrWorkerClosed
	}
	link.Reset(conn)

	// Send a heartbeat on the link immediately to confirm store information.
	// The heartbeat will be queued and send once worker started.
	// On error, the connection will be closed .
	wrk.log.Debug("Invoke heartbeater(%v)", link.ID())
	if err := wrk.heartbeater.SendToLink(link, protocol.PONG_FOR_DATA); err != nil {
		wrk.log.Warn("Heartbeat(%v) err: %v", link.ID(), err)
		link.Close()
		return err
	}

	// Serve the client.
	go func(link *Link) {
		_ = wrk.Server.ServeClient(link.Client, false) // Enable asych mode to allow sending request.
		wrk.flagReservationUsed(link)
		wrk.removeDataLink(link)
		link.Close()
	}(link)

	return nil
}

func (wrk *Worker) reserveDataLink(link *Link, token *struct{}) *Link {
	balance := atomic.AddInt32(&wrk.balance, -1)
	defer wrk.log.Debug("Link available, spare links: %d", wrk.startBalance-balance)

	// We will not create new link if there are spare links
	if balance < 0 && link == nil {
		return link
	}

	if link == nil {
		link = NewLink(false)
		link.id = int(atomic.AddInt32(&wrk.numLinks, 1))
	}
	if token == nil {
		token = &struct{}{}
	}
	link.GrantToken(token)
	return link
}

func (wrk *Worker) flagReservationUsed(link *Link) bool {
	token := link.RevokeToken()
	if token == nil {
		return false
	}

	availableTokens := wrk.availableTokens
	if availableTokens == nil {
		return false
	}

	if balance := atomic.AddInt32(&wrk.balance, 1); balance > 0 {
		select {
		case wrk.availableTokens <- token:
			wrk.log.Debug("Token recycled, spare links: %d", wrk.startBalance-balance)
		default:
			wrk.log.Warn("Token overflowed(balance: %d), reset balance.", balance)
			// TODO: This is a dangrous reset. However, program should not reach here, we'll debug this once we see the warning.
			atomic.StoreInt32(&wrk.balance, int32(len(wrk.availableTokens)))
		}
	} else {
		wrk.log.Debug("Link consumed, spare links: %d", wrk.startBalance-balance)
	}
	runtime.Gosched() // Encourage create another connection quickly.
	return true
}

func (wrk *Worker) acknowledge(link *Link) {
	link.acked.Resolve()
}

func (wrk *Worker) ackHandler(w resp.ResponseWriter, c *resp.Command) {
	link := LinkFromClient(redeo.GetClient(c.Context()))
	wrk.acknowledge(link)
	if !link.IsControl() {
		wrk.reserveDataLink(link, nil)
	}
}

func (wrk *Worker) WaitAck(cmd string, cb func(), links ...interface{}) {
	link := wrk.selectLink(links...)
	go func() {
		// Wait for resolve or timeout
		if err := link.acked.Timeout(); err != nil {
			wrk.log.Warn("Acknowledge of %v: %v", cmd, err)
			link.acked.Resolve()
		}
		cb()
	}()
}

// HandleCallback callback handler
func (wrk *Worker) responseHandler(w resp.ResponseWriter, r interface{}) {
	rsp := r.(Response)
	link := LinkFromClient(redeo.GetClient(rsp.Context()))

	if wrk.IsClosed() {
		wrk.log.Warn("Abort flushing response(%v): %v", rsp, ErrWorkerClosed)
		rsp.abandon(ErrWorkerClosed)
		return
	}

	err := rsp.flush(w)
	if err != nil {
		wrk.SetFailure(link, err)

		if wrk.IsClosed() {
			wrk.log.Warn("Error on flush response(%v), abandon attempts because the worker is closed: %v", rsp, ErrWorkerClosed)
			rsp.close()
			return
		} else if link.IsClosed() {
			wrk.log.Warn("Error on flush response(%v), abandon attempts because the link is closed: %v", rsp, ErrLinkClosed)
			rsp.close()
			return
		}

		left := rsp.markAttempt()
		retryIn := RetrialDelayStartFrom * time.Duration(math.Pow(float64(RetrialBackoffFactor), float64(rsp.maxAttempts()-left-1)))
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

		rsp.close()
		return
	}

	rsp.close()
}

func (wrk *Worker) waitDelay(delay time.Duration) time.Duration {
	<-time.After(delay)
	after := delay * RetrialBackoffFactor
	if after > RetrialMaxDelay {
		after = RetrialMaxDelay
	}
	return after
}

func (wrk *Worker) selectLink(links ...interface{}) (link *Link) {
	// Select link to use by parameter.
	if len(links) > 0 {
		switch dl := links[0].(type) {
		case *Link:
			link = dl
		case *redeo.Client:
			link = LinkFromClient(dl)
		}
	}
	// Default to use ctrlLink.
	if link == nil {
		link = wrk.ctrlLink
	}
	return
}

func (wrk *Worker) addDataLink(link *Link) {
	wrk.mu.Lock()
	defer wrk.mu.Unlock()

	wrk.dataLinks.PushBack(link)
	link.registry = wrk.dataLinks.Back()
}

func (wrk *Worker) removeDataLink(link *Link) {
	wrk.mu.Lock()
	defer wrk.mu.Unlock()

	if link.registry == nil {
		return
	}

	wrk.dataLinks.Remove(link.registry.(*list.Element))
	link.registry = nil
}

func (wrk *Worker) clearDataLinksLocked() {
	if wrk.availableTokens != nil {
		close(wrk.availableTokens)
		wrk.availableTokens = nil
	}
	for wrk.dataLinks.Len() > 0 {
		link := wrk.dataLinks.Remove(wrk.dataLinks.Front()).(*Link)
		link.registry = nil
		link.Close()
		wrk.log.Debug("%v cleared", link)
	}
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
