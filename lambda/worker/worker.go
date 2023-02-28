package worker

import (
	"container/list"
	"errors"
	"io"
	"math"
	"math/rand"
	sysnet "net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ds2-lab/infinistore/common/logger"
	"github.com/ds2-lab/infinistore/common/net"
	"github.com/ds2-lab/infinistore/common/stats"
	protocol "github.com/ds2-lab/infinistore/common/types"
	"github.com/ds2-lab/infinistore/common/util"
	"github.com/ds2-lab/infinistore/lambda/types"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

const (
	WorkerRunning        = int32(0)
	WorkerClosing        = int32(1)
	WorkerClosed         = int32(2)
	RetrialBackoffFactor = 2
	MinDataLinks         = 2
	MaxDataLinks         = 10
	MaxDataLinkRetrial   = 3
)

var (
	defaultOption WorkerOptions

	ErrWorkerClosed     = errors.New("worker closed")
	ErrNoProxySpecified = errors.New("no proxy specified")
	ErrInvalidShortcut  = errors.New("invalid shortcut connection")
	ErrShouldIgnore     = errors.New("should ignore")
	// MaxControlRequestSize = int64(200000) // 200KB, which can be transmitted in 20ms.

	DialTimeout           = 20 * time.Millisecond
	RetrialDelayStartFrom = 100 * time.Millisecond
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

	// Dynamic connection fields.

	// Tokens works as follows:
	// 1. It consumes a token to establish a new data link. The link will own the consumed token.
	// 2. On serving a request, the link will return owned token so the worker can establish
	//    a new data link that will be ready to serve another request ASAP. Abandon if there are sufficient spare links.
	// 3. After served the request, the link will be granted a token again.
	// Current implementation ensures a limited number of data links are newly created(unused) and ready to serve.
	availableTokens chan *struct{}
	minDLs          int32 // Minimum spare data links required.
	spareDLs        int32 // # of spare data links.
	dataLinks       *list.List
	updatedAt       time.Time

	// Proxies container
	proxies []*HandlerProxy

	// Stats
	movingRTT *stats.MovingStats
}

type WorkerOptions struct {
	DryRun       bool
	MinDataLinks int
	LogLevel     int
}

func NewWorker(lifeId int64) *Worker {
	rand.Seed(lifeId)
	worker := &Worker{
		id:       rand.Int31(),
		Server:   redeo.NewServer(nil),
		log:      &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Worker:"},
		ctrlLink: NewLink(true),
		// dataLink:    NewLink(false),
		heartbeater: new(DefaultHeartbeater),
		dataLinks:   list.New(),
		closed:      WorkerClosed,
		proxies:     make([]*HandlerProxy, 0, 10), // 10 for a initial size.
		movingRTT:   stats.NewMovingUnilateralValue(10, 0.875, 0.125),
	}
	worker.movingRTT.Add(float64(20 * time.Millisecond))
	worker.Server.HandleFunc(protocol.CMD_ACK, worker.ackHandler)
	worker.Server.HandleCallbackFunc(worker.responseHandler)
	return worker
}

// types.ServerStats implementation.
func (wrk *Worker) GetStats() types.ServerStats {
	return wrk
}

func (wrk *Worker) RTT() time.Duration {
	return time.Duration(wrk.movingRTT.Value())
}

func (wrk *Worker) Id() int32 {
	return wrk.id
}

func (wrk *Worker) SetHeartbeater(heartbeater Heartbeater) {
	wrk.heartbeater = heartbeater
}

func (wrk *Worker) StartOrResume(proxyAddr sysnet.Addr, args ...*WorkerOptions) (isStart bool, err error) {
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
	if logger, ok := wrk.log.(*logger.ColorLogger); ok {
		logger.Level = opts.LogLevel
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
	wrk.log.Info("Pausing worker")
	wrk.mu.Lock()
	defer wrk.mu.Unlock()

	wrk.clearDataLinksLocked()
}

func (wrk *Worker) Close() {
	wrk.log.Info("Closing worker")

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

// VerifyDataLinks Verify the number of available data links at proxy side.
func (wrk *Worker) VerifyDataLinks(availableLinks int, reportedAt time.Time) {
	if reportedAt.Before(wrk.updatedAt) {
		// Reported data are out of date.
		wrk.log.Debug("Discard out of date link report: %d, reported before %v", availableLinks, wrk.updatedAt.Sub(reportedAt))
		return
	}

	available := int32(availableLinks)
	spares := atomic.LoadInt32(&wrk.spareDLs)
	if available == spares {
		return
	}

	diff := available - spares
	spares = wrk.updateSpareDLs(diff, reportedAt)
	if available >= wrk.minDLs {
		wrk.log.Info("Correct data links, corrected %d, current spare links: %d.", diff, spares)
		return
	}

	borrowed := 0
For:
	for i := available; i < wrk.minDLs; i++ {
		select {
		case wrk.availableTokens <- &struct{}{}:
			borrowed++
		default:
			// Unlikely, but token list is full.
			break For
		}
	}
	wrk.log.Warn("Insufficient data links, corrected %d, will borrow: %d, current spare links: %d.", diff, borrowed, spares)
}

func (wrk *Worker) ensureConnection(link *Link, proxyAddr sysnet.Addr, opts *WorkerOptions, started *sync.WaitGroup) error {
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

func (wrk *Worker) serve(link *Link, proxyAddr sysnet.Addr, opts *WorkerOptions, started *sync.WaitGroup) {
	var once sync.Once
	defer wrk.readyToClose.Done()
	defer once.Do(started.Done)

	timeout := DialTimeout
	link.addr = proxyAddr.String() // To be compatibile with shortcut QueueAddr, keep a copy of string address.
	hbFlags := protocol.PONG_FOR_CTRL
	if !link.IsControl() {
		hbFlags = protocol.PONG_FOR_DATA
	}
	for {
		// Connect to proxy.
		var conn sysnet.Conn
		var remoteAddr string
		wrk.log.Debug("Ready to connect %v", link.addr)
		if opts.DryRun {
			shortcuts, ok := net.Shortcut.Dial(link.addr)
			if !ok {
				wrk.log.Error("Oops, no shortcut connection available for dry running, retry after %v", timeout)
				<-time.After(timeout)
			} else {
				conn = shortcuts[0].Client
				remoteAddr = shortcuts[0].String()
			}
		} else {
			dailer := &sysnet.Dialer{Timeout: timeout}
			cn, err := dailer.Dial("tcp", link.addr)
			if err == nil {
				conn = cn
				remoteAddr = cn.RemoteAddr().String()
			} else if netErr, ok := err.(sysnet.Error); ok && netErr.Timeout() {
				wrk.log.Warn("Failed to connect proxy %s: %v, retry.", proxyAddr, err)
			} else {
				wrk.log.Warn("Failed to connect proxy %s: %v, retry after %v", proxyAddr, err, timeout)
				<-time.After(timeout)
			}
		}

		// Recheck if server closed in mutex
		if wrk.IsClosed() {
			if conn != nil {
				conn.Close()
			}
			return
		}

		// Check connecting status.
		if conn != nil {
			timeout = DialTimeout
			link.Reset(conn)
			wrk.log.Info("Connection(%s:%v) to %v established.", util.Ifelse(link.IsControl(), "c", "d").(string), link.ID(), remoteAddr)
		}

		// Flag started after first connecting trial.
		once.Do(started.Done)

		if conn == nil {
			// retry
			timeout = wrk.nextDelay(timeout)
			continue
		}

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
			return
		}

		if err != nil && err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			wrk.log.Warn("Connection(%v) closed: %v, reconnecting...", link.ID(), err)
		} else {
			// Closed by the proxy, stop worker.
			wrk.log.Info("Connection(%v) closed from proxy. Closing worker...", link.ID())
			// Trigger close asynchoronously.
			go wrk.Close()
			return
		}
	}
}

func (wrk *Worker) reserveConnection(links *list.List, proxyAddr sysnet.Addr, opts *WorkerOptions, started *sync.WaitGroup) {
	wrk.availableTokens = make(chan *struct{}, opts.MinDataLinks)
	// Fill tokens
	for i := 0; i < opts.MinDataLinks; i++ {
		wrk.availableTokens <- &struct{}{}
	}
	wrk.minDLs = int32(len(wrk.availableTokens))
	wrk.spareDLs = 0
	wrk.updatedAt = time.Now()
	ch := wrk.availableTokens
	for token := range ch {
		if link := wrk.reserveDataLink(nil, token); link == nil {
			// The number of spare links exceeds the limit, abandon the token.
			// This is useful for token returning since we may borrow tokens during VerifyDataLinks().
			continue
		} else if err := wrk.serveOnce(link, proxyAddr, opts); err == nil {
			wrk.addDataLink(link)
		} else if err == ErrWorkerClosed {
			link.Close()
			// continue to drain the channel
		} else {
			// Failed to connect, return token and retry.
			wrk.flagReservationUsed(link)
			link.Close()
		}
	}
	wrk.log.Info("Data link reservation stopped.")
}

// serveOnce establishes the link and serves the link in a goroutine. No reconnecting will be performed.
// Token associated with the link will be returned on disconnecting if it has not been consumed (e.g. Not being used to serve any request).
func (wrk *Worker) serveOnce(link *Link, proxyAddr sysnet.Addr, opts *WorkerOptions) (err error) {
	// Connect to proxy.
	var conn sysnet.Conn
	var remoteAddr string
	timeout := DialTimeout
	for i := 0; i < MaxDataLinkRetrial; i++ {
		if opts.DryRun {
			proxyAddr.(*net.QueueAddr).Pop()
			wrk.log.Debug("Ready to connect %v", proxyAddr)
			link.addr = proxyAddr.String()
			shortcuts, ok := net.Shortcut.Dial(proxyAddr.String())
			if !ok {
				// Dail to shortcut should not fail and will not retry.
				wrk.log.Error("Oops, no shortcut connection available for dry running")
				return ErrInvalidShortcut
			}
			conn = shortcuts[0].Client
			remoteAddr = shortcuts[0].String()
		} else {
			wrk.log.Debug("Ready to connect %v, attempt %d", proxyAddr, i+1)
			link.addr = proxyAddr.String()
			dialer := &sysnet.Dialer{Timeout: timeout}
			conn, err = dialer.Dial("tcp", proxyAddr.String())
			if err != nil {
				if netErr, ok := err.(sysnet.Error); ok && netErr.Timeout() {
					wrk.log.Warn("Failed to connect proxy %s, attempt %d: %v", proxyAddr, i+1, err)
				} else {
					wrk.log.Warn("Failed to connect proxy %s, attempt %d: %v, retry after %v", proxyAddr, i+1, err, DialTimeout)
					<-time.After(timeout)
				}
				timeout = wrk.nextDelay(timeout)
				continue
			}
			remoteAddr = conn.RemoteAddr().String()
		}
	}
	if err != nil {
		wrk.log.Error("Stop attempts to connect to proxy %s", proxyAddr)
		return err
	} else {
		// Log with len+1, will add to the link if no error.
		wrk.log.Info("Connection(%v) to %v established(total: %d).", link, remoteAddr, wrk.dataLinks.Len()+1)
	}

	// Recheck if server closed in mutex
	if wrk.IsClosed() {
		return ErrWorkerClosed
	}
	link.Reset(conn)

	// Send a heartbeat on the link immediately to confirm store information.
	// The heartbeat will be queued and send once worker started.
	// On error, the connection will be closed .
	wrk.log.Debug("Invoke heartbeater(%v)", link.ID())
	if err := wrk.heartbeater.SendToLink(link, protocol.PONG_FOR_DATA); err != nil {
		wrk.log.Warn("Heartbeat(%v) err: %v", link.ID(), err)
		return err
	}

	// Serve the client.
	go func(link *Link) {
		client := link.Client
		if client == nil {
			// Can be cleared during worker pausing.
			return
		}
		_ = wrk.Server.ServeClient(link.Client, false) // Enable asych mode to allow sending request.
		// Recycle the spare token if the link owns one.
		wrk.flagReservationUsed(link)
		wrk.removeDataLink(link)
		link.Close()
		wrk.log.Info("Connection(%v) disconnected(total: %d).", link, wrk.dataLinks.Len())
	}(link)

	return nil
}

// reserveDataLink grants the link a spare token, create the token if not specified.
// The link will be created if not already existed with the exception if the number spare links exceeds limit.
func (wrk *Worker) reserveDataLink(link *Link, token *struct{}) *Link {
	spares := wrk.updateSpareDLs(1)
	defer wrk.log.Debug("Link available, spare links: %d", spares)

	// We will not create new link if there are spare links
	if spares > wrk.minDLs && link == nil {
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

// flagReservationUsed returns the associated spare token, so new link can be created to serve more requests.
func (wrk *Worker) flagReservationUsed(link *Link) bool {
	token := link.RevokeToken()
	if token == nil {
		return false
	}

	availableTokens := wrk.availableTokens
	if availableTokens == nil {
		return false
	}

	// Update the number of spare links. If the number of spare links exceeds the limit, abandon the token.
	if spares := wrk.updateSpareDLs(-1); spares < wrk.minDLs {
		select {
		case wrk.availableTokens <- token:
			wrk.log.Debug("Token recycled, spare links: %d", spares)
		default:
			// Unexpected contradiction: we are short of spare links but the token channel is full.
			// Reconcile the number of spare links.
			if spares > 0 {
				spares = wrk.updateSpareDLs(-spares) // Use relative value to avoid locking.
			}
			wrk.log.Warn("Token overflowed, reset spare links, current spare links: %d.", spares)
		}
	} else {
		wrk.log.Debug("Token destroyed, spare links: %d", spares)
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
		start := time.Now()
		if err := link.acked.Timeout(); err != nil {
			wrk.log.Warn("Acknowledge of %v: %v", cmd, err)
			link.acked.Resolve()
		} else {
			wrk.movingRTT.Add(float64(time.Since(start)))
		}
		cb()
	}()
}

// HandleCallback callback handler
func (wrk *Worker) responseHandler(w resp.ResponseWriter, r interface{}) {
	rsp := r.(Response)
	client := redeo.GetClient(rsp.Context())
	link := LinkFromClient(client)

	if wrk.IsClosed() {
		wrk.log.Warn("Abort flushing response(%v): %v", rsp, ErrWorkerClosed)
		rsp.abandon(ErrWorkerClosed)
		return
	}

	err := rsp.flush(w)
	if err != nil && err != ErrShouldIgnore {
		// Set link as failure instead of closing it. Different links (control or data) may reconnect or disconnect.
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
		retryIn := DialTimeout * time.Duration(math.Pow(float64(RetrialBackoffFactor), float64(rsp.maxAttempts()-left-1)))
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

func (wrk *Worker) nextDelay(delay time.Duration) time.Duration {
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
	wrk.log.Info("Data links cleared.")
}

func (wrk *Worker) updateSpareDLs(change int32, reports ...time.Time) int32 {
	reportedAt := time.Now()
	if len(reports) > 0 {
		reportedAt = reports[0]
	}
	spares := atomic.AddInt32(&wrk.spareDLs, change)
	if reportedAt.After(wrk.updatedAt) {
		wrk.updatedAt = reportedAt
	}
	return spares
}

type TestClient struct {
	Conn   sysnet.Conn
	Writer *resp.RequestWriter
	Reader resp.ResponseReader
}

func NewTestClient(cn sysnet.Conn) *TestClient {
	return &TestClient{
		Conn:   cn,
		Writer: resp.NewRequestWriter(cn),
		Reader: resp.NewResponseReader(cn),
	}
}
