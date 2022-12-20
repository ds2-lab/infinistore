package client

import (
	"context"
	"fmt"
	"math/rand"
	sysSync "sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/infinicache/common/sync"
)

const (
	BucketSize = 20
	WindowSize = 20
	SeqStep    = 1000000
)

var (
	ErrAcked   = newWindowError("request acked")
	ErrNotSent = newWindowError("request not sent")
	ErrNotSeen = newWindowError("request not seen")

	NilBucket      = &ReqBucket{}
	DefaultTimeout = time.Second
)

type RequestMeta struct {
	// Request embeds application level requests.
	Request

	// Acked indicates the request is acknownledged.
	Acked bool

	// Deadline suggests default deadline of the requests. It can be overridden by request's context.
	Deadline time.Time

	// Notifier unblocks the request on window advancing.
	Notifier sync.WaitGroup
}

// IsTimeout returns if the request is timeout given the status of the request's context.
func (m *RequestMeta) testTimeout(t time.Time) error {
	if m.Acked || m.Notifier.IsWaiting() {
		return nil
	}

	// To avoid using lock, ensure the Request is stll avaiable.
	req := m.Request
	if req == nil {
		// If the request is not available, it must be acked.
		return nil
	}

	if dl, ok := req.Context().Deadline(); ok && dl.Before(t) {
		return context.DeadlineExceeded
	} else if !ok && m.Deadline.Before(t) {
		return ErrTimeout
	}
	return nil
}

type ReqBucket struct {
	seq      int64 //the sequence of the first request of the bucket
	acked    int64 // counter of the acked requests in the bucket
	filled   int64 // counter of the added requests in total in the bucket
	requests []*RequestMeta
	next     *ReqBucket
	refs     int32 // reference counter of the bucket, if refs is not 0, cannot be reset
}

func newReqBucket(seq int64) *ReqBucket {
	return &ReqBucket{
		seq:      seq,
		requests: make([]*RequestMeta, BucketSize),
		next:     NilBucket,
	}
}

func (b *ReqBucket) Reset() {
	if b.acked < b.filled {
		for i := 0; i < len(b.requests); i++ {
			if b.requests[i] != nil && !b.requests[i].Acked {
				_ = b.requests[i].SetResponse(ErrConnectionClosed)
				b.requests[i] = nil
			}
		}
	}
	b.refs = 0
	b.acked = 0
	b.filled = 0
}

func (b *ReqBucket) Ref() *ReqBucket {
	atomic.AddInt32(&b.refs, 1)
	return b
}

func (b *ReqBucket) Next() *ReqBucket {
	defer b.DeRef()
	return b.next.Ref()
}

func (b *ReqBucket) DeRef() {
	atomic.AddInt32(&b.refs, -1)
}

func (b *ReqBucket) Refs() int {
	return int(atomic.LoadInt32(&b.refs))
}

func (b *ReqBucket) String() string {
	if b == NilBucket {
		return "NilBucket"
	} else if b.seq == 0 {
		return "UnexpectedBucket"
	} else {
		return fmt.Sprintf("Bucket %d", b.seq)
	}
}

type Window struct {
	seq    int64 // Max sequence seen.
	top    *ReqBucket
	active *ReqBucket // Bucket stores the earlist unknowledged request.
	tail   *ReqBucket
	acked  int64 // Max continous acked sequence.
	size   int64
	gcing  int32
	close  int32

	mu       sysSync.RWMutex
	metaPool sysSync.Pool
	closed   chan struct{}
}

func NewWindow() *Window {
	rand.Seed(time.Now().UnixNano())
	seqStart := rand.Int63n(SeqStep)
	wnd := &Window{
		seq:    seqStart,
		top:    newReqBucket(seqStart + SeqStep),
		size:   WindowSize,
		closed: make(chan struct{}),
	}
	wnd.active = wnd.top
	wnd.tail = wnd.top
	wnd.acked = wnd.seq
	go wnd.cleanUp()
	return wnd
}

func (wnd *Window) Len() int {
	return (int(atomic.LoadInt64(&wnd.seq) - atomic.LoadInt64(&wnd.acked))) / SeqStep
}

func (wnd *Window) SetSize(size int) int {
	return int(atomic.SwapInt64(&wnd.size, int64(size)))
}

func (wnd *Window) ChangeSize(offset int) int {
	changed := int(atomic.SwapInt64(&wnd.size, int64(offset)))
	return changed - offset
}

func (wnd *Window) Reset() {
	wnd.mu.Lock()
	defer wnd.mu.Unlock()

	// Drain top
	tail := wnd.tail
	for wnd.top != tail {
		wnd.top.Reset()
		wnd.top = wnd.recycleTopLocked(wnd.top)
	}
	wnd.top.Reset()
	wnd.active = wnd.top

	// Reset availables
	atomic.StoreInt64(&wnd.acked, atomic.LoadInt64(&wnd.seq))
}

func (wnd *Window) AddRequest(req Request) (*RequestMeta, error) {
	wnd.mu.RLock()
	defer wnd.mu.RUnlock()

	if wnd.IsClosed() {
		return nil, ErrConnectionClosed
	}

	meta, _ := wnd.metaPool.Get().(*RequestMeta)
	if meta == nil {
		meta = &RequestMeta{}
	}
	meta.Request = req
	meta.Acked = false
	meta.Deadline = time.Now().Add(DefaultTimeout)
	seq := atomic.AddInt64(&wnd.seq, SeqStep)
	meta.SetSeq(seq)

	// Seek and fill bucket
	active := wnd.active.Ref()
	for active.seq > seq {
		// Lock free, wait for active to be updated on recycling.
		active.DeRef()
		active = wnd.active.Ref()
	}
	bucket, i, err := wnd.seekRLocked(seq, active, true)
	if err != nil {
		return nil, err
	}
	bucket.requests[i] = meta
	atomic.AddInt64(&bucket.filled, 1)

	// Check window's size
	l := seq - atomic.LoadInt64(&wnd.acked)
	if l > atomic.LoadInt64(&wnd.size)*SeqStep {
		// Wait for available slot.
		// log.Printf("Wait %d, len %d", seq, len)
		meta.Notifier.Add(1)
		wnd.mu.RUnlock()
		meta.Notifier.Wait()
		wnd.mu.RLock()
	}

	// log.Printf("Add %d, %d", seq, i)
	meta.Deadline = time.Now().Add(DefaultTimeout)
	return meta, nil
}

func (wnd *Window) MatchRequest(seq int64) (Request, error) {
	wnd.mu.RLock()
	defer wnd.mu.RUnlock()

	if wnd.IsClosed() {
		return nil, ErrConnectionClosed
	}

	_, _, meta, err := wnd.findRequestMetaRLocked(seq)
	if err != nil {
		return nil, err
	}

	return meta.Request, nil
}

func (wnd *Window) AckRequest(seq int64) (Request, error) {
	// Avoid deadlock: the requests will be cleared during closing.
	if wnd.IsClosed() {
		return nil, ErrConnectionClosed
	}

	wnd.mu.RLock()
	defer wnd.mu.RUnlock()

	if wnd.IsClosed() {
		return nil, ErrConnectionClosed
	}

	// Locate the request.
	bucket, i, meta, err := wnd.findRequestMetaRLocked(seq)
	if err != nil {
		return nil, err
	}

	req := meta.Request
	meta.Acked = true
	atomic.AddInt64(&bucket.acked, 1)
	// log.Printf("Ack %d", seq)

	blocked := bucket
	advanced := 0
	// Load "acked" after acknowledgement.
	// If loaded < seq - 1, earlier request will cover current sequence.
	acked := atomic.LoadInt64(&wnd.acked)
	for seq == acked+SeqStep {
		if !atomic.CompareAndSwapInt64(&wnd.acked, acked, seq) {
			// If failed to update, another updator took control.
			// log.Printf("Yield ack %d, acked %d", seq, atomic.LoadInt64(&wnd.acked))
			break
		}
		acked = seq
		advanced++
		bucket.requests[i] = nil
		meta.Request = nil
		meta.Notifier.Reset()
		wnd.metaPool.Put(meta)
		// log.Printf("Update acked to %d, try release %d", acked, seq+atomic.LoadInt64(&wnd.size)*SeqStep)

		// Notify possible blocked requests.
		blocked, i, _ = wnd.seekRLocked(seq+atomic.LoadInt64(&wnd.size)*SeqStep, blocked.Ref(), false)
		if blocked != NilBucket {
			meta := blocked.requests[i]
			if meta != nil {
				// log.Printf("Release %d", meta.Seq())
				meta.Notifier.Done() // Customized version allows calling safely.
			}
		} else {
			blocked = bucket // Reset
		}

		// Succeeded routing goes to the end. Use updated seq for each iteration
		seq += SeqStep
		if seq > atomic.LoadInt64(&wnd.seq) {
			break
		}
		bucket, i, _ = wnd.seekRLocked(seq, bucket.Ref(), false)
		if bucket == NilBucket {
			break
		}
		meta = bucket.requests[i]
		if meta == nil || !meta.Acked {
			break
		}
	}

	if wnd.top != wnd.tail && advanced > 0 && acked >= wnd.top.next.seq-SeqStep && atomic.CompareAndSwapInt32(&wnd.gcing, 0, 1) {
		go wnd.gc(acked)
	}
	return req, nil
}

func (wnd *Window) Close() error {
	if !atomic.CompareAndSwapInt32(&wnd.close, 0, 1) {
		return nil
	}
	wnd.mu.Lock()
	defer wnd.mu.Unlock()

	select {
	case <-wnd.closed:
		return nil
	default:
		close(wnd.closed)
	}

	for bucket := wnd.top; bucket != NilBucket; bucket, bucket.next = bucket.next, nil {
		bucket.Reset()
		// bucket.requests = nil
	}
	wnd.top = nil
	wnd.active = nil
	wnd.tail = nil
	return nil
}

func (wnd *Window) IsClosed() bool {
	return atomic.LoadInt32(&wnd.close) == 1
}

func (wnd *Window) seekRLocked(seq int64, bucket *ReqBucket, forSet bool) (*ReqBucket, int64, error) {
	// defer bucket.DeRef()
	i := seq - bucket.seq
	var err error
	for i >= BucketSize*SeqStep {
		if forSet {
			bucket, err = wnd.prepareNextBucketForSetRLocked(bucket)
			if err != nil {
				return bucket, i / SeqStep, err
			}
		} else {
			bucket = bucket.Next()
			if bucket == NilBucket {
				err = ErrNotSeen
				break
			}
		}
		i = seq - bucket.seq
	}
	bucket.DeRef()
	return bucket, i / SeqStep, err
}

func (wnd *Window) findRequestMetaRLocked(seq int64) (*ReqBucket, int64, *RequestMeta, error) {
	if seq <= atomic.LoadInt64(&wnd.acked) {
		// log.Printf("acked seq: %d, acked: %d", seq, acked)
		return nil, 0, nil, ErrAcked
	} else if seq > atomic.LoadInt64(&wnd.seq) {
		// log.Printf("unsent seq: %d, seen: %d", seq, seen)
		return nil, 0, nil, ErrNotSent
	}

	// Seek and locate meta
	bucket, i, err := wnd.seekRLocked(seq, wnd.active.Ref(), false)
	if err != nil {
		// log.Printf("unseen seq: %d, no more bucket", seq)
		return bucket, i, nil, err
	}
	meta := bucket.requests[i]
	if meta == nil {
		// log.Printf("unseen seq: %d, no meta", seq)
		return bucket, i, nil, ErrNotSeen
	} else if meta.Notifier.IsWaiting() {
		// log.Printf("unsent seq: %d, waiting", seq)
		return bucket, i, meta, ErrNotSent
	}

	return bucket, i, meta, nil
}

func (wnd *Window) prepareNextBucketForSetRLocked(bucket *ReqBucket) (*ReqBucket, error) {
	if bucket.next != NilBucket {
		return bucket.Next(), nil
	}

	// Tail
	wnd.mu.RUnlock()
	wnd.mu.Lock()
	defer wnd.mu.RLock()
	defer wnd.mu.Unlock()

	if wnd.IsClosed() {
		bucket.DeRef()
		return NilBucket, ErrConnectionClosed
	}

	if bucket == wnd.tail {
		wnd.tail.next = newReqBucket(wnd.tail.seq + BucketSize*SeqStep)
		wnd.tail = wnd.tail.next
	}
	return bucket.Next(), nil
}

func (wnd *Window) recycleTopLocked(bucket *ReqBucket) *ReqBucket {
	bucket.seq = wnd.tail.seq + BucketSize*SeqStep
	next := bucket.next
	wnd.tail.next = bucket // Append old top to tail
	wnd.tail = bucket      // Update tail to old top
	wnd.tail.next = NilBucket
	return next
}

func (wnd *Window) cleanUp() {
	timer := time.NewTimer(DefaultTimeout / 2)
	var t time.Time
	for {
		select {
		case <-wnd.closed:
			return
		case t = <-timer.C:
		}

		if wnd.Len() > 0 {
			seq := atomic.LoadInt64(&wnd.acked) + SeqStep
			bucket := wnd.active
			i := int64(0)
			for j := int64(0); j < atomic.LoadInt64(&wnd.size); j++ {
				wnd.mu.RLock()
				bucket, i, _ = wnd.seekRLocked(seq+j*SeqStep, bucket.Ref(), false)
				if bucket == NilBucket {
					wnd.mu.RUnlock()
					break
				}
				// In lock, get a reference of the meta to avoid wnd being closed
				meta := bucket.requests[i]
				wnd.mu.RUnlock()

				if meta == nil {
					continue
				} else if err := meta.testTimeout(t); err != nil {
					meta.SetResponse(err)
					req := meta.Request
					if req != nil {
						_, _ = wnd.AckRequest(req.Seq())
					}
				}
			}
		}

		timer.Reset(DefaultTimeout / 2)
	}
}

func (wnd *Window) gc(acked int64) {
	wnd.mu.Lock()
	defer wnd.mu.Unlock()
	defer atomic.StoreInt32(&wnd.gcing, 0)

	if wnd.IsClosed() {
		return
	}

	// Recycle bucket between top and active
	for wnd.top != wnd.active && wnd.top.Refs() == 0 {
		// log.Printf("Recycle bucket %d-%d", wnd.top.seq, wnd.top.seq+(BucketSize-1)*SeqStep)
		wnd.top.Reset() // Nothing but reset start and end only.
		wnd.top = wnd.recycleTopLocked(wnd.top)
	}

	// Update active
	for wnd.active.next != NilBucket && acked >= wnd.active.next.seq-SeqStep {
		wnd.active = wnd.active.next
		// log.Printf("Set active to bucket %d-%d", wnd.active.seq, wnd.active.seq+(BucketSize-1)*SeqStep)
	}
}
