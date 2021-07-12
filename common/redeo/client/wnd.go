package client

import (
	"errors"
	sysSync "sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/sync"
)

const (
	BucketSize = 20
	WindowSize = 20
)

var (
	ErrAcked   = errors.New("request acked")
	ErrNotSent = errors.New("request not sent")
	ErrNotSeen = errors.New("request not seen")

	CtxNotifier = struct{}{}
	NilBucket   = &ReqBucket{}
)

type RequestMeta struct {
	Request
	Acked    bool
	Notifier sync.WaitGroup
}

type ReqBucket struct {
	seq      int64
	acked    int64
	filled   int64
	requests []*RequestMeta
	next     *ReqBucket
	refs     int32
}

func (b *ReqBucket) Reset() {
	if b.acked < b.filled {
		for i := 0; i < len(b.requests); i++ {
			if b.requests[i] != nil && !b.requests[i].Acked {
				b.requests[i].SetResponse(ErrConnectionClosed)
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
	b.DeRef()
	return b.next.Ref()
}

func (b *ReqBucket) DeRef() {
	atomic.AddInt32(&b.refs, -1)
}

func (b *ReqBucket) Refs() int {
	return int(atomic.LoadInt32(&b.refs))
}

type Window struct {
	seq     int64 // Max sequence seen.
	baseSeq int64
	top     *ReqBucket
	active  *ReqBucket
	tail    *ReqBucket
	acked   int64 // Max continous acked sequence.
	size    int64
	gcing   int32

	mu       sysSync.RWMutex
	metaPool sysSync.Pool
}

func NewWindow() *Window {
	wnd := &Window{
		seq:     0,
		baseSeq: 1,
		top: &ReqBucket{
			requests: make([]*RequestMeta, BucketSize),
			next:     NilBucket,
		},
		size: WindowSize,
	}
	wnd.top.seq = wnd.baseSeq
	wnd.active = wnd.top
	wnd.tail = wnd.top
	return wnd
}

func (wnd *Window) Len() int {
	return int(atomic.LoadInt64(&wnd.seq) - atomic.LoadInt64(&wnd.acked))
}

func (wnd *Window) SetSize(size int) int {
	return int(atomic.SwapInt64(&wnd.size, int64(size)))
}

func (wnd *Window) ChangeSize(offset int) int {
	new := int(atomic.SwapInt64(&wnd.size, int64(offset)))
	return new - offset
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
	wnd.baseSeq = atomic.LoadInt64(&wnd.seq) + 1
	atomic.StoreInt64(&wnd.acked, atomic.LoadInt64(&wnd.seq))
}

func (wnd *Window) AddRequest(req Request) {
	meta, _ := wnd.metaPool.Get().(*RequestMeta)
	if meta == nil {
		meta = &RequestMeta{}
	}
	meta.Request = req
	meta.Acked = false
	seq := atomic.AddInt64(&wnd.seq, 1)
	meta.SetSeq(seq)

	// Seek and fill bucket
	active := wnd.active.Ref()
	for active.seq > seq {
		active.DeRef()
		active = wnd.active.Ref()
	}
	bucket, i := wnd.seek(seq, wnd.active.Ref(), true)
	bucket.requests[i] = meta
	atomic.AddInt64(&bucket.filled, 1)

	// Check window's size
	len := seq - atomic.LoadInt64(&wnd.acked)
	if len > atomic.LoadInt64(&wnd.size) {
		// Wait for available slot.
		// log.Printf("Wait %d, len %d", seq, len)
		meta.Notifier.Add(1)
		meta.Notifier.Wait()
	}

	// log.Printf("Add %d, %d", seq, i)
}

func (wnd *Window) AckRequest(seq int64) (Request, error) {
	wnd.mu.RLock()
	defer wnd.mu.RUnlock()

	if seq <= atomic.LoadInt64(&wnd.acked) {
		// log.Printf("acked seq: %d, acked: %d", seq, acked)
		return nil, ErrAcked
	} else if seq > atomic.LoadInt64(&wnd.seq) {
		// log.Printf("unsent seq: %d, seen: %d", seq, seen)
		return nil, ErrNotSent
	}

	// Seek and locate meta
	bucket, i := wnd.seek(seq, wnd.active.Ref(), false)
	if bucket == NilBucket {
		// log.Printf("unseen seq: %d, no more bucket", seq)
		return nil, ErrNotSeen
	}
	meta := bucket.requests[i]
	if meta == nil {
		// log.Printf("unseen seq: %d, no meta", seq)
		return nil, ErrNotSeen
	} else if meta.Notifier.IsWaiting() {
		// log.Printf("unsent seq: %d, waiting", seq)
		return nil, ErrNotSent
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
	for seq == acked+1 {
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
		// log.Printf("Update acked to %d", acked)

		// Notify possible blocked requests.
		blocked, i = wnd.seek(seq+atomic.LoadInt64(&wnd.size), blocked.Ref(), false)
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
		seq++
		if seq > atomic.LoadInt64(&wnd.seq) {
			break
		}
		bucket, i = wnd.seek(seq, bucket.Ref(), false)
		if bucket == NilBucket {
			break
		}
		meta = bucket.requests[i]
		if meta == nil || !meta.Acked {
			break
		}
	}

	if wnd.top != wnd.tail && advanced > 0 && acked >= wnd.top.next.seq-1 && atomic.CompareAndSwapInt32(&wnd.gcing, 0, 1) {
		go wnd.gc(acked)
	}
	return req, nil
}

func (wnd *Window) Close() error {
	wnd.mu.Lock()
	defer wnd.mu.Unlock()

	for bucket := wnd.top; bucket != NilBucket; bucket, bucket.next = bucket.next, nil {
		bucket.Reset()
		bucket.requests = nil
	}
	wnd.top = nil
	wnd.tail = nil
	return nil
}

func (wnd *Window) seek(seq int64, bucket *ReqBucket, forSet bool) (*ReqBucket, int64) {
	i := seq - bucket.seq
	for i >= BucketSize {
		if forSet {
			bucket = wnd.prepareNextBucketForSet(bucket)
		} else {
			bucket = bucket.Next()
			if bucket == NilBucket {
				break
			}
		}
		i = seq - bucket.seq
	}
	bucket.DeRef()
	return bucket, i
}

func (wnd *Window) prepareNextBucketForSet(bucket *ReqBucket) *ReqBucket {
	next := bucket.Next()
	if next != NilBucket {
		return next
	} else {
		// Ensure reference intact
		bucket.Ref()
	}

	// Tail
	wnd.mu.Lock()
	defer wnd.mu.Unlock()

	if bucket == wnd.tail {
		wnd.tail.next = &ReqBucket{
			seq:      wnd.tail.seq + BucketSize,
			requests: make([]*RequestMeta, BucketSize),
			next:     NilBucket,
		}
		wnd.tail = wnd.tail.next
	}
	return bucket.Next()
}

func (wnd *Window) recycleTopLocked(bucket *ReqBucket) *ReqBucket {
	bucket.seq = wnd.tail.seq + BucketSize
	next := bucket.next
	wnd.tail.next = bucket // Append old top to tail
	wnd.tail = bucket      // Update tail to old top
	wnd.tail.next = NilBucket
	return next
}

func (wnd *Window) gc(acked int64) {
	wnd.mu.Lock()
	defer wnd.mu.Unlock()

	// Recycle bucket between top and active
	for wnd.top != wnd.active && wnd.top.Refs() == 0 {
		// log.Printf("Recycle bucket %d-%d", wnd.top.seq, wnd.top.seq+BucketSize-1)
		wnd.top.Reset() // Nothing but reset start and end only.
		wnd.top = wnd.recycleTopLocked(wnd.top)
	}

	// Update active
	for wnd.active.next != NilBucket && acked >= wnd.active.next.seq-1 {
		wnd.active = wnd.active.next
		// log.Printf("Set active to bucket %d-%d", wnd.active.seq, wnd.active.seq+BucketSize-1)
	}

	atomic.StoreInt32(&wnd.gcing, 0)
}
