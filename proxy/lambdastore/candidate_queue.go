package lambdastore

import "sync"

type CandidateProvider func() *Instance

type CandidatesProvider interface {
	LoadCandidates(*CandidateQueue, []*Instance) int
}

type CandidateQueue struct {
	provider  CandidatesProvider
	buf       []*Instance
	pos       int
	allocator chan *Instance
	done      chan struct{}
	mu        sync.Mutex
}

func NewCandidateQueue(bufsize int, provider CandidatesProvider) *CandidateQueue {
	return &CandidateQueue{
		provider:  provider,
		buf:       make([]*Instance, bufsize),
		pos:       bufsize,
		allocator: make(chan *Instance, bufsize),
	}
}

// Start the autoload service
func (q *CandidateQueue) Start() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done == nil {
		q.done = make(chan struct{})
		go q.start()
		return true
	}

	return false
}

func (q *CandidateQueue) Candidates() <-chan *Instance {
	return q.allocator
}

func (q *CandidateQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done == nil {
		return
	}

	select {
	case <-q.done:
	default:
		close(q.done)
	}
}

func (q *CandidateQueue) start() {
	for {
		if q.pos == len(q.buf) {
			q.buf = q.buf[:cap(q.buf)] // reset len
			n := q.provider.LoadCandidates(q, q.buf)
			if n == 0 {
				close(q.allocator)
				return
			}
			if n < cap(q.buf) {
				q.buf = q.buf[:n]
			}
			q.pos = 0
		}
		select {
		case <-q.done:
			close(q.allocator)
			return
		case q.allocator <- q.buf[q.pos]:
			q.pos++
		}
	}
}
