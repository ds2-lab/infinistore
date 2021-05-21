package lambdastore

import "sync"

type CandidateProvider func() *Instance

type CandidatesProvider interface {
	GetBackupCandidates([]*Instance) int
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
		done:      make(chan struct{}),
	}
}

func (q *CandidateQueue) Start() {
	go q.start()
}

func (q *CandidateQueue) Candidates() <-chan *Instance {
	return q.allocator
}

func (q *CandidateQueue) Close() {
	q.mu.Lock()

	select {
	case <-q.done:
	default:
		close(q.done)
	}

	q.mu.Unlock()
}

func (q *CandidateQueue) start() {
	for {
		if q.pos == len(q.buf) {
			q.buf = q.buf[:cap(q.buf)] // reset len
			n := q.provider.GetBackupCandidates(q.buf)
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
