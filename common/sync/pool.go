package sync

import "sync"

var (
	PoolForStrictConcurrency = PoolPerformanceOption(1)
	PoolForPerformance       = PoolPerformanceOption(2)
)

type PoolPerformanceOption int

type Pool struct {
	New      func() interface{}
	Finalize func(interface{})

	capacity  int
	allocated int
	pooled    chan interface{}

	mu   sync.Mutex
	cond *sync.Cond
}

func NewPool(cap int, opt PoolPerformanceOption) *Pool {
	return (&Pool{}).init(cap, opt)
}

func InitPool(p *Pool, cap int, opt PoolPerformanceOption) *Pool {
	return p.init(cap, opt)
}

func (p *Pool) init(cap int, opt PoolPerformanceOption) *Pool {
	p.capacity = cap * int(opt)
	p.pooled = make(chan interface{}, p.capacity)
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Pool) Get() interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		select {
		case i := <-p.pooled:
			return i
		default:
			if p.allocated < p.capacity {
				p.allocated++
				if p.New == nil {
					return nil
				} else {
					return p.New()
				}
			}

			p.cond.Wait()
		}
	}
}

func (p *Pool) Put(i interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case p.pooled <- i:
		p.cond.Signal()
	default:
		// Abandon. This is unlikely, but just in case.
		if p.Finalize != nil {
			p.Finalize(i)
		}
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	finalize := p.Finalize
	if finalize == nil {
		finalize = p.defaultFinalizer
	}
	for len(p.pooled) > 0 {
		finalize(<-p.pooled)
	}
}

func (p *Pool) defaultFinalizer(i interface{}) {
}
