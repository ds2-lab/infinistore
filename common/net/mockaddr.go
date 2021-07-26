package net

type StrAddr string

func (addr StrAddr) String() string {
	return string(addr)
}

func (addr StrAddr) Network() string {
	return "tcp"
}

type QueueAddr struct {
	last string
	ch   chan string
}

func NewQueueAddr(addr string) *QueueAddr {
	return &QueueAddr{
		last: addr,
		ch:   make(chan string, 1),
	}
}

func (addr *QueueAddr) String() string {
	return addr.last
}

func (addr *QueueAddr) Network() string {
	return "tcp"
}

func (addr *QueueAddr) Push(a string) {
	addr.ch <- a
}

func (addr *QueueAddr) Pop() string {
	addr.last = <-addr.ch
	return addr.last
}
