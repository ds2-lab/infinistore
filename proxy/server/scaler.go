package server

import (
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

type Scaler struct {
	log     logger.ILogger
	proxy   *Proxy
	Signal  chan struct{}
	ready   chan struct{}
	counter int32
	step    int
}

func NewScaler() *Scaler {
	s := &Scaler{
		log: &logger.ColorLogger{
			Prefix: "Scaler ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		Signal:  make(chan struct{}, 1),
		ready:   make(chan struct{}),
		counter: 0,
		step:    5,
	}
	return s
}

// check the cluster usage information periodically
func (s *Scaler) Daemon() {
	for {
		s.log.Debug("in scaler Daemon, Group len is %v, active instance is %v", s.proxy.group.Len(), ActiveInstance)
		select {
		// receive scaling out signal
		case <-s.Signal:
			//TODO: receive scaling out signal, enlarge group capacity
			for i := 0; i < s.step; i++ {

				name := LambdaPrefix
				s.log.Debug("[Scaling lambda instance %v%v]", name, i)
				node := scheduler.GetForGroup(s.proxy.group, i, "out")
				node.Meta.Capacity = InstanceCapacity
				node.Meta.IncreaseSize(InstanceOverhead)

				go func() {
					node.WarmUp()
					if atomic.AddInt32(&s.counter, 1) == STEP {
						s.log.Info("[scale out is ready]")
						close(s.ready)
					}
				}()

				// Begin handle requests
				go node.HandleRequests()
				//s.placer.Append(node)
			}

			// update current active lambda instance
			ActiveInstance += STEP

			// reset counter
			s.counter = 0

			// update bucket and placer info
			s.proxy.movingWindow.getCurrentBucket().pointer += s.step
			s.proxy.placer.scaling = false
		}
	}
}

func (s *Scaler) Ready() chan struct{} {
	return s.ready
}
