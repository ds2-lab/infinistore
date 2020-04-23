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
			bucket := s.proxy.movingWindow.getCurrentBucket()
			tmpGroup := NewGroup(NumLambdaClusters)
			//TODO: receive scaling out signal, enlarge group capacity
			for i := range tmpGroup.All {
				node := scheduler.GetForGroup(tmpGroup, i, "")
				node.Meta.Capacity = InstanceCapacity
				node.Meta.IncreaseSize(InstanceOverhead)
				//s.log.Debug("[scaling lambda instance %v, size %v]", node.Name(), node.Size())

				go func() {
					node.WarmUp()
					if atomic.AddInt32(&s.counter, 1) == int32(len(tmpGroup.All)) {
						s.log.Info("[scale out is ready]")
					}
				}()

				// Begin handle requests
				go node.HandleRequests()
			}

			// reset counter
			s.counter = 0

			// update bucket and placer info

			// append to current bucket group
			bucket.scale(tmpGroup)

			// move current bucket offset
			//s.proxy.movingWindow.getCurrentBucket().offset += NumLambdaClusters

			// update proxy group
			s.proxy.group = s.proxy.movingWindow.getAllGroup()
			s.proxy.placer.scaling = false
		}
	}
}

func (s *Scaler) Ready() chan struct{} {
	return s.ready
}
