package server

import (
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

type Scaler struct {
	log     logger.ILogger
	placer  *LruPlacer
	Signal  chan struct{}
	ready   chan struct{}
	counter int32
}

func NewScaler(placer *LruPlacer) *Scaler {
	s := &Scaler{
		log: &logger.ColorLogger{
			Prefix: "Scaler ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		placer:  placer,
		Signal:  make(chan struct{}, 1),
		ready:   make(chan struct{}),
		counter: 0,
	}
	return s
}

// check the cluster usage information periodically
func (s *Scaler) Daemon() {
	for {
		s.log.Debug("in scaler Daemon, Group len is %v, active instance is %v", s.placer.group.Len(), ActiveInstance)
		t := time.NewTicker(INTERVAL)
		select {
		// receive scaling out signal
		case <-s.Signal:
			//TODO: receive scaling out signal, enlarge group capacity
			for i := ActiveInstance; i < ActiveInstance+STEP; i++ {
				name := LambdaPrefix
				s.log.Debug("[Scaling lambda instance %v%v]", name, i)

				node := scheduler.GetForGroup(s.placer.group, i, "out")
				node.Meta.Capacity = InstanceCapacity
				node.Meta.IncreaseSize(InstanceOverhead)

				go func() {
					node.WarmUp()
					//if atomic.AddInt32(&s.counter, 1) == STEP {
					//	s.log.Info("[scale out is ready]")
					//	close(s.ready)
					//}
				}()

				// Begin handle requests
				go node.HandleRequests()
				s.placer.Append(node)
			}

			// update current active lambda instance
			ActiveInstance += STEP

			// reset counter
			s.counter = 0

		case <-t.C:
			//TODO: periodically check storage capacity information
			// Responsible for scaling in phase
			s.log.Debug("current status is %v, len is %v", s.placer.AvgSize(), s.placer.group.Len())

		}
	}
}

func (s *Scaler) Ready() chan struct{} {
	return s.ready
}
