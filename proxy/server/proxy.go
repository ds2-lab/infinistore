package server

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/cluster"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type Proxy struct {
	log     logger.ILogger
	cluster cluster.Cluster
	placer  metastore.Placer

	ready sync.WaitGroup
}

// initial lambda group
func New(replica bool) *Proxy {
	p := &Proxy{
		log: global.GetLogger("Proxy: "),
	}
	switch config.Cluster {
	case config.StaticCluster:
		p.cluster = cluster.NewStaticCluster(config.NumLambdaClusters)
	default:
		p.cluster = cluster.NewMovingWindow(10, 1)
	}
	p.placer = p.cluster.GetPlacer()

	// first group init
	err := p.cluster.Start()
	if err != nil {
		p.log.Error("Failed to start cluster: %v", err)
	}

	lambdastore.IM = p.cluster

	return p
}

func (p *Proxy) GetStatsProvider() interface{} {
	return p.cluster
}

func (p *Proxy) Serve(lis net.Listener) {
	for {
		cn, err := lis.Accept()
		if err != nil {
			return
		}

		conn := lambdastore.NewConnection(cn)
		go conn.ServeLambda()
	}
}

func (p *Proxy) WaitReady() {
	p.cluster.WaitReady()
	p.log.Info("[Proxy is ready]")
}

func (p *Proxy) Close(lis net.Listener) {
	lis.Close()
}

func (p *Proxy) Release() {
	p.cluster.Close()
	cluster.CleanUpPool()
}

// HandleSet "set chunk" handler
func (p *Proxy) HandleSet(w resp.ResponseWriter, c *resp.CommandStream) {
	client := redeo.GetClient(c.Context())

	// Get args
	key, _ := c.NextArg().String()
	reqId, _ := c.NextArg().String()
	size, _ := c.NextArg().Int()
	dChunkId, _ := c.NextArg().Int()
	chunkId := strconv.FormatInt(dChunkId, 10)
	dataChunks, _ := c.NextArg().Int()
	parityChunks, _ := c.NextArg().Int()
	lambdaId, _ := c.NextArg().Int()
	randBase, _ := c.NextArg().Int()

	bodyStream, err := c.Next()
	if err != nil {
		p.log.Error("Error on get value reader: %v", err)
		return
	}
	bodyStream.(resp.Holdable).Hold() // Hold to prevent being closed

	// Start counting time.
	collectEntry, _ := collector.CollectRequest(collector.LogStart, nil, protocol.CMD_SET, reqId, chunkId, time.Now().UnixNano())

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	prepared := p.placer.NewMeta(
		key, size, dataChunks, parityChunks, dChunkId, int64(bodyStream.Len()), int(lambdaId), int(randBase))

	meta, _, postProcess := p.placer.GetOrInsert(key, prepared)
	if meta.Deleted {
		// Object may be evicted in some cases:
		// 1: Some chunks were set.
		// 2: Placer evicted this object (unlikely).
		// 3: We got evicted meta.
		w.AppendErrorf("KEY %s@%s not set to lambda store, may got evicted before all chunks are set.", chunkId, key)
		w.Flush()
		return
	}
	if postProcess != nil {
		postProcess(p.dropEvicted)
	}
	chunkKey := meta.ChunkKey(int(dChunkId))
	lambdaDest := meta.Placement[dChunkId]

	p.log.Debug("chunkId id is %v, placement is %v", chunkId, meta.Placement)

	// Send chunk to the corresponding lambda instance in group
	p.log.Debug("Requesting to set %s: %d", chunkKey, lambdaDest)
	instance, _ := p.cluster.Instance(uint64(lambdaDest))
	if err := instance.Dispatch(&types.Request{
		Id:             types.Id{ReqId: reqId, ChunkId: chunkId},
		InsId:          uint64(lambdaDest),
		Cmd:            protocol.CMD_SET,
		Key:            chunkKey,
		BodyStream:     bodyStream,
		Client:         client,
		CollectorEntry: collectEntry,
		Info:           meta,
	}); err != nil {
		w.AppendErrorf("%v", err)
		w.Flush()
		return
	}
	// p.log.Debug("KEY is", key.String(), "IN SET UPDATE, reqId is ", reqId, ", chunkId is ", chunkId, ", lambdaStore Id is", lambdaId)
	// temp, _ := p.placer.Get(key, int(dChunkId))
	// p.log.Debug("get test placement is %v", temp.Placement)
}

// HandleGet "get chunk" handler
func (p *Proxy) HandleGet(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())
	key := c.Arg(0).String()
	reqId := c.Arg(1).String()
	dChunkId, _ := c.Arg(2).Int()
	chunkId := strconv.FormatInt(dChunkId, 10)

	// Start couting time.
	collectorEntry, _ := collector.CollectRequest(collector.LogStart, nil, protocol.CMD_GET, reqId, chunkId, time.Now().UnixNano())

	// key is "key"+"chunkId"
	meta, ok := p.placer.Get(key, int(dChunkId))
	if !ok {
		p.log.Warn("KEY %s@%s not found", chunkId, key)
		w.AppendNil()
		w.Flush()
		return
	}
	lambdaDest := meta.Placement[dChunkId]
	counter := global.ReqCoordinator.Register(reqId, protocol.CMD_GET, meta.DChunks, meta.PChunks)
	chunkKey := meta.ChunkKey(int(dChunkId))
	req := &types.Request{
		Id:             types.Id{ReqId: reqId, ChunkId: chunkId},
		InsId:          uint64(lambdaDest),
		Cmd:            protocol.CMD_GET,
		BodySize:       meta.ChunkSize,
		Key:            chunkKey,
		Client:         client,
		CollectorEntry: collectorEntry,
		Info:           meta,
	}

	// Validate the status of the instance
	instance, exists := p.cluster.Instance(uint64(lambdaDest))
	if exists {
		p.log.Debug("Requesting to get %s: %d", chunkKey, lambdaDest)
	} else if instance = p.relocate(req, meta, int(dChunkId), chunkKey, fmt.Sprintf("Invalid instance(%d).", lambdaDest)); instance == nil {
		// Failed to relocate.
		w.AppendNil()
		w.Flush()
		return
	}

	// Update counter
	counter.Requests[dChunkId] = req

	// Send request to lambda channel
	if counter.IsFulfilled() || instance.IsReclaimed() {
		// Unlikely, just to be safe
		p.log.Debug("late request %v", reqId)
		status := counter.AddReturned(int(dChunkId))
		req.Abandon()
		counter.ReleaseIfAllReturned(status)
		return
	}
	// Dispatch
	if err := instance.Dispatch(req); err != nil {
		// Err if instance is closed, relocate.
		if instance = p.relocate(req, meta, int(dChunkId), chunkKey, fmt.Sprintf("%s(%d).", err, lambdaDest)); instance == nil {
			// Failed to relocate.
			w.AppendNil()
			w.Flush()
		}

		if err := instance.Dispatch(req); err != nil {
			// This is unlikely, abandon
			w.AppendNil()
			w.Flush()
			p.log.Warn("%s(%d). Failed to request %s.", err, instance.Id(), chunkKey)
		}
	}
}

// HandleCallback callback handler
func (p *Proxy) HandleCallback(w resp.ResponseWriter, r interface{}) {
	wrapper := r.(*types.ProxyResponse)
	switch rsp := wrapper.Response.(type) {
	case *types.Response:
		t := time.Now()
		switch wrapper.Request.Cmd {
		case protocol.CMD_RECOVER:
			// on GET request from reclaimed instances, it will get recovered from new instances,
			// the response of this cmd_recover's behavior is the same as cmd_get
			fallthrough
		case protocol.CMD_GET:
			rsp.Size = wrapper.Request.Info.(*metastore.Meta).Size
			rsp.PrepareForGet(w)
		case protocol.CMD_SET:
			rsp.PrepareForSet(w)
		default:
			p.log.Error("Unsupport request on proxy reponse: %s", wrapper.Request.Cmd)
			return
		}
		d1 := time.Since(t)
		t2 := time.Now()
		// flush buffer, return on errors
		if err := rsp.Flush(); err != nil {
			p.log.Error("Error on flush response: %v", err)
			return
		}

		d2 := time.Since(t2)
		//p.log.Debug("Server AppendInt time is", time0,
		//	"AppendBulk time is", time1,
		//	"Server Flush time is", time2,
		//	"Chunk body len is ", len(rsp.Body))
		tgg := time.Now()
		if _, err := collector.CollectRequest(collector.LogServer2Client, wrapper.Request.CollectorEntry.(*collector.DataEntry), rsp.Cmd, rsp.Id.ReqId, rsp.Id.ChunkId,
			int64(tgg.Sub(t)), int64(d1), int64(d2), tgg.UnixNano()); err != nil {
			p.log.Warn("LogServer2Client err %v", err)
		}

		// update placement at reroute
		if wrapper.Request.Cmd == protocol.CMD_RECOVER {
			if wrapper.Request.Changes&types.CHANGE_PLACEMENT > 0 {
				meta := wrapper.Request.Info.(*metastore.Meta)
				meta.Placement[rsp.Id.Chunk()] = int(wrapper.Request.InsId)
				p.log.Debug("Relocated %v to %d.", wrapper.Request.Key, wrapper.Request.InsId)
			}
		}

		// Async logic
		if wrapper.Request.Cmd == protocol.CMD_GET {
			// random select whether current chunk need to be refresh, if > hard limit, do refresh.
			instance, ok := p.cluster.TryRelocate(wrapper.Request.Info, wrapper.Request.Id.Chunk())
			if !ok {
				return
			}
			recoverReqId := uuid.New().String()
			p.log.Debug("Relocation triggered. Relocating %s(%d) to %d", wrapper.Request.Key, p.getPlacementFromRequest(wrapper.Request), instance.Id())

			control := &types.Control{
				Cmd: protocol.CMD_RECOVER,
				Request: &types.Request{
					Id:         types.Id{ReqId: recoverReqId, ChunkId: wrapper.Request.Id.ChunkId},
					InsId:      instance.Id(),
					Cmd:        protocol.CMD_RECOVER,
					RetCommand: protocol.CMD_RECOVER,
					BodySize:   wrapper.Request.BodySize,
					Key:        wrapper.Request.Key,
					Info:       wrapper.Request.Info,
					Changes:    types.CHANGE_PLACEMENT,
				},
				Callback: p.handleRecoverCallback,
			}
			if err := instance.Dispatch(control); err == nil {
				global.ReqCoordinator.RegisterControl(recoverReqId, control)
			}
		}
		// Use more general way to deal error
	default:
		w.AppendErrorf("%v", rsp)
		w.Flush()
	}
}

// CollectData Trigger data collection.
func (p *Proxy) CollectData() {
	p.cluster.CollectData()
}

func (p *Proxy) handleRecoverCallback(ctrl *types.Control, arg interface{}) {
	instance := arg.(*lambdastore.Instance)
	ctrl.Info.(*metastore.Meta).Placement[ctrl.Request.Id.Chunk()] = int(instance.Id())
	p.log.Debug("async updated instance %v", int(instance.Id()))
}

func (p *Proxy) dropEvicted(meta *metastore.Meta) {
	reqId := uuid.New().String()
	for i, lambdaId := range meta.Placement {
		instance, exists := p.cluster.Instance(uint64(lambdaId))
		if exists {
			instance.Dispatch(&types.Request{
				Id:    types.Id{ReqId: reqId, ChunkId: strconv.Itoa(i)},
				InsId: uint64(lambdaId),
				Cmd:   protocol.CMD_DEL,
				Key:   meta.ChunkKey(i),
			})
		} // Or it has been expired.
	}
	p.log.Warn("Evict %s", meta.Key)
}

func (p *Proxy) getPlacementFromRequest(req *types.Request) int {
	return req.Info.(*metastore.Meta).Placement[req.Id.Chunk()]
}

func (p *Proxy) relocate(req *types.Request, meta *metastore.Meta, chunk int, key string, reason string) *lambdastore.Instance {
	instance := p.cluster.Relocate(meta, chunk)
	if instance == nil {
		p.log.Warn("%s Failed to relocate %s. This is unlikely, please check the cluster implementation.", reason, key)
	} else {
		// Update fields
		req.ToRecover(instance.Id())
		p.log.Debug("%s Requesting to relocate and recover %s: %d", reason, key, instance.Id())
	}
	return instance
}
