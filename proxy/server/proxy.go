package server

import (
	"fmt"
	"net"
	"strconv"
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
}

// initial lambda group
func New() *Proxy {
	p := &Proxy{
		log: global.GetLogger("Proxy: "),
	}
	switch config.Cluster {
	case config.StaticCluster:
		p.cluster = cluster.NewStaticCluster(config.NumLambdaClusters)
	default:
		p.cluster = cluster.NewMovingWindow()
	}
	p.placer = p.cluster.GetPlacer()

	// first group init
	err := p.cluster.Start()
	if err != nil {
		p.log.Error("Failed to start cluster: %v", err)
	}

	lambdastore.CM = p.cluster

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

	prepared := p.placer.NewMeta(
		key, size, int(dataChunks), int(parityChunks), int(dChunkId), int64(bodyStream.Len()), uint64(lambdaId), int(randBase))
	chunkKey := prepared.ChunkKey(int(dChunkId))
	req := &types.Request{
		Id:             types.Id{ReqId: reqId, ChunkId: chunkId},
		InsId:          uint64(lambdaId),
		Cmd:            protocol.CMD_SET,
		Key:            chunkKey,
		BodyStream:     bodyStream,
		Client:         client,
		CollectorEntry: collectEntry,
		Info:           prepared,
	}

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	meta, postProcess, err := p.placer.InsertAndPlace(key, prepared, req)
	if err != nil {
		w.AppendError(err.Error())
		w.Flush()
		return
	} else if meta.Deleted {
		// Object may be evicted in some cases:
		// 1: Some chunks were set.
		// 2: Placer evicted this object (unlikely).
		// 3: We got evicted meta.
		w.AppendErrorf("KEY %s not set to lambda store, may got evicted before all chunks are set.", chunkKey)
		w.Flush()
		return
	}

	if postProcess != nil {
		postProcess(p.dropEvicted)
		// continue
	}
	p.log.Debug("Requested to set %s to node %d", chunkKey, meta.Placement[dChunkId])
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
	// Update counter
	counter.Requests[dChunkId] = req

	// Send request to lambda channel
	if counter.IsFulfilled() {
		// Unlikely, just to be safe
		p.log.Debug("late request %v", reqId)
		status := counter.AddReturned(int(dChunkId))
		req.Abandon()
		counter.ReleaseIfAllReturned(status)
		return
	}

	// Validate the status of the instance
	instance := p.cluster.Instance(uint64(lambdaDest))
	if instance == nil || instance.IsReclaimed() || instance.Dispatch(req) != nil {
		// In both case, the instance can be closed, try relocate.
		_, err := p.relocate(req, meta, int(dChunkId), chunkKey, fmt.Sprintf("Instance(%d) closed.", lambdaDest))
		if err != nil {
			// If relocating still fails, abandon.
			w.AppendNil()
			w.Flush()
			p.log.Warn("Failed to request %s: %v", chunkKey, err)
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
				meta.Placement[rsp.Id.Chunk()] = wrapper.Request.InsId
				p.log.Debug("Relocated %v to %d.", wrapper.Request.Key, wrapper.Request.InsId)
			}
		}

		// Async logic
		if wrapper.Request.Cmd == protocol.CMD_GET {
			// Build control command
			recoverReqId := uuid.New().String()
			control := &types.Control{
				Cmd: protocol.CMD_RECOVER,
				Request: &types.Request{
					Id: types.Id{ReqId: recoverReqId, ChunkId: wrapper.Request.Id.ChunkId},
					// InsId:      wrapper.Request,      // Leave empty, the cluster will set it on relocating.
					Cmd:        protocol.CMD_RECOVER,
					RetCommand: protocol.CMD_RECOVER,
					BodySize:   wrapper.Request.BodySize,
					Key:        wrapper.Request.Key,
					Info:       wrapper.Request.Info,
					Changes:    types.CHANGE_PLACEMENT,
				},
				Callback: p.handleRecoverCallback,
			}

			// random select whether current chunk need to be refresh, if > hard limit, do refresh.
			instance, triggered, err := p.cluster.TryRelocate(wrapper.Request.Info, wrapper.Request.Id.Chunk(), control)
			if !triggered {
				// pass
			} else if err != nil {
				p.log.Debug("Relocation triggered. Failed to relocate %s(%d): %v", wrapper.Request.Key, p.getPlacementFromRequest(wrapper.Request), err)
			} else {
				global.ReqCoordinator.RegisterControl(recoverReqId, control)
				p.log.Debug("Relocation triggered. Relocating %s(%d) to %d", wrapper.Request.Key, p.getPlacementFromRequest(wrapper.Request), instance.Id())
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
	ctrl.Info.(*metastore.Meta).Placement[ctrl.Request.Id.Chunk()] = instance.Id()
	p.log.Debug("async updated instance %v", int(instance.Id()))
}

func (p *Proxy) dropEvicted(meta *metastore.Meta) {
	reqId := uuid.New().String()
	for i, lambdaId := range meta.Placement {
		if instance := p.cluster.Instance(uint64(lambdaId)); instance != nil {
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

func (p *Proxy) getPlacementFromRequest(req *types.Request) uint64 {
	return req.Info.(*metastore.Meta).Placement[req.Id.Chunk()]
}

func (p *Proxy) relocate(req *types.Request, meta *metastore.Meta, chunk int, key string, reason string) (*lambdastore.Instance, error) {
	instance, err := p.cluster.Relocate(meta, chunk, req.ToRecover())
	if err == nil {
		p.log.Debug("%s Requesting to relocate and recover %s: %d", reason, key, instance.Id())
	}
	return instance, err
}
