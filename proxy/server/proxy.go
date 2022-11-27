package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	"github.com/mason-leap-lab/infinicache/common/redeo/server"
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
	switch global.Options.GetClusterType() {
	case config.StaticCluster:
		p.cluster = cluster.NewStaticCluster(global.Options.GetNumFunctions())
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

// HandleSetChunk is the handler for "set chunk".
func (p *Proxy) HandleSetChunk(w resp.ResponseWriter, c *resp.CommandStream) {
	client := redeo.GetClient(c.Context())

	// Get args
	seq, _ := c.NextArg().Int()
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

	p.log.Debug("HandleSet %s: %d@%s", reqId, dChunkId, key)

	// Start counting time.
	collectEntry, _ := collector.CollectRequest(collector.LogRequestStart, nil, protocol.CMD_SET, reqId, chunkId, time.Now().UnixNano())

	prepared := p.placer.NewMeta(reqId,
		key, size, int(dataChunks), int(parityChunks), int(dChunkId), int64(bodyStream.Len()), uint64(lambdaId), int(randBase))
	prepared.SetTimout(protocol.GetBodyTimeout(bodyStream.Len())) // Set timeout for the operation to be considered as failed.
	// Added by Tianium: 20221102
	// We need the counter to figure out when the object is fully stored.
	counter := global.ReqCoordinator.Register(reqId, protocol.CMD_SET, prepared.DChunks, prepared.PChunks, nil)

	// Updated by Tianium: 20221102
	// req.Key will not be set until we get meta and have information about the version.
	req := types.GetRequest(client)
	req.Seq = seq
	req.Id = types.Id{ReqId: reqId, ChunkId: chunkId}
	req.InsId = uint64(lambdaId)
	req.Cmd = protocol.CMD_SET
	req.BodyStream = bodyStream
	req.CollectorEntry = collectEntry
	req.Info = prepared
	// Added by Tianium: 20221102
	// Add counter support.
	req.Cleanup = counter // Set cleanup so the counter can always be released.
	counter.Requests[dChunkId] = req

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	meta, postProcess, err := p.placer.InsertAndPlace(key, prepared, req)
	if err != nil && err == metastore.ErrConcurrentCreation {
		// Later concurrent setting will be automatically abandoned and return the same result as the earlier one.
		bodyStream.(resp.Holdable).Unhold() // bodyStream now will automatically be closed.
		meta.Wait()
		if meta.IsValid() {
			rsp := &types.Response{Cmd: protocol.CMD_SET, Id: req.Id, Body: []byte(strconv.FormatUint(meta.Placement[dChunkId], 10))}
			req.SetResponse(rsp)
		} else {
			req.SetResponse(types.ErrMaxAttemptsReached)
		}
		return
	} else if err != nil {
		server.NewErrorResponse(w, seq, err.Error()).Flush()
		return
	} else if meta.IsDeleted() {
		// Object may be deleted during PUT in a rare case in cache mode (COS is disabled) such as:
		// T1: Some chunks are set.
		// T2: The placer decides to evict this object (in rare case) by DELETE it.
		// T3: We get a deleted meta.
		server.NewErrorResponse(w, seq, "KEY %s not set to lambda store, may got evicted before all chunks are set.", meta.ChunkKey(int(dChunkId))).Flush()
		return
	}

	if postProcess != nil {
		postProcess(p.dropEvicted)
		// continue
	}
}

// HandleGetChunk is the handler for "get chunk".
func (p *Proxy) HandleGetChunk(w resp.ResponseWriter, c *resp.Command) {
	// Response with pong to confirm the preflight test.
	// w.AppendBulkString(protocol.CMD_PONG)
	// if err := w.Flush(); err != nil {
	// 	// Network error, abandon request
	// 	return
	// }

	client := redeo.GetClient(c.Context())
	var i util.Int
	seq, _ := c.Arg(i.Int()).Int()
	key := c.Arg(i.Add1()).String()
	reqId := c.Arg(i.Add1()).String()
	dChunkId, _ := c.Arg(i.Add1()).Int()
	chunkId := strconv.FormatInt(dChunkId, 10)

	// Start couting time.
	collectorEntry, _ := collector.CollectRequest(collector.LogRequestStart, nil, protocol.CMD_GET, reqId, chunkId, time.Now().UnixNano())

	// key is "key"+"chunkId"
	meta, ok := p.placer.Get(key, int(dChunkId))
	if !ok {
		p.log.Warn("KEY %s@%s not found", chunkId, key)
		server.NewNilResponse(w, seq).Flush()
		return
	}

	// Validate the version of meta matches.
	counter := global.ReqCoordinator.Register(reqId, protocol.CMD_GET, meta.DChunks, meta.PChunks, meta)
	if counter.Meta.(*metastore.Meta).Version() != meta.Version() {
		meta = counter.Meta.(*metastore.Meta)
	}

	// Validate if the chunk id is still valid for the returned meta.
	if dChunkId >= int64(meta.NumChunks()) {
		server.NewNilResponse(w, seq).Flush()
		return
	}

	lambdaDest := meta.Placement[dChunkId]
	chunkKey := meta.ChunkKey(int(dChunkId))
	req := types.GetRequest(client)
	req.Seq = seq
	req.Id = types.Id{ReqId: reqId, ChunkId: chunkId}
	req.InsId = uint64(lambdaDest)
	req.Cmd = protocol.CMD_GET
	req.BodySize = meta.ChunkSize
	req.Key = chunkKey
	req.CollectorEntry = collectorEntry
	req.Info = meta
	req.Cleanup = counter
	// Update counter
	counter.Requests[dChunkId] = req

	p.log.Debug("HandleGet %s: %s from %d", reqId, chunkKey, lambdaDest)

	// Validate the status of meta. If evicted, replace. All chunks will be replaced, so fulfill shortcut is not applicable here.
	// Not all placers support eviction.
	if meta.IsDeleted() {
		// Unlikely, just to be safe
		p.log.Debug("replace evicted chunk %s", chunkKey)

		_, postProcess, err := p.placer.Place(meta, int(dChunkId), req.ToRecover())
		if err != nil {
			p.log.Warn("Failed to replace %v: %v", req.Id, err)
			req.SetResponse(err)
			return
		}
		if postProcess != nil {
			postProcess(p.dropEvicted)
		}
		return
	}

	// Check late chunk request.
	if counter.IsFulfilled() {
		// Unlikely, just to be safe
		p.log.Debug("late request %v", reqId)
		req.Abandon() // counter will be released on abandoning (req.Cleanup set).
		return
	}

	// Send request to lambda channel

	// Validate the status of the instance
	instance := p.cluster.Instance(uint64(lambdaDest))
	var err error
	// No long we care if instance is reclaimed or not. Reclaimed instance will be delegated.
	if instance == nil {
		err = lambdastore.ErrInstanceClosed
	} else {
		// If reclaimed, instance will try delegate and relocate chunk concurrently, return ErrRelocationFailed if failed.
		err = p.placer.Dispatch(instance, req)
	}
	if err != nil && err != lambdastore.ErrQueueTimeout && err != lambdastore.ErrRelocationFailed {
		// In some cases, the instance doesn't try relocating, relocate the chunk as failover.
		req.Option = 0
		_, err = p.relocate(req, meta, int(dChunkId), chunkKey, fmt.Sprintf("Instance(%d) failed: %v", lambdaDest, err))
	}
	if err != nil {
		p.log.Warn("Failed to dispatch %v: %v", req.Id, err)
		req.SetResponse(err)
	}
}

// HandleCallback callback handler
func (p *Proxy) HandleCallback(w resp.ResponseWriter, r interface{}) {
	wrapper := r.(*types.ProxyResponse)
	client := redeo.GetClient(wrapper.Context())

	switch rsp := wrapper.Response.(type) {
	case *types.Response:
		t := time.Now()
		switch wrapper.Request.Cmd {
		case protocol.CMD_RECOVER:
			// on GET request from reclaimed instances, it will get recovered from new instances,
			// the response of this cmd_recover's behavior is the same as cmd_get
			fallthrough
		case protocol.CMD_GET:
			rsp.Size = strconv.FormatInt(wrapper.Request.Info.(*metastore.Meta).Size, 10)
			rsp.PrepareForGet(w, wrapper.Request.Seq)
		case protocol.CMD_SET:
			rsp.PrepareForSet(w, wrapper.Request.Seq)
			// Added by Tianium 20221102
			// Confirm the meta
			if wrapper.Request.AllSucceeded {
				wrapper.Request.Info.(*metastore.Meta).ConfirmCreated()
			}
		default:
			rsp := server.NewErrorResponse(w, wrapper.Request.Seq, "unable to respond unsupport command %s", wrapper.Request.Cmd)
			if err := rsp.Flush(); err != nil {
				client.Conn().Close()
			}
			return
		}
		d1 := time.Since(t)
		t2 := time.Now()
		// flush buffer, return on errors
		if err := rsp.Flush(); err != nil {
			client.Conn().Close()
			if err != context.Canceled {
				p.log.Warn("Error on flush response %v: %v", rsp, err)
			} else {
				p.log.Debug("Abandon flushing %v", rsp)
			}
			return
		}

		d2 := time.Since(t2)
		//p.log.Debug("Server AppendInt time is", time0,
		//	"AppendBulk time is", time1,
		//	"Server Flush time is", time2,
		//	"Chunk body len is ", len(rsp.Body))
		tgg := time.Now()
		if _, err := collector.CollectRequest(collector.LogRequestProxyResponse, wrapper.Request.CollectorEntry,
			int64(tgg.Sub(t)), int64(d1), int64(d2), tgg.UnixNano()); err != nil {
			p.log.Warn("LogRequestProxyResponse err %v", err)
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
			}

			// random select whether current chunk need to be refresh, if > hard limit, do refresh.
			instance, triggered, err := p.cluster.TryRelocate(wrapper.Request.Info, wrapper.Request.Id.Chunk(), control)
			if !triggered {
				// pass
			} else if err != nil {
				p.log.Debug("Relocation triggered. Failed to relocate %s(%d): %v", wrapper.Request.Key, p.getPlacementFromRequest(wrapper.Request), err)
			} else {
				p.log.Debug("Relocation triggered. Relocating %s(%d) to %d", wrapper.Request.Key, p.getPlacementFromRequest(wrapper.Request), instance.Id())
			}
		}
		// Use more general way to deal error
	default:
		collector.CollectRequest(collector.LogRequestAbandon, wrapper.Request.CollectorEntry)
		r := server.NewErrorResponse(w, wrapper.Request.Seq, "%v", rsp)
		// Added by Tianium 20221102
		// Fail the meta
		if wrapper.Request.Cmd == protocol.CMD_SET {
			wrapper.Request.Info.(*metastore.Meta).Invalidate()
		}
		if err := r.Flush(); err != nil {
			client.Conn().Close()
			return
		}
	}
}

// CollectData Trigger data collection.
func (p *Proxy) CollectData() {
	p.cluster.CollectData()
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
