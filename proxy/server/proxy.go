package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	"github.com/mason-leap-lab/infinicache/common/redeo/server"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/cache"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/cluster"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type Proxy struct {
	log               logger.ILogger
	cluster           cluster.Cluster
	placer            metastore.Placer
	port              int // Starting listen port
	ports             int // Number of ports to listen
	listeners         []net.Listener
	roundRobinCounter uint64
	cache             types.PersistCache

	initListeners sync.WaitGroup
	done          sync.WaitGroup
}

// initial lambda group
func New() *Proxy {
	p := &Proxy{
		log:       global.GetLogger("Proxy: "),
		port:      global.BasePort + 1,
		ports:     global.LambdaServePorts,
		listeners: make([]net.Listener, global.LambdaServePorts),
	}

	p.Serve()
	p.initListeners.Wait()

	switch global.Options.GetClusterType() {
	case config.StaticCluster:
		p.cluster = cluster.NewStaticCluster(p, global.Options.GetNumFunctions())
	default:
		p.cluster = cluster.NewMovingWindow(p)
	}
	p.placer = p.cluster.GetPlacer()

	// Enable persist cache.
	if global.IsLocalCacheEnabled() {
		p.cache = cache.NewPersistCache()
		p.placer.RegisterHandler(metastore.PlacerEventBeforePlacing, p.beforePlacingHandler)
	}

	// Set CM before starting the cluster.
	lambdastore.CM = p.cluster

	// first group init
	err := p.cluster.Start()
	if err != nil {
		p.log.Error("Failed to start cluster: %v", err)
	}

	return p
}

func (p *Proxy) GetStatsProvider() interface{} {
	return p.cluster
}

func (p *Proxy) Serve() {
	p.log.Info("Initiating lambda servers")
	p.done.Add(p.ports)
	p.initListeners.Add(p.ports)
	for i := 0; i < p.ports; i++ {
		go p.serve(p.port+i, &p.done)
	}
}

func (p *Proxy) Wait() {
	p.done.Wait()
}

func (p *Proxy) serve(port int, done *sync.WaitGroup) {
	defer done.Done()

	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		p.log.Error("Failed to listen lambdas on %d: %v", port, err)
		p.initListeners.Done()
		return
	}
	p.listeners[port-p.port] = lis
	p.log.Info("Listening lambdas on %d", port)
	p.initListeners.Done()

	for {
		cn, err := lis.Accept()
		if err != nil {
			return
		}

		conn := lambdastore.NewConnection(cn)
		go conn.ServeLambda()
	}
}

// GetServePort implements cluster.ServerProvider interface. It round-robin selects a port to serve
func (p *Proxy) GetServePort(id uint64) int {
	if p.ports == 1 {
		return p.port
	}

	for failed := 0; failed < p.ports; failed++ {
		port := (atomic.AddUint64(&p.roundRobinCounter, 1) - 1) % uint64(p.ports)
		if p.listeners[port] != nil {
			return int(port) + p.port
		}
	}
	// If no ports is available, simply return a port.
	return 0
}

// GetPersistCache implements cluster.ServerProvider interface.
func (p *Proxy) GetPersistCache() types.PersistCache {
	return p.cache
}

// PersistCacheLen implements types.ServerStats interface.
func (p *Proxy) PersistCacheLen() int {
	return p.cache.Len()
}

func (p *Proxy) WaitReady() {
	p.cluster.WaitReady()
	p.log.Info("[Proxy is ready]")
}

func (p *Proxy) Close() {
	for i, lis := range p.listeners {
		if lis != nil {
			lis.Close()
			p.listeners[i] = nil
		}
	}
}

func (p *Proxy) Release() {
	if p.cache != nil {
		p.cache.Report()
	}
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

	p.log.Debug("HandleSet %s(%d): %d@%s", reqId, dChunkId, dChunkId, key)

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
	req.Id.ReqId = reqId
	req.Id.ChunkId = chunkId
	req.InsId = uint64(lambdaId)
	req.Cmd = protocol.CMD_SET
	req.BodyStream = bodyStream
	req.BodyStream.(resp.Holdable).Hold() // Hold to prevent being closed
	req.CollectorEntry = collectEntry
	req.Info = prepared
	// Added by Tianium: 20221102
	// Add counter support.
	req.RequestGroup = counter // Set cleanup so the counter can always be released.
	counter.Requests[dChunkId] = req
	bodyStream = nil // Don't use bodyStream anymore, req.BodyStream can be updated in InsertAndPlace().

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	meta, postProcess, err := p.placer.InsertAndPlace(key, prepared, req)
	if err != nil && err == metastore.ErrConcurrentCreation {
		// Later concurrent setting will be automatically abandoned and return the same result as the earlier one.
		req.BodyStream.(resp.Holdable).Unhold() // bodyStream now will automatically be closed.
		meta.Wait()
		if meta.IsValid() {
			rsp := &types.Response{Cmd: protocol.CMD_SET, Id: req.Id, Body: []byte(strconv.FormatUint(meta.Placement[dChunkId], 10))}
			req.SetResponse(rsp)
		} else {
			req.SetErrorResponse(types.ErrMaxAttemptsReached)
		}
		return
	} else if err != nil {
		req.BodyStream.(resp.Holdable).Unhold()
		server.NewErrorResponse(w, seq, err.Error()).Flush()
		return
	} else if meta.IsDeleted() {
		req.BodyStream.(resp.Holdable).Unhold()
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
	req.Id.ReqId = reqId
	req.Id.ChunkId = chunkId
	req.InsId = uint64(lambdaDest)
	req.Cmd = protocol.CMD_GET
	req.BodySize = meta.ChunkSize
	req.Key = chunkKey
	req.CollectorEntry = collectorEntry
	req.Info = meta
	req.RequestGroup = counter
	// Update counter
	counter.Requests[dChunkId] = req

	p.log.Debug("HandleGet %v(%d): %s from %d", reqId, dChunkId, chunkKey, lambdaDest)

	if p.cache != nil {
		// Query the persist cache.
		cached, first := p.cache.GetOrCreate(meta.ChunkKey(int(dChunkId)), meta.ChunkSize)
		if first {
			// Only the first of concurrent requests will be sent to lambda.
			p.log.Debug("Persisting %v to cache %s", &req.Id, cached.Key())
			req.PersistChunk = cached
		} else {
			go p.waitForCache(req, cached, counter)
			p.log.Debug("Serving %v from cache %s", &req.Id, cached.Key())
			return
		}
	}

	// Validate the status of meta. If evicted, replace. All chunks will be replaced, so fulfill shortcut is not applicable here.
	// Not all placers support eviction.
	// NOTE: Since no delete request is provided, delection will only happen in cache mode.
	if meta.IsDeleted() {
		// Unlikely, just to be safe
		p.log.Debug("replace evicted chunk %s", chunkKey)

		_, postProcess, err := p.placer.Place(meta, int(dChunkId), req.ToRecover())
		if err != nil {
			p.log.Warn("Failed to re-place %v: %v", &req.Id, err)
			req.SetErrorResponse(err)
			return
		}
		if postProcess != nil {
			postProcess(p.dropEvicted)
		}
		return
	}

	// Check late chunk request. Continue if persist chunk is available.
	if counter.IsFulfilled() && !req.MustRequest() {
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
		req.SetErrorResponse(err)
	}
}

// HandleCallback callback handler
func (p *Proxy) HandleCallback(w resp.ResponseWriter, r interface{}) {
	wrapper := r.(types.ProxyResponse)
	client := redeo.GetClient(wrapper.Context())

	switch rsp := wrapper.Response().(type) {
	case *types.Response:
		t := time.Now()
		switch wrapper.Request().Cmd {
		case protocol.CMD_RECOVER:
			// on GET request from reclaimed instances, it will get recovered from new instances,
			// the response of this cmd_recover's behavior is the same as cmd_get
			fallthrough
		case protocol.CMD_GET:
			rsp.Size = strconv.FormatInt(wrapper.Request().Info.(*metastore.Meta).Size, 10)
			rsp.PrepareForGet(w, wrapper.Request().Seq)
		case protocol.CMD_SET:
			rsp.PrepareForSet(w, wrapper.Request().Seq)
			// Added by Tianium 20221102
			// Confirm the meta
			if wrapper.Request().AllSucceeded {
				wrapper.Request().Info.(*metastore.Meta).ConfirmCreated()
			}
		default:
			rsp := server.NewErrorResponse(w, wrapper.Request().Seq, "unable to respond unsupport command %s", wrapper.Request().Cmd)
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
		} else {
			p.log.Debug("Flushed response %v", rsp)
		}

		d2 := time.Since(t2)
		//p.log.Debug("Server AppendInt time is", time0,
		//	"AppendBulk time is", time1,
		//	"Server Flush time is", time2,
		//	"Chunk body len is ", len(rsp.Body))
		tgg := time.Now()
		if _, err := collector.CollectRequest(collector.LogRequestProxyResponse, wrapper.Request().CollectorEntry,
			int64(tgg.Sub(t)), int64(d1), int64(d2), tgg.UnixNano()); err != nil {
			p.log.Warn("LogRequestProxyResponse err %v", err)
		}

		// Async logic
		if wrapper.Request().Cmd == protocol.CMD_GET {
			// Build control command
			recoverReqId := uuid.New().String()
			control := &types.Control{
				Cmd: protocol.CMD_RECOVER,
				Request: &types.Request{
					Id: types.Id{ReqId: recoverReqId, ChunkId: wrapper.Request().Id.ChunkId},
					// InsId:      wrapper.Request,      // Leave empty, the cluster will set it on relocating.
					Cmd:        protocol.CMD_RECOVER,
					RetCommand: protocol.CMD_RECOVER,
					BodySize:   wrapper.Request().BodySize,
					Key:        wrapper.Request().Key,
					Info:       wrapper.Request().Info,
					Changes:    types.CHANGE_PLACEMENT,
				},
			}

			// random select whether current chunk need to be refresh, if > hard limit, do refresh.
			instance, triggered, err := p.cluster.TryRelocate(wrapper.Request().Info, wrapper.Request().Id.Chunk(), control)
			if !triggered {
				// pass
			} else if err != nil {
				p.log.Debug("Relocation triggered. Failed to relocate %s(%d): %v", wrapper.Request().Key, p.getPlacementFromRequest(wrapper.Request()), err)
			} else {
				p.log.Debug("Relocation triggered. Relocating %s(%d) to %d", wrapper.Request().Key, p.getPlacementFromRequest(wrapper.Request()), instance.Id())
			}
		}
		// Use more general way to deal error
	default:
		collector.CollectRequest(collector.LogRequestAbandon, wrapper.Request().CollectorEntry)
		r := server.NewErrorResponse(w, wrapper.Request().Seq, "%v", rsp)
		// Added by Tianium 20221102
		// Fail the meta
		if wrapper.Request().Cmd == protocol.CMD_SET {
			wrapper.Request().Info.(*metastore.Meta).Invalidate()
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

func (p *Proxy) beforePlacingHandler(meta *metastore.Meta, chunkId int, cmd types.Command) {
	// Initiate persist cache
	req := cmd.(*types.Request)
	key := meta.ChunkKey(chunkId)
	pChunk, _ := p.cache.GetOrCreate(key, req.BodyStream.Len())
	p.log.Debug("Persisting %v to cache %s", &req.Id, pChunk.Key())
	bodyStream, err := pChunk.Store(req.BodyStream)
	if err != nil {
		p.log.Warn("Failed to initiate persist cache on setting %s: %v", key, err)
		return
	}
	req.BodyStream = bodyStream
	req.BodyStream.(resp.Holdable).Hold()
	req.PersistChunk = pChunk
	// Finish interception whatever.
	go bodyStream.Close()
}

func (p *Proxy) waitForCache(req *types.Request, cached types.PersistChunk, counter *global.RequestCounter) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cached.Load(ctx)
	if err != nil {
		req.SetErrorResponse(fmt.Errorf("failed to load from persist cache: %v", err))
		cancel()
		return
	}

	// Update counter
	counter.AddSucceeded(req.Id.Chunk(), false)

	// Prepare response
	rsp := req.ToCachedResponse()
	rsp.SetBodyStream(stream)
	stream.(resp.Holdable).Hold()
	defer rsp.Close()
	defer cancel()

	// Set response
	if err := req.SetResponse(rsp); err != nil && err != types.ErrResponded {
		p.log.Warn("Failed to set response on streaming cached %v: %v", &rsp.Id, err)
		stream.(resp.Holdable).Unhold()
		stream.Close()
		return
	}

	// Close will block until the stream is flushed.
	if err := rsp.WaitFlush(true); err == nil {
		p.log.Debug("Flushed streaming cached %v", &rsp.Id)
		status := counter.AddFlushed(req.Id.Chunk())
		if counter.IsFulfilled(status) && !counter.IsAllFlushed(status) {
			p.log.Debug("Request fulfilled: %v(%x), abandon unflushed chunks.", &rsp.Id, status)
			for _, req := range counter.Requests {
				if req != nil {
					req.Abandon()
				}
			}
		}
	} else if err != context.Canceled {
		p.log.Warn("Unexpected error on streaming %v: %v", &rsp.Id, err)
	} else {
		p.log.Debug("Abandoned streaming cached %v", &rsp.Id)
	}
}
