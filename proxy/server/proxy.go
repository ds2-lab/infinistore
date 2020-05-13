package server

import (
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
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type Proxy struct {
	log logger.ILogger
	//group        *Group
	movingWindow *MovingWindow
	placer       *Placer

	initialized int32
	ready       sync.WaitGroup
}

// initial lambda group
func New(replica bool) *Proxy {
	p := &Proxy{
		log: &logger.ColorLogger{
			Prefix: "Proxy ",
			Level:  global.Log.GetLevel(),
			Color:  !global.Options.NoColor,
		},
		//group:        group,
		movingWindow: NewMovingWindow(10, 1),
		placer:       NewPlacer(NewMataStore()),
	}

	p.placer.proxy = p
	p.movingWindow.placer = p.placer
	// first group init
	p.movingWindow.start()

	// start moving-window and auto-scaling Daemon
	go p.movingWindow.Daemon()

	lambdastore.Registry = p.movingWindow

	return p
}

func (p *Proxy) GetStatsProvider() types.GroupedClusterStats {
	return p.movingWindow
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
	//p.ready.Wait()
	p.movingWindow.waitReady()
	p.log.Info("[Proxy is ready]")
}

func (p *Proxy) Close(lis net.Listener) {
	lis.Close()
}

func (p *Proxy) Release() {
	for i, node := range p.movingWindow.group.All {
		scheduler.Recycle(node.LambdaDeployment)
		p.movingWindow.group.All[i] = nil
	}
	scheduler.Clear(p.movingWindow.group)
}

// from client
func (p *Proxy) HandleSet(w resp.ResponseWriter, c *resp.CommandStream) {
	client := redeo.GetClient(c.Context())
	connId := int(client.ID())

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
	if err := collector.Collect(collector.LogStart, protocol.CMD_SET, reqId, chunkId, time.Now().UnixNano()); err != nil {
		p.log.Warn("Fail to record start of request: %v", err)
	}

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	prepared := p.placer.NewMeta(
		key, size, dataChunks, parityChunks, dChunkId, int64(bodyStream.Len()), int(lambdaId), int(randBase))

	meta, _ := p.placer.GetOrInsert(key, prepared)

	p.log.Debug("chunkId id is %v, placement is %v", chunkId, meta.Placement)
	//if meta.Deleted {
	//	// Object may be evicted in some cases:
	//	// 1: Some chunks were set.
	//	// 2: Placer evicted this object (unlikely).
	//	// 3: We got evicted meta.
	//	p.log.Warn("KEY %s@%s not set to lambda store, may got evicted before all chunks are set.", chunkId, key)
	//	w.AppendErrorf("KEY %s@%s not set to lambda store, may got evicted before all chunks are set.", chunkId, key)
	//	w.Flush()
	//	return
	//}
	//if postProcess != nil {
	//	postProcess(p.dropEvicted)
	//}
	chunkKey := meta.ChunkKey(int(dChunkId))
	lambdaDest := meta.Placement[dChunkId]

	// Send chunk to the corresponding lambda instance in group
	p.log.Debug("Requesting to set %s: %d", chunkKey, lambdaDest)
	p.movingWindow.group.Instance(lambdaDest).C() <- &types.Request{
		Id:              types.Id{ConnId: connId, ReqId: reqId, ChunkId: chunkId},
		InsId:           uint64(lambdaDest),
		Cmd:             protocol.CMD_SET,
		Key:             chunkKey,
		BodyStream:      bodyStream,
		Client:          client,
		EnableCollector: true,
		Obj:             meta,
	}
	// p.log.Debug("KEY is", key.String(), "IN SET UPDATE, reqId is", reqId, "connId is", connId, "chunkId is", chunkId, "lambdaStore Id is", lambdaId)
	temp, _ := p.placer.Get(key, int(dChunkId))
	p.log.Debug("get test placement is %v", temp.Placement)
}

func (p *Proxy) HandleGet(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())
	connId := int(client.ID())
	key := c.Arg(0).String()
	reqId := c.Arg(1).String()
	dChunkId, _ := c.Arg(2).Int()
	chunkId := strconv.FormatInt(dChunkId, 10)

	// Start couting time.
	if err := collector.Collect(collector.LogStart, protocol.CMD_GET, reqId, chunkId, time.Now().UnixNano()); err != nil {
		p.log.Warn("Fail to record start of request: %v", err)
	}

	// key is "key"+"chunkId"
	//meta, ok := p.metaStore.Get(key, int(dChunkId))
	//if !ok || meta.Deleted {
	//	// Object may be deleted.
	//	p.log.Warn("KEY %s@%s not found in lambda store, please set first.", chunkId, key)
	//	w.AppendErrorf("KEY %s@%s not found in lambda store, please set first.", chunkId, key)
	//	w.Flush()
	//	return
	//}

	meta, ok := p.placer.Get(key, int(dChunkId))
	p.log.Debug("ok ? %v", ok)
	if !ok {
		p.log.Warn("KEY %s@%s not found in lambda store, please set first.", chunkId, key)
		w.AppendErrorf("KEY %s@%s not found in lambda store, please set first.", chunkId, key)
		w.Flush()
		return
	}
	lambdaDest := meta.Placement[dChunkId]
	counter := global.ReqCoordinator.Register(reqId, protocol.CMD_GET, meta.DChunks, meta.PChunks)
	instance := p.movingWindow.group.Instance(lambdaDest)

	p.log.Debug("chunkId is %v, lambdaDest is %v, instance id is %v", dChunkId, lambdaDest, instance.Name())

	chunkKey := meta.ChunkKey(int(dChunkId))
	// Send request to lambda channel
	p.log.Debug("Requesting to get %s: %d", chunkKey, lambdaDest)

	req := &types.Request{
		Id:              types.Id{ConnId: connId, ReqId: reqId, ChunkId: chunkId},
		InsId:           uint64(lambdaDest),
		Cmd:             protocol.CMD_GET,
		Key:             chunkKey,
		Client:          client,
		EnableCollector: true,
		Obj:             meta,
	}
	counter.Requests[dChunkId] = req

	// Unlikely, just to be safe
	if counter.IsFulfilled() || instance.IsReclaimed() {
		p.log.Debug("late request %v", reqId)
		status := counter.AddReturned(int(dChunkId))
		req.Abandon()
		counter.ReleaseIfAllReturned(status)
	} else {
		instance.C() <- req
	}
}

func (p *Proxy) HandleCallback(w resp.ResponseWriter, r interface{}) {
	wrapper := r.(*types.ProxyResponse)
	switch rsp := wrapper.Response.(type) {
	case *types.Response:
		t := time.Now()
		rsp.PrepareFor(w)
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
		if wrapper.Request.EnableCollector {
			err := collector.Collect(collector.LogServer2Client, rsp.Cmd, rsp.Id.ReqId, rsp.Id.ChunkId, int64(tgg.Sub(t)), int64(d1), int64(d2), tgg.UnixNano())
			if err != nil {
				p.log.Warn("LogServer2Client err %v", err)
			}
		}
	// Use more general way to deal error
	default:
		w.AppendErrorf("%v", rsp)
		w.Flush()
	}

	// Async logic
	if wrapper.Request.Cmd == protocol.CMD_GET {
		// random select whether current chunk need to be refresh, if > hard limit, do refresh.
		instance, ok := p.movingWindow.Refresh(wrapper.Request.Obj.(*Meta), wrapper.Request.Id.Chunk())
		if !ok {
			return
		}
		recoverReqId := uuid.New().String()

		//p.movingWindow.group.Instance(int(instanceId)).C() <- &types.Control{
		instance.C() <- &types.Control{
			Cmd: protocol.CMD_RECOVER,
			Request: &types.Request{
				Id:    types.Id{ConnId: wrapper.Request.Id.ConnId, ReqId: recoverReqId, ChunkId: wrapper.Request.Id.ChunkId,},
				InsId: instance.Id(),
				Cmd:   protocol.CMD_RECOVER,
				Key:   wrapper.Request.Key,
			},
			Callback: p.handleRecoverCallback,
		}
	}
}

func (p *Proxy) CollectData() {
	// for _, ins := range p.movingWindow.group.All {
	// 	p.log.Debug("active instance in proxy %v", ins.Name())
	// }

	for i, _ := range p.movingWindow.group.All {
		// send data command
		p.movingWindow.group.Instance(i).CollectData()
	}
	p.log.Info("Waiting data from Lambda")
	global.DataCollected.Wait()
	if err := collector.Flush(); err != nil {
		p.log.Error("Failed to save data from lambdas: %v", err)
	} else {
		p.log.Info("Data collected.")
	}
}

func (p *Proxy) handleRecoverCallback(ctrl *types.Control, arg interface{}) {
	instance := arg.(*lambdastore.Instance)
	ctrl.Obj.(*Meta).Placement[ctrl.Request.Id.Chunk()] = int(instance.Id())
}

func (p *Proxy) dropEvicted(meta *Meta) {
	reqId := uuid.New().String()
	for i, lambdaId := range meta.Placement {
		instance := p.movingWindow.group.Instance(lambdaId)
		instance.C() <- &types.Request{
			Id:    types.Id{ConnId: 0, ReqId: reqId, ChunkId: strconv.Itoa(i)},
			InsId: uint64(lambdaId),
			Cmd:   protocol.CMD_DEL,
			Key:   meta.ChunkKey(i),
		}
	}
}
