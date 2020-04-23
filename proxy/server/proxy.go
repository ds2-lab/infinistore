package server

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
	"github.com/wangaoone/LambdaObjectstore/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/collector"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
	"github.com/wangaoone/LambdaObjectstore/proxy/lambdastore"
	"github.com/wangaoone/LambdaObjectstore/proxy/types"
)

type Proxy struct {
	log          logger.ILogger
	group        *Group
	//groupAll     *Group
	movingWindow *MovingWindow
	placer       *Placer

	scaler *Scaler

	initialized int32
	ready       chan struct{}
}

// initial lambda group
func New(replica bool) *Proxy {
	group := NewGroup(NumLambdaClusters) // use Cluster number to initial group
	//groupAll := NewGroup(NumLambdaClusters) // use Cluster number to initial group
	p := &Proxy{
		log: &logger.ColorLogger{
			Prefix: "Proxy ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		group: group,
		//groupAll:     groupAll,
		movingWindow: NewMovingWindow(10, 1),
		placer:       NewPlacer(NewMataStore()),
		scaler:       NewScaler(),
		ready:        make(chan struct{}),
	}

	p.movingWindow.proxy = p
	p.placer.proxy = p
	p.scaler.proxy = p

	p.group = p.movingWindow.start(p.Ready())

	// start moving window rolling
	go p.movingWindow.Rolling()
	// start auto scaler
	go p.scaler.Daemon()

	return p
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

func (p *Proxy) Ready() chan struct{} {
	return p.ready
}

func (p *Proxy) Close(lis net.Listener) {
	lis.Close()
}

func (p *Proxy) Release() {
	for i, node := range p.group.All {
		scheduler.Recycle(node.LambdaDeployment)
		p.group.All[i] = nil
	}
	scheduler.Clear(p.group)
}

// from client
func (p *Proxy) HandleSet(w resp.ResponseWriter, c *resp.CommandStream) {
	client := redeo.GetClient(c.Context())
	connId := int(client.ID())

	// Get args
	key, _ := c.NextArg().String()
	dChunkId, _ := c.NextArg().Int()
	chunkId := strconv.FormatInt(dChunkId, 10)
	lambdaId, _ := c.NextArg().Int()
	randBase, _ := c.NextArg().Int()
	reqId, _ := c.NextArg().String()
	// _, _ = c.NextArg().Int()
	// _, _ = c.NextArg().Int()
	dataChunks, _ := c.NextArg().Int()
	parityChunks, _ := c.NextArg().Int()

	bodyStream, err := c.Next()
	if err != nil {
		p.log.Error("Error on get value reader: %v", err)
		return
	}
	bodyStream.(resp.Holdable).Hold()

	// Start counting time.
	if err := collector.Collect(collector.LogStart, "set", reqId, chunkId, time.Now().UnixNano()); err != nil {
		p.log.Warn("Fail to record start of request: %v", err)
	}

	// We don't use this for now
	// global.ReqMap.GetOrInsert(reqId, &types.ClientReqCounter{"set", int(dataChunks), int(parityChunks), 0})

	// Check if the chunk key(key + chunkId) exists, base of slice will only be calculated once.
	prepared := p.placer.NewMeta(
		key, int(randBase), int(dataChunks+parityChunks), int(dChunkId), int(lambdaId), bodyStream.Len())
	//meta, _, postProcess :=  p.metaStore.GetOrInsert(key, prepared)
	p.log.Debug("key is %v, lambdaId is %v", prepared.Key, lambdaId)
	// redirect to do load balance
	meta, _ := p.placer.GetOrInsert(key, prepared)
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
	p.group.Instance(lambdaDest).C() <- &types.Request{
		Id:              types.Id{connId, reqId, chunkId},
		Cmd:             strings.ToLower(c.Name),
		Key:             chunkKey,
		BodyStream:      bodyStream,
		ChanResponse:    client.Responses(),
		EnableCollector: true,
	}
	// p.log.Debug("KEY is", key.String(), "IN SET UPDATE, reqId is", reqId, "connId is", connId, "chunkId is", chunkId, "lambdaStore Id is", lambdaId)
}

func (p *Proxy) HandleGet(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())
	connId := int(client.ID())
	key := c.Arg(0).String()
	dChunkId, _ := c.Arg(1).Int()
	chunkId := strconv.FormatInt(dChunkId, 10)
	reqId := c.Arg(2).String()
	dataChunks, _ := c.Arg(3).Int()
	parityChunks, _ := c.Arg(4).Int()

	// Start counting time.
	if err := collector.Collect(collector.LogStart, "get", reqId, chunkId, time.Now().UnixNano()); err != nil {
		p.log.Warn("Fail to record start of request: %v", err)
	}

	global.ReqMap.GetOrInsert(reqId, &types.ClientReqCounter{"get", int(dataChunks), int(parityChunks), 0})

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
	chunkKey := meta.ChunkKey(int(dChunkId))
	lambdaDest := meta.Placement[dChunkId]

	// Send request to lambda channel
	p.log.Debug("Requesting to get %s: %d", chunkKey, lambdaDest)
	p.group.Instance(lambdaDest).C() <- &types.Request{
		Id:              types.Id{connId, reqId, chunkId},
		Cmd:             strings.ToLower(c.Name),
		Key:             chunkKey,
		ChanResponse:    client.Responses(),
		EnableCollector: true,
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
}

func (p *Proxy) CollectData() {

	for _, ins := range p.group.All {
		p.log.Debug("active instance in proxy ", ins.Name())
	}

	for i, _ := range p.group.All {
		global.DataCollected.Add(1)
		// send data command
		p.group.Instance(i).C() <- &types.Control{Cmd: "data"}
	}
	p.log.Info("Waiting data from Lambda")
	global.DataCollected.Wait()
	if err := collector.Flush(); err != nil {
		p.log.Error("Failed to save data from lambdas: %v", err)
	} else {
		p.log.Info("Data collected.")
	}
}

func (p *Proxy) dropEvicted(meta []*Meta) {
	for m := 0; m < len(meta); m++ {
		p.log.Debug("evict obj is %v", meta[m].Key)
		reqId := uuid.New().String()
		for i, lambdaId := range meta[m].Placement {
			instance := p.group.Instance(lambdaId)
			instance.C() <- &types.Request{
				Id:  types.Id{0, reqId, strconv.Itoa(i)},
				Cmd: "del",
				Key: meta[m].ChunkKey(i),
			}
		}
	}
}
