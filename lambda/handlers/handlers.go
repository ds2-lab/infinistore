package handlers

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/migrator"
	. "github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/types"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	log = Log
)

func TestHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	extension := lambdaLife.TICK_ERROR
	if session.Requests > 1 {
		extension = lambdaLife.TICK
	}
	defer session.Timeout.DoneBusyWithReset(extension, c.Name)

	log.Debug("In Test handler")

	rsp, _ := Server.AddResponsesWithPreparer(c.Name, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
	}, client)
	if err := rsp.Flush(); err != nil {
		log.Error("Error on test::flush: %v", err)
	}
}

func GetHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := lambdaLife.TICK_ERROR
	if session.Requests > 1 {
		extension = lambdaLife.TICK
	}
	defer Server.WaitAck(c.Name, func() {
		session.Timeout.DoneBusyWithReset(extension, c.Name)
	}, client)

	t := time.Now()
	log.Debug("In GET handler(link:%d)", worker.LinkFromClient(client).ID())

	reqId := c.Arg(0).String()
	// Skip: chunkId := c.Arg(1).String()
	key := c.Arg(2).String()

	var recovered int64
	chunkId, stream, ret := Store.GetStream(key)
	// Recover if not found. This is not desired if recovery is enabled and will generate a warning.
	if ret.Error() == types.ErrNotFound && Persist != nil {
		if Lineage != nil {
			log.Warn("Key not found while recovery is enabled: %v", key)
		}
		errRsp := &worker.ErrorResponse{}
		chunkId = c.Arg(1).String()
		sizeArg := c.Arg(3)
		if sizeArg == nil {
			errRsp.Error = errors.New("size must be set for trying recovery from persistent layer")
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
			}
			return
		}
		size, szErr := sizeArg.Int()
		if szErr != nil {
			errRsp.Error = szErr
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
			}
			return
		}
		ret = Persist.SetRecovery(key, chunkId, uint64(size))
		if ret.Error() != nil {
			errRsp.Error = ret.Error()
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
			}
			return
		}
		recovered = 1

		// Retry
		chunkId, stream, ret = Store.GetStream(key)
	}
	if stream != nil {
		defer stream.Close()
	}
	d1 := time.Since(t)

	if ret.Error() == nil {
		// construct lambda store response
		response := &worker.ObjectResponse{
			BaseResponse: worker.BaseResponse{
				Cmd:        c.Name,
				BodyStream: stream,
			},
			ReqId:     reqId,
			ChunkId:   chunkId,
			Recovered: recovered,
		}

		t2 := time.Now()
		Server.AddResponses(response, client)
		if err := response.Flush(); err != nil {
			log.Error("Error on flush(get key %s): %v", key, err)
			return
		}
		d2 := time.Since(t2)

		dt := time.Since(t)
		log.Debug("Get key:%s, chunk:%s, duration:%v, transmission:%v", key, chunkId, dt, d1)
		collector.AddRequest(t, types.OP_GET, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
	} else {
		var respError *ResponseError
		if ret.Error() == types.ErrNotFound {
			// Not found
			respError = NewResponseError(404, "Key not found %s: %v", key, ret.Error())
		} else {
			respError = NewResponseError(500, "Failed to get %s: %v", key, ret.Error())
		}
		errResponse := &worker.ErrorResponse{Error: respError}
		Server.AddResponses(errResponse, client)
		if err := errResponse.Flush(); err != nil {
			log.Error("Error on flush: %v", err)
		}
		collector.AddRequest(t, types.OP_GET, respError.Status(), reqId, "-1", 0, 0, time.Since(t), 0, session.Id)
	}
}

func SetHandler(w resp.ResponseWriter, c *resp.CommandStream) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := lambdaLife.TICK_ERROR
	if session.Requests > 1 {
		extension = lambdaLife.TICK
	}

	t := time.Now()
	log.Debug("In SET handler(link:%d)", worker.LinkFromClient(client).ID())

	var reqId, chunkId string
	cmd := c.Name
	finalize := func(ret *types.OpRet, ds ...time.Duration) {
		Server.WaitAck(c.Name, func() {
			if ret != nil && ret.IsDelayed() {
				ret.Wait()
				collector.AddRequest(t, types.OP_SET, "200", reqId, chunkId, ds[0], ds[1], ds[2], time.Since(t), session.Id)
			} else {
				// Only if error
				collector.AddRequest(t, types.OP_SET, "500", reqId, chunkId, 0, 0, time.Since(t), 0, session.Id)
			}
			session.Timeout.DoneBusyWithReset(extension, cmd)
		}, client)
	}

	errRsp := &worker.ErrorResponse{}
	reqId, _ = c.NextArg().String()
	chunkId, _ = c.NextArg().String()
	key, _ := c.NextArg().String()
	valReader, err := c.Next()
	if err != nil {
		errRsp.Error = NewResponseError(500, "Error on get value reader: %v", err)
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
		}
		finalize(nil)
		return
	}

	// Streaming set.
	client.Conn().SetReadDeadline(lambdaLife.GetStreamingDeadline(valReader.Len()))
	ret := Store.SetStream(key, chunkId, valReader)
	client.Conn().SetReadDeadline(time.Time{})
	d1 := time.Since(t)
	err = ret.Error()
	if err != nil {
		errRsp.Error = err
		log.Error("%v", err)
		Server.AddResponses(errRsp, client)

		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
			// Ignore, network error will be handled by redeo.
		}
		// If the setstream err is net error (timeout), cut the line.
		if util.IsConnectionFailed(err) {
			Server.SetFailure(client, err)
		}

		finalize(ret)
		return
	}

	// write Key, clientId, chunkId, body back to proxy
	response := &worker.ObjectResponse{
		BaseResponse: worker.BaseResponse{Cmd: c.Name},
		ReqId:        reqId,
		ChunkId:      chunkId,
	}

	t2 := time.Now()
	Server.AddResponses(response, client)
	if err := response.Flush(); err != nil {
		log.Error("Error on set::flush(set key %s): %v", key, err)
		// Ignore
	}
	d2 := time.Since(t2)

	dt := time.Since(t)
	log.Debug("Set key:%s, chunk: %s, duration:%v, transmission:%v", key, chunkId, dt, d1)
	finalize(ret, d1, d2, dt)
}

func RecoverHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := lambdaLife.TICK_ERROR
	if session.Requests > 1 {
		extension = lambdaLife.TICK
	}
	var ret *types.OpRet
	cmd := c.Name
	defer Server.WaitAck(c.Name, func() {
		if ret != nil && ret.IsDelayed() {
			ret.Wait()
		}
		session.Timeout.DoneBusyWithReset(extension, cmd)
	}, client)

	t := time.Now()
	log.Debug("In RECOVER handler(link:%d)", worker.LinkFromClient(client).ID())

	errRsp := &worker.ErrorResponse{}
	reqId := c.Arg(0).String()
	chunkId := c.Arg(1).String()
	key := c.Arg(2).String()
	retCmd := c.Arg(3).String()
	sizeArg := c.Arg(4)
	if sizeArg == nil {
		errRsp.Error = errors.New("size must be set")
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
		}
		return
	}

	size, szErr := sizeArg.Int()
	if szErr != nil {
		errRsp.Error = szErr
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
		}
		return
	}

	if Persist == nil {
		errRsp.Error = errors.New("recover is not supported")
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
		}
		return
	}

	// Recover.
	ret = Persist.SetRecovery(key, chunkId, uint64(size))
	if ret.Error() != nil {
		errRsp.Error = ret.Error()
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
		}
		return
	}

	log.Debug("Success to recover from persistent store, Key:%s, ChunkID: %s", key, chunkId)

	// Immediate get, unlikely to error, don't overwrite ret.
	var stream resp.AllReadCloser
	if retCmd == protocol.CMD_GET {
		_, stream, _ = Store.GetStream(key)
		if stream != nil {
			defer stream.Close()
		}
	}
	d1 := time.Since(t)

	// write Key, clientId, chunkId, body back to proxy
	response := &worker.ObjectResponse{
		BaseResponse: worker.BaseResponse{
			Cmd:        retCmd,
			BodyStream: stream,
		},
		ReqId:     reqId,
		ChunkId:   chunkId,
		Recovered: 1,
	}

	t2 := time.Now()
	Server.AddResponses(response, client)
	if err := response.Flush(); err != nil {
		log.Error("Error on recover::flush(recover key %s): %v", key, err)
		// Ignore
	}
	d2 := time.Since(t2)

	dt := time.Since(t)
	log.Debug("Recover complete, Key:%s, ChunkID: %s", key, chunkId)
	if retCmd == protocol.CMD_GET {
		collector.AddRequest(t, types.OP_RECOVER, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
	}
}

func DelHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := lambdaLife.TICK_ERROR
	if session.Requests > 1 {
		extension = lambdaLife.TICK
	}
	var ret *types.OpRet
	cmd := c.Name
	defer Server.WaitAck(c.Name, func() {
		if ret != nil && ret.IsDelayed() {
			ret.Wait()
		}
		session.Timeout.DoneBusyWithReset(extension, cmd)
	}, client)

	//t := time.Now()
	log.Debug("In Del Handler")

	reqId := c.Arg(0).String()
	chunkId := c.Arg(1).String()
	key := c.Arg(2).String()

	ret = Store.Del(key, chunkId)
	if ret.Error() == nil {
		// write Key, clientId, chunkId, body back to proxy
		response := &worker.ObjectResponse{
			BaseResponse: worker.BaseResponse{Cmd: c.Name},
			ReqId:        reqId,
			ChunkId:      chunkId,
		}
		Server.AddResponses(response, client)
		if err := response.Flush(); err != nil {
			log.Error("Error on del::flush(set key %s): %v", key, err)
			return
		}
	} else {
		var respError *ResponseError
		if ret.Error() == types.ErrNotFound {
			// Not found
			respError = NewResponseError(404, "Failed to del %s: %v", key, ret.Error())
		} else {
			respError = NewResponseError(500, "Failed to del %s: %v", key, ret.Error())
		}
		errResponse := &worker.ErrorResponse{Error: respError}
		Server.AddResponses(errResponse, client)
		if err := errResponse.Flush(); err != nil {
			log.Error("Error on flush: %v", err)
		}
	}
}

func DataHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Halt()
	log.Debug("In DATA handler")

	if session.Migrator != nil {
		session.Migrator.SetError(types.ErrProxyClosing)
		session.Migrator.Close()
		session.Migrator = nil
	}

	// put DATA to s3
	collector.Save()

	rsp, _ := Server.AddResponsesWithPreparer(c.Name, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
		w.AppendBulkString("OK")
	}, client)

	if err := rsp.Flush(); err != nil {
		log.Error("Error on data::flush: %v", err)
	}

	log.Debug("data complete")
	if err := lambdaLife.TimeoutAfter(Server.Close, worker.RetrialDelayStartFrom); err != nil {
		log.Error("Timeout on closing the worker.")
	}
	Lifetime.Rest()

	// Reset store
	Store = nil
	Persist = nil
	Lineage = nil
	log.Debug("before done")
	session.Done()
}

func MigrateHandler(w resp.ResponseWriter, c *resp.Command) {
	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Halt()
	log.Debug("In MIGRATE handler")

	// addr:port
	addr := c.Arg(0).String()
	deployment := c.Arg(1).String()
	newId, _ := c.Arg(2).Int()
	requestFromProxy := false

	if !session.IsMigrating() {
		// Migration initiated by proxy
		requestFromProxy = true
		session.Migrator = migrator.NewClient()
	}

	// dial to migrator
	if err := session.Migrator.Connect(addr); err != nil {
		return
	}

	if err := session.Migrator.TriggerDestination(deployment, &protocol.InputEvent{
		Cmd:    "migrate",
		Id:     uint64(newId),
		Proxy:  session.Input.Proxy,
		Addr:   addr,
		Prefix: collector.Prefix,
		Log:    log.GetLevel(),
	}); err != nil {
		return
	}

	// Now, we serve migration connection
	go func(session *lambdaLife.Session) {
		// In session gorouting
		session.Migrator.WaitForMigration(Server.Server)
		// Migration ends or is interrupted.

		// Should be ready if migration ended.
		if session.Migrator.IsReady() {
			// put data to s3 before migration finish
			collector.Save()

			// This is essential for debugging, and useful if deployment pool is not large enough.
			Lifetime.Rest()
			// Keep or not? It is a problem.
			// KEEP: MUST if migration is used for backup
			// DISCARD: SHOULD if to be reused after migration.
			// lifetime.Store = storage.New()

			// Close session
			session.Migrator = nil
			session.Done()
		} else if requestFromProxy {
			session.Migrator = nil
			session.Timeout.Restart(lambdaLife.TICK_ERROR)
		}
	}(session)

	// Gracefully close the server.
	// The server will not be closed immediately. Instead, it waits until:
	// 1. The replica will connect to the proxy and relay concurrently.
	// 2.a The proxy will disconnect the ctrl and data link in the worker, yet the redeo server in worker is still serving.
	// 2.b The redeo server continue serves the connection from the replica through the relay.
	Server.CloseWithOptions(true)

	// Signal migrator is ready and start migration. The migration will only begin if:
	// 1. The replica is connected (handled in mhello)
	// 2. The worker is disconnected by proxy (worker closed)
	session.Migrator.SetReady()

	// Prevent timeout
	session.Timeout.EndInterruption()
}

func MHelloHandler(w resp.ResponseWriter, c *resp.Command) {
	session := lambdaLife.GetSession()
	if session.Migrator == nil {
		log.Error("Migration is not initiated.")
		return
	}

	// Wait for ready, which means connection to proxy is closed and we are safe to proceed.
	err := <-session.Migrator.Ready()
	if err != nil {
		return
	}

	// Send key list by access time
	w.AppendBulkString("mhello")
	w.AppendBulkString(strconv.Itoa(Store.Len()))

	delList := make([]string, 0, 2*Store.Len())
	getList := delList[Store.Len():Store.Len()]
	for key := range Store.Keys() {
		_, _, ret := Store.Get(key)
		if ret.Error() == types.ErrNotFound {
			delList = append(delList, key)
		} else {
			getList = append(getList, key)
		}
	}

	for _, key := range delList {
		w.AppendBulkString(fmt.Sprintf("%d%s", types.OP_DEL, key))
	}
	for _, key := range getList {
		w.AppendBulkString(fmt.Sprintf("%d%s", types.OP_GET, key))
	}

	if err := w.Flush(); err != nil {
		log.Error("Error on mhello::flush: %v", err)
		return
	}
}
