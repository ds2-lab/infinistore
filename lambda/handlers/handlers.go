package handlers

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"

	"github.com/kelindar/binary"
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

func BuildPiggyback(response *worker.ObjectResponse) {
	if Lineage != nil {
		confirmed, status := Lineage.Status(true)
		if status != nil {
			log.Info("Attaching unconfirmed terms: %d/%d confirmed", confirmed, status[0].Term)
			response.PiggyFlags |= protocol.PONG_WITH_PAYLOAD | protocol.PONG_RECONCILE
			response.PiggyPayload, _ = binary.Marshal(status.ShortStatus())
		}
	}
}

func GetDefaultExtension(session *lambdaLife.Session) time.Duration {
	extension := Server.GetStats().RTT() * 2 // Expecting new requests to arrive within RTT.
	if extension < lambdaLife.TICK_EXTENSION {
		extension = lambdaLife.TICK_EXTENSION
	}
	// if session.Requests > 1 {
	// 	extension = lambdaLife.TICK_EXTENSION
	// }
	return extension
}

func TestHandler(w resp.ResponseWriter, c *resp.Command) {
	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session := lambdaLife.GetSession()
	session.Timeout.Busy(c.Name)
	extension := GetDefaultExtension(session)
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
	session := lambdaLife.GetSession()
	if session == nil {
		log.Warn("Detected nil session in Get Handler")
		return
	}

	client := redeo.GetClient(c.Context())
	link := worker.LinkFromClient(client)

	Pong.Cancel()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := GetDefaultExtension(session)
	cmd := c.Name // Save for defer, command is reused by redeo.
	defer Server.WaitAck(cmd, func() {
		session.Timeout.DoneBusyWithReset(extension, cmd)
	}, client)

	t := time.Now()
	log.Debug("In GET handler(link:%v, extension:%v)", link, extension)

	reqId := c.Arg(0).String()
	// Skip: chunkId := c.Arg(1).String()
	key := c.Arg(2).String()

	var recovered int64
	chunkId, stream, ret := Store.GetStream(key)
	// Recover if not found. This is not desired if recovery is enabled and will generate a warning.
	// Deleted chunk(ret.Error() == types.ErrDeleted) is considered as not found for reasons:
	// 1. Meta deleted object will not be sent here.
	// 2. The most possible reason for a key being deleted and requested again is the key has been deleted becaused cache space eviction.
	if (ret.Error() == types.ErrNotFound || ret.Error() == types.ErrDeleted || ret.Error() == types.ErrIncomplete) && Persist != nil {
		if Lineage != nil {
			log.Info("Key %v while recovery is enabled: %v %s", ret.Error(), key, reqId)
		} else {
			log.Debug("Key %v locally, try recovery: %v %s", ret.Error(), key, reqId)
		}
		errRsp := &worker.ErrorResponse{}
		chunkId = c.Arg(1).String()
		sizeArg := c.Arg(3)
		option, _ := c.Arg(4).Int()
		if option&protocol.REQUEST_GET_OPTIONAL > 0 {
			errRsp.Error = ret.Error()
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
			}
			return
		}
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
		ret = Persist.SetRecovery(key, chunkId, uint64(size), int(option))
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
			Extension: extension - Server.GetStats().RTT(), // Proxy don't need to consider RTT
		}
		BuildPiggyback(response)

		t2 := time.Now()
		Server.AddResponses(response, client)
		if err := response.Flush(); err != nil {
			// Error is ignored here, since the client may simply discard late response.
			log.Warn("Error on flush(get %s %s): %v", key, reqId, err)
		}
		d2 := time.Since(t2)

		dt := time.Since(t)
		log.Info("Get(link:%v) key:%s %v, duration:%v, prepare: %v, transmission:%v", link, key, reqId, dt, d1, d2)
		collector.AddRequest(t, types.OP_GET, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
	} else {
		var respError *ResponseError
		if ret.Error() == types.ErrNotFound {
			// Not found
			respError = NewResponseError(404, "Key not found %s: %v", key, ret.Error())
		} else {
			respError = NewResponseError(500, "Failed to get %s: %v,%s", key, ret.Error(), ret.Message())
		}
		errResponse := &worker.ErrorResponse{Error: respError}
		Server.AddResponses(errResponse, client)
		if err := errResponse.Flush(); err != nil {
			log.Error("Error on flush error %v: %v", respError, err)
		}
		collector.AddRequest(t, types.OP_GET, respError.Status(), reqId, "-1", 0, 0, time.Since(t), 0, session.Id)
	}
}

func SetHandler(w resp.ResponseWriter, c *resp.CommandStream) {
	session := lambdaLife.GetSession()
	if session == nil {
		log.Warn("Detected nil session in Set Handler")
		return
	}

	client := redeo.GetClient(c.Context())
	link := worker.LinkFromClient(client)

	Pong.Cancel()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := GetDefaultExtension(session)

	t := time.Now()
	var t2 time.Time
	log.Debug("In SET handler(link:%v, extension:%v)", link, extension)

	var reqId, chunkId, key string
	cmd := c.Name
	committed := false
	finalize := func(ret *types.OpRet, ds ...time.Duration) {
		Server.WaitAck(c.Name, func() {
			// Wait if the ret has not been concluded.
			if ret.IsDelayed() {
				ret.Wait()
			}
			d2 := time.Since(t2)

			// Confirm the object is persisted.
			if committed && session.Input.IsWaitForCOSDisabled() {
				var rsp worker.Response
				if err := ret.Error(); err == nil {
					log.Debug("Sending persisted notification: %s", key)
					// Notification will send using control link.
					rsp, _ = Server.AddResponsesWithPreparer(protocol.CMD_PERSISTED, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
						w.AppendBulkString(rsp.Cmd)
						w.AppendBulkString(key)
					})
				} else {
					log.Debug("Sending persist failure notification: %s", key)
					// Notification will send using control link.
					rsp, _ = Server.AddResponsesWithPreparer(protocol.CMD_PERSIST_FAILED, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
						w.AppendBulkString(rsp.Cmd)
						w.AppendBulkString(key)
					})
				}
				if err := rsp.Flush(); err != nil {
					log.Error("Error on flush(persist key %s): %v", key, err)
					// Ignore, network error will be handled by redeo.
				}
			}

			if session.Input.IsWaitForCOSDisabled() {
				log.Info("Set(link:%v) key:%s, chunk: %s, duration:%v, transmission:%v, persistence:%v", link, key, chunkId, ds[2], ds[0], d2)
			}

			// Output experiment data.
			if err := ret.Error(); err == nil {
				collector.AddRequest(t, types.OP_SET, "200", reqId, chunkId, ds[0], ds[1], ds[2], time.Since(t), session.Id)
			} else {
				// If the setstream err is net error (timeout), cut the line.
				if util.IsConnectionFailed(err) {
					Server.SetFailure(client, err)
				}
				collector.AddRequest(t, types.OP_SET, "500", reqId, chunkId, 0, 0, time.Since(t), 0, session.Id)
			}
			session.Timeout.DoneBusyWithReset(extension, cmd)
		}, client)
	}

	errRsp := &worker.ErrorResponse{}
	reqId, _ = c.NextArg().String()
	chunkId, _ = c.NextArg().String()
	key, _ = c.NextArg().String()
	valReader, err := c.Next()
	if err != nil {
		errRsp.Error = NewResponseError(500, "Error on get value reader: %v", err)
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
			// Ignore, network error will be handled by redeo.
		}
		finalize(types.OpError(err))
		return
	}

	// Streaming set.
	client.Conn().SetReadDeadline(protocol.GetBodyDeadline(valReader.Len()))
	ret := Store.SetStream(key, chunkId, valReader)
	client.Conn().SetReadDeadline(time.Time{})
	t2 = time.Now()
	d1 := t2.Sub(t)
	err = ret.Error()
	if err != nil {
		errRsp.Error = err
		Server.AddResponses(errRsp, client)
		if err := errRsp.Flush(); err != nil {
			log.Error("Error on flush(error 500): %v", err)
			// Ignore, network error will be handled by redeo.
		}
		finalize(ret)
		return
	}

	// write Key, clientId, chunkId, body back to proxy
	response := &worker.ObjectResponse{
		BaseResponse: worker.BaseResponse{Cmd: c.Name},
		ReqId:        reqId,
		ChunkId:      chunkId,
		Extension:    extension - Server.GetStats().RTT(), // Proxy don't need to consider RTT
	}
	BuildPiggyback(response)

	if !session.Input.IsWaitForCOSDisabled() {
		err := ret.Wait()
		if err != nil {
			errRsp.Error = err
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
				// Ignore, network error will be handled by redeo.
			}
			finalize(ret)
			return
		}
	}
	d2 := time.Since(t2)

	Server.AddResponses(response, client)
	if err := response.Flush(); err != nil {
		log.Error("Error on set::flush(set key %s): %v", key, err)
		// Ignore
	}
	dt := time.Since(t)
	committed = true

	if !session.Input.IsWaitForCOSDisabled() {
		log.Info("Set(link:%v) key:%s, chunk: %s, duration:%v, transmission:%v, persistence:%v", link, key, chunkId, dt, d1, d2)
	}
	finalize(ret, d1, d2, dt)
}

func RecoverHandler(w resp.ResponseWriter, c *resp.Command) {
	session := lambdaLife.GetSession()
	if session == nil {
		log.Warn("Detected nil session in Recover Handler")
		return
	}

	client := redeo.GetClient(c.Context())
	link := worker.LinkFromClient(client)

	Pong.Cancel()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := GetDefaultExtension(session)

	var ret *types.OpRet
	cmd := c.Name
	defer Server.WaitAck(cmd, func() {
		if ret != nil && ret.IsDelayed() {
			ret.Wait()
		}
		session.Timeout.DoneBusyWithReset(extension, cmd)
	}, client)

	t := time.Now()
	log.Debug("In RECOVER handler(link:%v, extension:%v)", link, extension)

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
	ret = Persist.SetRecovery(key, chunkId, uint64(size), 0)
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
		Extension: extension - Server.GetStats().RTT(), // Proxy don't need to consider RTT
	}
	BuildPiggyback(response)

	t2 := time.Now()
	Server.AddResponses(response, client)
	if err := response.Flush(); err != nil {
		log.Error("Error on recover::flush(recover key %s): %v", key, err)
		// Ignore
	}
	d2 := time.Since(t2)

	dt := time.Since(t)
	log.Info("Recovered(link:%v) key:%s %s, duration:%v, prepare: %v, transmission:%v", link, key, reqId, dt, d1, d2)
	if retCmd == protocol.CMD_GET {
		collector.AddRequest(t, types.OP_RECOVER, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
	}
}

func DelHandler(w resp.ResponseWriter, c *resp.Command) {
	session := lambdaLife.GetSession()
	if session == nil {
		log.Warn("Detected nil session in Del Handler")
		return
	}

	client := redeo.GetClient(c.Context())

	Pong.Cancel()
	session.Timeout.Busy(c.Name)
	session.Requests++
	extension := GetDefaultExtension(session)

	var ret *types.OpRet
	cmd := c.Name
	defer Server.WaitAck(cmd, func() {
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

	ret = Store.Del(key, "request")
	if ret.Error() == nil {
		// write Key, clientId, chunkId, body back to proxy
		response := &worker.ObjectResponse{
			BaseResponse: worker.BaseResponse{Cmd: c.Name},
			ReqId:        reqId,
			ChunkId:      chunkId,
			Extension:    extension - Server.GetStats().RTT(), // Proxy don't need to consider RTT
		}
		BuildPiggyback(response)
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
