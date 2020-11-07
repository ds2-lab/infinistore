package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	//	"github.com/wangaoone/s3gof3r"

	"math/rand"
	"net/url"
	"os"
	"runtime"

	// "runtime/pprof"
	"strconv"
	"sync"
	"time"

	mock "github.com/jordwest/mock-conn"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	"github.com/mason-leap-lab/infinicache/lambda/handlers"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/migrator"
	"github.com/mason-leap-lab/infinicache/lambda/storage"
	. "github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/types"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

var (
	ExpectedGOMAXPROCS = 2
	DefaultStatus      = protocol.Status{}

	log  = Log
	pong = handlers.NewPongHandler()
)

func init() {
	if DRY_RUN {
		log.Level = logger.LOG_LEVEL_ALL
	}
	goroutines := runtime.GOMAXPROCS(0)
	if goroutines < ExpectedGOMAXPROCS {
		log.Debug("Set GOMAXPROCS to %d (original %d)", ExpectedGOMAXPROCS, goroutines)
		runtime.GOMAXPROCS(ExpectedGOMAXPROCS)
	} else {
		log.Debug("GOMAXPROCS %d", goroutines)
	}

	Lifetime = lambdaLife.New(LIFESPAN)
	Server = worker.NewWorker(Lifetime.Id())
	Server.SetHeartbeater(pong)

	collector.S3Bucket = S3_COLLECTOR_BUCKET
	collector.Lifetime = Lifetime
}

func getAwsReqId(ctx context.Context) string {
	lc, ok := lambdacontext.FromContext(ctx)
	if !ok {
		log.Debug("get lambda context failed %v", ok)
	}
	if lc == nil && DRY_RUN {
		return "dryrun"
	}
	return lc.AwsRequestID
}

func HandleRequest(ctx context.Context, input protocol.InputEvent) (protocol.Status, error) {
	// Just once, persistent feature can not be changed anymore.
	storage.Backups = input.Backups
	Store, _ = Store.Init(input.Id, input.IsPersistencyEnabled())
	Lineage = Store.(*storage.Storage).ConfigS3Lineage(S3_BACKUP_BUCKET, "")
	Persist = (types.PersistentStorage)(nil)
	if Lineage != nil {
		Persist = Store.(*storage.Storage)
	}

	// Initialize session.
	Lifetime.RebornIfDead() // Reset if necessary. This is essential for debugging, and useful if deployment pool is not large enough.
	session := lambdaLife.GetOrCreateSession()
	session.Sid = input.Sid
	session.Id = getAwsReqId(ctx)
	session.Input = &input
	defer lambdaLife.ClearSession()

	// Setup timeout.
	deadline, _ := ctx.Deadline()
	session.Timeout.SetLogger(log)
	session.Timeout.StartWithDeadline(deadline)
	collector.Session = session

	// Ensure pong will only be issued once on invocation
	pong.Issue(input.Cmd == protocol.CMD_PING)
	// Setup of the session is done.
	session.Setup.Done()

	// Update global parameters
	collector.Prefix = input.Prefix
	log.Level = input.Log
	Store.(*storage.Storage).ConfigLogger(log.Level, log.Color)
	lambdaLife.Immortal = !input.IsReplicaEnabled()

	log.Info("New lambda invocation: %v", input.Cmd)

	// migration triggered lambda
	if input.Cmd == protocol.CMD_MIGRATE && !migrateHandler(&input, session) {
		return DefaultStatus, nil
	}

	// Check connection
	Server.SetManualAck(true)
	if started, err := Server.StartOrResume(input.Proxy, &worker.WorkerOptions{DryRun: DRY_RUN}); err != nil {
		return DefaultStatus, err
	} else if started {
		Lifetime.Reborn()
	}

	// Extend timeout for expecting requests except invocation with cmd "warmup".
	if input.Cmd == protocol.CMD_WARMUP {
		session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR, input.Cmd)
	} else {
		session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR_EXTEND, input.Cmd)
	}

	// Start data collector
	go collector.Collect(session)

	var recoverErrs []chan error
	flags := protocol.PONG_FOR_CTRL
	if Lineage == nil {
		// PONG represents the node is ready to serve, no fast recovery required.
		pong.SendWithFlags(ctx, flags)
	} else {
		log.Debug("Input meta: %v", input.Status)
		if len(input.Status) == 0 {
			return Lineage.Status().ProtocolStatus(), errors.New("no node status found in the input")
		} else if len(input.Status) == 1 {
			// No backup info
			Lineage.ResetBackup()
		}

		// Preprocess protocol meta and check consistency
		metas := make([]*types.LineageMeta, len(input.Status))
		var err error
		var inconsistency int
		for i := 0; i < len(metas); i++ {
			metas[i], err = types.LineageMetaFromProtocol(&input.Status[i])
			if err != nil {
				return Lineage.Status().ProtocolStatus(), err
			}

			consistent, err := Lineage.IsConsistent(metas[i])
			if err != nil {
				return Lineage.Status().ProtocolStatus(), err
			} else if !consistent {
				if input.IsBackingOnly() && i == 0 {
					// In backing only mode, we will not try to recover main repository.
					// And any data loss will be regarded as signs of reclaimation.
					flags |= protocol.PONG_RECLAIMED
				} else {
					inconsistency++
				}
			}
		}

		// Recover if inconsistent
		if inconsistency == 0 {
			// PONG represents the node is ready to serve, no fast recovery required.
			pong.SendWithFlags(ctx, flags)
		} else {
			session.Timeout.Busy("recover")
			recoverErrs = make([]chan error, 0, inconsistency)

			// Meta 0 is always the main meta
			if !input.IsBackingOnly() && !metas[0].Consistent {
				fast, chanErr := Lineage.Recover(metas[0])
				// PONG represents the node is ready to serve, request fast recovery.
				if fast {
					flags |= protocol.PONG_RECOVERY
				}
				pong.SendWithFlags(ctx, flags)
				recoverErrs = append(recoverErrs, chanErr)
			} else {
				pong.SendWithFlags(ctx, flags)
			}

			// Recovery backup
			for i := 1; i < len(metas); i++ {
				if !metas[i].Consistent {
					_, chanErr := Lineage.Recover(metas[i])
					recoverErrs = append(recoverErrs, chanErr)
				}
			}
		}

		// Start tracking
		Lineage.TrackLineage()
	}
	Server.SetManualAck(false)

	// Wait until recovered to avoid timeout on recovery.
	if recoverErrs != nil {
		waitForRecovery(recoverErrs...)
		// Signal proxy the recover procedure is done.
		// Normally the recovery of main repository is longer than backup, so we just wait all is done.
		if flags&protocol.PONG_RECOVERY > 0 {
			err := recoveredHandler(ctx)
			if err != nil {
				log.Error("Error on notify recovery done: %v", err)
				// Continue...
			}
		}
		session.Timeout.DoneBusy("recover")
	}

	// Adaptive timeout control
	meta := wait(session, Lifetime).ProtocolStatus()
	log.Debug("Output meta: %v", meta)
	if IsDebug() {
		log.Debug("All go routing cleared(%d)", runtime.NumGoroutine())
	}
	log.Debug("Function returns at %v, interrupted: %v", session.Timeout.Since(), session.Timeout.Interrupted())
	log.Info("served %d, interrupted: %d", session.Timeout.Since(), session.Timeout.Interrupted())
	return meta, nil
}

func waitForRecovery(chs ...chan error) {
	if len(chs) == 1 {
		for err := range chs[0] {
			log.Warn("Error on recovering: %v", err)
		}
		return
	}

	// For multiple channels
	var wg sync.WaitGroup
	for _, ch := range chs {
		wg.Add(1)
		go func(ch chan error) {
			waitForRecovery(ch)
			wg.Done()
		}(ch)
	}
	wg.Wait()
}

func wait(session *lambdaLife.Session, lifetime *lambdaLife.Lifetime) (status types.LineageStatus) {
	defer session.CleanUp.Wait()

	var commitOpt *types.CommitOption
	if Lineage != nil {
		session.Timeout.Confirm = func(timeout *lambdaLife.Timeout) bool {
			// Commit and wait, error will be logged.
			commitOpt, _ = Lineage.Commit()
			return true
		}
	}

	select {
	case <-session.WaitDone():
		// Usually, timeout is used to quit.
		// On system closing, the lineage should have been unset.
		if Lineage != nil {
			log.Error("Seesion aborted faultly when persistence is enabled.")
			status = Lineage.Status()
		}
		return
	case <-session.Timeout.C():
		// There's no turning back.
		session.Timeout.Halt()

		if Lifetime.IsTimeUp() && Store.Len() > 0 {
			// Time to migrate
			// Check of number of keys in store is necessary. As soon as there is any value
			// in the store and time up, we should start migration.

			// Initiate migration
			session.Migrator = migrator.NewClient()
			log.Info("Initiate migration.")
			initiator := func() error { return initMigrateHandler() }
			for err := session.Migrator.Initiate(initiator); err != nil; {
				log.Warn("Fail to initiaiate migration: %v", err)
				if err == types.ErrProxyClosing {
					return
				}

				log.Warn("Retry migration")
				err = session.Migrator.Initiate(initiator)
			}
			log.Debug("Migration initiated.")
		} else {
			// Finalize, this is quick usually.
			if Lineage != nil {
				status = Lineage.StopTracker(commitOpt)
			}
			byeHandler()
			session.Done()
			log.Debug("Lambda timeout, return(%v).", session.Timeout.Since())
			return
		}
	}

	return
}

func recoveredHandler(ctx context.Context) error {
	log.Debug("Sending recovered notification.")
	rsp, _ := Server.AddResponsesWithPreparer(protocol.CMD_RECOVERED, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
	})
	return rsp.Flush()
}

func migrateHandler(input *protocol.InputEvent, session *lambdaLife.Session) bool {
	if len(session.Input.Addr) == 0 {
		log.Error("No migrator set.")
		return false
	}

	// Enter migration mode, ensure the worker is not running and the lifetime is reset.
	Server.Close()
	Lifetime.Reborn()

	// connect to migrator
	session.Migrator = migrator.NewClient()
	if err := session.Migrator.Connect(input.Addr); err != nil {
		log.Error("Failed to connect migrator %s: %v", input.Addr, err)
		return false
	}

	// Send hello
	reader, err := session.Migrator.Send("mhello", nil)
	if err != nil {
		log.Error("Failed to hello source on migrator: %v", err)
		return false
	}

	// Apply store adapter to coordinate migration and normal requests
	adapter := session.Migrator.GetStoreAdapter(Store)
	Store = adapter

	// Reader will be avaiable after connecting and source being replaced
	go func(s *lambdaLife.Session) {
		// In-session gorouting
		s.Timeout.Busy(input.Cmd)
		defer s.Timeout.DoneBusy(input.Cmd)

		s.Migrator.Migrate(reader, Store)
		s.Migrator = nil
		Store = adapter.Restore()
	}(session)

	return true
}

func initMigrateHandler() error {
	// init backup cmd
	rsp, _ := Server.AddResponsesWithPreparer("initMigrate", func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
	})
	return rsp.Flush()
}

func byeHandler() error {
	if DRY_RUN {
		log.Info("Bye")
		return nil
	}
	// init backup cmd
	rsp, _ := Server.AddResponsesWithPreparer("bye", func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
		w.AppendBulkString(rsp.Cmd)
	})
	return rsp.Flush()
}

func main() {
	// Define handlers
	Server.HandleFunc(protocol.CMD_TEST, func(w resp.ResponseWriter, c *resp.Command) {
		client := redeo.GetClient(c.Context())

		pong.Cancel()
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
	})

	Server.HandleFunc(protocol.CMD_GET, func(w resp.ResponseWriter, c *resp.Command) {
		client := redeo.GetClient(c.Context())

		pong.Cancel()
		session := lambdaLife.GetSession()
		session.Timeout.Busy(c.Name)
		session.Requests++
		extension := lambdaLife.TICK_ERROR
		if session.Requests > 1 {
			extension = lambdaLife.TICK
		}
		defer session.Timeout.DoneBusyWithReset(extension, c.Name)

		t := time.Now()
		log.Debug("In GET handler(link:%d)", worker.LinkFromClient(client).ID())

		reqId := c.Arg(0).String()
		// Skip: chunkId := c.Arg(1).String()
		key := c.Arg(2).String()

		chunkId, stream, ret := Store.GetStream(key)
		if stream != nil {
			defer stream.Close()
		}
		d1 := time.Since(t)

		if ret.Error() == nil {
			// construct lambda store response
			response := &worker.ObjectResponse{
				Cmd:        c.Name,
				ReqId:      reqId,
				ChunkId:    chunkId,
				BodyStream: stream,
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
			collector.AddRequest(types.OP_GET, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
		} else {
			var respError *handlers.ResponseError
			if ret.Error() == types.ErrNotFound {
				// Not found
				respError = handlers.NewResponseError(404, "Key not found %s: %v", key, ret.Error())
			} else {
				respError = handlers.NewResponseError(500, "Failed to get %s: %v", key, ret.Error())
			}
			errResponse := &worker.ErrorResponse{Error: respError}
			Server.AddResponses(errResponse, client)
			if err := errResponse.Flush(); err != nil {
				log.Error("Error on flush: %v", err)
			}
			collector.AddRequest(types.OP_GET, respError.Status(), reqId, "-1", 0, 0, time.Since(t), 0, session.Id)
		}
	})

	Server.HandleStreamFunc(protocol.CMD_SET, func(w resp.ResponseWriter, c *resp.CommandStream) {
		client := redeo.GetClient(c.Context())

		pong.Cancel()
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
		var finalize func(*types.OpRet, bool, ...time.Duration)
		finalize = func(ret *types.OpRet, wait bool, ds ...time.Duration) {
			if ret == nil || !ret.IsDelayed() {
				// Only if error
				collector.AddRequest(types.OP_SET, "500", reqId, chunkId, 0, 0, time.Since(t), 0, session.Id)
			} else if wait {
				ret.Wait()
				collector.AddRequest(types.OP_SET, "200", reqId, chunkId, ds[0], ds[1], ds[2], time.Since(t), session.Id)
			} else {
				go finalize(ret, true, ds...)
				return
			}
			session.Timeout.DoneBusyWithReset(extension, c.Name)
		}

		errRsp := &worker.ErrorResponse{}
		reqId, _ = c.NextArg().String()
		chunkId, _ = c.NextArg().String()
		key, _ := c.NextArg().String()
		valReader, err := c.Next()
		if err != nil {
			errRsp.Error = handlers.NewResponseError(500, "Error on get value reader: %v", err)
			Server.AddResponses(errRsp, client)
			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
			}
			finalize(nil, false)
			return
		}

		// Streaming set.
		client.Conn().SetReadDeadline(lambdaLife.GetStreamingDeadline(valReader.Len()))
		ret := Store.SetStream(key, chunkId, valReader)
		client.Conn().SetReadDeadline(time.Time{})
		d1 := time.Since(t)
		if ret.Error() != nil {
			errRsp.Error = ret.Error()
			log.Error("%v", errRsp.Error)
			Server.AddResponses(errRsp, client)

			if err := errRsp.Flush(); err != nil {
				log.Error("Error on flush(error 500): %v", err)
				// Ignore
			}
			finalize(ret, false)
			return
		}

		// write Key, clientId, chunkId, body back to proxy
		response := &worker.ObjectResponse{
			Cmd:     c.Name,
			ReqId:   reqId,
			ChunkId: chunkId,
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
		finalize(ret, false, d1, d2, dt)
	})

	Server.HandleFunc(protocol.CMD_RECOVER, func(w resp.ResponseWriter, c *resp.Command) {
		client := redeo.GetClient(c.Context())

		session := lambdaLife.GetSession()
		session.Timeout.Busy(c.Name)
		session.Requests++
		extension := lambdaLife.TICK_ERROR
		if session.Requests > 1 {
			extension = lambdaLife.TICK
		}
		var ret *types.OpRet
		defer func() {
			if ret == nil || !ret.IsDelayed() {
				session.Timeout.DoneBusyWithReset(extension, c.Name)
			} else {
				go func() {
					ret.Wait()
					session.Timeout.DoneBusyWithReset(extension, c.Name)
				}()
			}
		}()

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
			Cmd:        retCmd,
			ReqId:      reqId,
			ChunkId:    chunkId,
			BodyStream: stream,
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
			collector.AddRequest(types.OP_RECOVER, "200", reqId, chunkId, d1, d2, dt, 0, session.Id)
		}
	})

	Server.HandleFunc(protocol.CMD_DEL, func(w resp.ResponseWriter, c *resp.Command) {
		client := redeo.GetClient(c.Context())

		pong.Cancel()
		session := lambdaLife.GetSession()
		session.Timeout.Busy(c.Name)
		session.Requests++
		extension := lambdaLife.TICK_ERROR
		if session.Requests > 1 {
			extension = lambdaLife.TICK
		}
		var ret *types.OpRet
		defer func() {
			if ret == nil || !ret.IsDelayed() {
				session.Timeout.DoneBusyWithReset(extension, c.Name)
			} else {
				go func() {
					ret.Wait()
					session.Timeout.DoneBusyWithReset(extension, c.Name)
				}()
			}
		}()

		//t := time.Now()
		log.Debug("In Del Handler")

		reqId := c.Arg(0).String()
		chunkId := c.Arg(1).String()
		key := c.Arg(2).String()

		ret = Store.Del(key, chunkId)
		if ret.Error() == nil {
			// write Key, clientId, chunkId, body back to proxy
			response := &worker.ObjectResponse{
				Cmd:     c.Name,
				ReqId:   reqId,
				ChunkId: chunkId,
			}
			Server.AddResponses(response, client)
			if err := response.Flush(); err != nil {
				log.Error("Error on del::flush(set key %s): %v", key, err)
				return
			}
		} else {
			var respError *handlers.ResponseError
			if ret.Error() == types.ErrNotFound {
				// Not found
				respError = handlers.NewResponseError(404, "Failed to del %s: %v", key, ret.Error())
			} else {
				respError = handlers.NewResponseError(500, "Failed to del %s: %v", key, ret.Error())
			}
			errResponse := &worker.ErrorResponse{Error: respError}
			Server.AddResponses(errResponse, client)
			if err := errResponse.Flush(); err != nil {
				log.Error("Error on flush: %v", err)
			}
		}
	})

	Server.HandleFunc(protocol.CMD_DATA, func(w resp.ResponseWriter, c *resp.Command) {
		client := redeo.GetClient(c.Context())

		pong.Cancel()
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
		Store = (*storage.Storage)(nil)
		Lineage = nil
		log.Debug("before done")
		session.Done()
	})

	Server.HandleFunc(protocol.CMD_PING, func(w resp.ResponseWriter, c *resp.Command) {
		// Drain payload anyway.
		payload := c.Arg(0).Bytes()

		session := lambdaLife.GetSession()
		if session == nil {
			// Possibilities are ping may comes after HandleRequest returned or before session started.
			log.Debug("PING ignored: session ended.")
			return
		} else if !session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR_EXTEND, c.Name) && !session.IsMigrating() {
			// Failed to extend timeout, do nothing and prepare to return from lambda.
			log.Debug("PING ignored: timeout extension denied.")
			return
		}

		// Ensure the session is setup.
		session.Setup.Wait()
		if grant := pong.Issue(true); !grant {
			// The only reason for pong response is not being granted is because it conflicts with PONG issued on invocation,
			// which means this PING is a legacy from last invocation.
			log.Debug("PING ignored: request to issue a PONG is denied.")
			return
		}

		log.Debug("PING")
		pong.SendWithFlags(c.Context(), protocol.PONG_FOR_CTRL)

		// Deal with payload
		if len(payload) > 0 {
			session.Timeout.Busy(c.Name)
			skip := true

			var pmeta protocol.Meta
			if err := binary.Unmarshal(payload, &pmeta); err != nil {
				log.Warn("Error on parse payload of the ping: %v", err)
			} else if Lineage == nil {
				log.Warn("Recovery is requested but lineage is not available.")
			} else {
				log.Debug("PING meta: %v", pmeta)

				// For now, only backup request supported.
				meta, err := types.LineageMetaFromProtocol(&pmeta)
				if err != nil {
					log.Warn("Error on get meta: %v", err)
				}

				consistent, err := Lineage.IsConsistent(meta)
				if err != nil {
					log.Warn("Error on check consistency: %v", err)
				}

				if !consistent {
					skip = false
					_, chanErr := Lineage.Recover(meta)
					session.Timeout.ResetWithExtension(lambdaLife.TICK, c.Name) // Update extension for backup
					go func() {
						waitForRecovery(chanErr)
						session.Timeout.DoneBusy(c.Name)
					}()
				} else {
					log.Debug("Backup node(%d) consistent, skip.", meta.Meta.Id)
				}
			}

			if skip {
				// No more request expected for a ping with payload (backup ping).
				session.Timeout.DoneBusyWithReset(lambdaLife.TICK_ERROR, c.Name)
			}
		}
	})

	Server.HandleFunc(protocol.CMD_MIGRATE, func(w resp.ResponseWriter, c *resp.Command) {
		pong.Cancel()
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
	})

	Server.HandleFunc(protocol.CMD_MHELLO, func(w resp.ResponseWriter, c *resp.Command) {
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
	})

	if DRY_RUN {
		var printInfo bool
		flag.BoolVar(&printInfo, "h", false, "Help info?")

		flag.BoolVar(&DRY_RUN, "dryrun", false, "Dryrun on local.")

		var input protocol.InputEvent
		input.Sid = "dummysid"
		input.Status = make(protocol.Status, 1, 2)
		// input.Status = append(input.Status, protocol.Meta{
		// 	1, 2, 203, 10, "ce4d34a28b9ad449a4113d37469fc517741e6b244537ed60fa5270381df3f083", 0, 0, 0, "",
		// })
		flag.StringVar(&input.Cmd, "cmd", "warmup", "Command to trigger")
		flag.Uint64Var(&input.Id, "id", 1, "Node id")
		flag.StringVar(&input.Proxy, "proxy", "", "Proxy address:port")
		flag.StringVar(&input.Prefix, "prefix", "log/dryrun", "Experiment data prefix")
		flag.IntVar(&input.Log, "log", logger.LOG_LEVEL_ALL, "Log level")
		flag.Uint64Var(&input.Flags, "flags", 0, "Flags to customize node behavior, see common/types/types.go")
		flag.Uint64Var(&input.Status[0].Term, "term", 1, "Lineage.Term")
		flag.Uint64Var(&input.Status[0].Updates, "updates", 0, "Lineage.Updates")
		flag.Float64Var(&input.Status[0].DiffRank, "diffrank", 0, "Difference rank")
		flag.StringVar(&input.Status[0].Hash, "hash", "", "Lineage.Hash")
		flag.Uint64Var(&input.Status[0].SnapshotTerm, "snapshot", 0, "Snapshot.Term")
		flag.Uint64Var(&input.Status[0].SnapshotUpdates, "snapshotupdates", 0, "Snapshot.Updates")
		flag.Uint64Var(&input.Status[0].SnapshotSize, "snapshotsize", 0, "Snapshot.Size")
		flag.StringVar(&input.Status[len(input.Status)-1].Tip, "tip", "", "Tips in http query format: bak=1&baks=10")

		// More args
		timeout := flag.Int("timeout", 900, "Execution timeout")
		numToInsert := flag.Int("insert", 0, "Number of random chunks to be inserted on launch")
		sizeToInsert := flag.Int("cksize", 100000, "Size of random chunks to be inserted on launch")
		concurrency := flag.Int("c", 5, "Concurrency of recovery")
		buckets := flag.Int("b", 1, "Number of buckets used to persist.")
		statusAsPayload := flag.Bool("payload", false, "Status will be passed as payload of ping")
		// var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
		// var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

		flag.Parse()

		if printInfo {
			fmt.Fprintf(os.Stderr, "Usage: ./lambda -dryrun [options]\n")
			fmt.Fprintf(os.Stderr, "Example: \n")
			fmt.Fprintf(os.Stderr, "\tPersistently insert 1MB: ./lambda -dryrun -flags=256 -cksize=1000000 -hash=dummy -insert=100\n")
			fmt.Fprintf(os.Stderr, "\tExample output: [{1 2 972 100 hash 2 972 355 }]\n")
			fmt.Fprintf(os.Stderr, "\tPersistently recovery: ./lambda -dryrun -flags=256 -hash=dummy -term=2 -updates=972 -snapshot=2 -snapshotupdates=972 -snapshotsize=355\n")
			fmt.Fprintf(os.Stderr, "Available options:\n")
			flag.PrintDefaults()
			os.Exit(0)
		}

		// if *cpuprofile != "" {
		// 	f, err := os.Create(*cpuprofile)
		// 	if err != nil {
		// 		log.Error("could not create CPU profile: %v", err)
		// 	}
		// 	defer f.Close() // error handling omitted for example
		// 	if err := pprof.StartCPUProfile(f); err != nil {
		// 		log.Error("could not start CPU profile: %v", err)
		// 	}
		// 	defer pprof.StopCPUProfile()
		// }

		input.Status[0].Id = input.Id
		tips, err := url.ParseQuery(input.Status[len(input.Status)-1].Tip)
		if err != nil {
			log.Warn("Invalid tips(%s) in protocol meta: %v", input.Status[len(input.Status)-1].Tip, err)
		}

		var payload *protocol.Meta
		if *statusAsPayload {
			payload = &protocol.Meta{}
			*payload = input.Status[0]
			input.Id++
			input.Status[0] = protocol.Meta{
				Id:   input.Id,
				Term: 1,
			}
		}

		if DRY_RUN {
			ctx := context.Background()

			log.Color = true
			log.Verbose = true
			storage.Concurrency = *concurrency
			storage.Buckets = *buckets
			var shortcut *protocol.ShortcutConn
			if input.Proxy == "" {
				protocol.InitShortcut()
				shortcut = protocol.Shortcut.Prepare("dryrun", 0, 2)
				input.Proxy = shortcut.Address
			}

			ready := make(chan struct{})
			invokes := make(chan *protocol.InputEvent, 1)
			ended := make(chan context.Context, 1)
			alldone := sync.WaitGroup{}
			exit := make(chan struct{})

			// Dummy Proxy
			if shortcut == nil {
				ctx = context.WithValue(ctx, &handlers.ContextKeyReady, ready)
				invokes <- &input
				close(invokes)
			} else {
				writePing := func(writer *resp.RequestWriter, payload []byte) {
					writer.WriteMultiBulkSize(2)
					writer.WriteBulkString(protocol.CMD_PING)
					writer.WriteBulk(payload)
					writer.Flush()
				}

				readPong := func(reader resp.ResponseReader) {
					reader.ReadBulkString()     // pong
					reader.ReadInt()            // store id
					reader.ReadBulkString()     // session id
					flag, _ := reader.ReadInt() // flag
					log.Debug("pong flag: %d", flag)
				}

				// writeTest := func(writer *resp.RequestWriter) {
				// 	writer.WriteCmd(protocol.CMD_TEST)
				// 	writer.Flush()
				// }

				// readTest := func(reader resp.ResponseReader) {
				// 	reader.ReadBulkString() // test
				// }

				consumeDataPongs := func(conns ...*mock.Conn) {
					for _, conn := range conns {
						go func(cn net.Conn) {
							client := worker.NewClient(cn)
							readPong(client.Reader)
							log.Info("Data PONG received.")
						}(conn.Server)
					}
				}

				// cutConnection := func(idx int, permanent bool) net.Conn {
				// 	old := shortcut.Conns[idx]
				// 	shortcut.Conns[idx] = mock.NewConn()
				// 	if permanent {
				// 		old.Server.Close()
				// 	} else {
				// 		old.Close()
				// 	}

				// 	return shortcut.Conns[idx].Server
				// }

				alldone.Add(1)
				// Proxy simulator
				go func() {
					log.Info("First Invocation")
					// First invocation
					invokes <- &input

					// Consume messages from datalinks
					consumeDataPongs(shortcut.Conns[1:]...)
					ctrlClient := worker.NewClient(shortcut.Conns[0].Server)
					readPong(ctrlClient.Reader)
					log.Info("Ctrl PONG received.")
					ready <- struct{}{}

					if *statusAsPayload {
						pl, _ := binary.Marshal(payload)
						writePing(ctrlClient.Writer, pl)
						readPong(ctrlClient.Reader)
					}

					<-ended

					// // Control link interruption test
					//
					// interupted := false
					// for i := 0; i < 2; i++ {
					// 	start := time.Now()
					// 	if !interupted {
					// 		writePing(ctrlClient.Writer, nil)
					// 	}
					// 	readPong(ctrlClient.Reader)
					// 	log.Info("HeartBeat latency %v", time.Since(start))

					// 	start = time.Now()
					// 	writeTest(ctrlClient.Writer)
					// 	readTest(ctrlClient.Reader)
					// 	log.Info("Test latency %v", time.Since(start))

					// 	// Simulate network interruption. First one must interrupt.
					// 	interupted = i == 0 || rand.Int()%2 == 0
					// 	if interupted {
					// 		ctrlClient = worker.NewClient(cutConnection(0, false))
					// 	}
					// }

					// // Consume left pongs
					// if interupted {
					// 	readPong(ctrlClient.Reader)
					// }

					// // Data link interruption test
					// // Simulate network interruption. First one must interrupt.
					// dataClient := worker.NewClient(shortcut.Conns[1].Server)
					// interupted := true
					// for i := 0; i < 2; i++ {
					// 	start := time.Now()
					// 	writePing(ctrlClient.Writer, nil)
					// 	readPong(ctrlClient.Reader)
					// 	log.Info("HeartBeat latency %v", time.Since(start))

					// 	start = time.Now()
					// 	writeTest(dataClient.Writer)
					// 	if interupted {
					// 		dataClient = worker.NewClient(cutConnection(1, false))
					// 		readPong(dataClient.Reader)
					// 	}
					// 	readTest(dataClient.Reader)
					// 	log.Info("Test latency %v", time.Since(start))

					// 	interupted = rand.Int()%2 == 0
					// }

					// // Data link close test
					// // Simulate network interruption. First one must interrupt.
					// dataClient := worker.NewClient(shortcut.Conns[1].Server)
					// interupted := false
					// for i := 0; i < 2; i++ {
					// 	start := time.Now()
					// 	writePing(ctrlClient.Writer, nil)
					// 	readPong(ctrlClient.Reader)
					// 	log.Info("HeartBeat latency %v", time.Since(start))

					// 	start = time.Now()
					// 	writeTest(dataClient.Writer)
					// 	if interupted {
					// 		cutConnection(1, true) // To trigger close event, we may need to change error handling in worker
					// 		break
					// 	}
					// 	readTest(dataClient.Reader)
					// 	log.Info("Test latency %v", time.Since(start))

					// 	interupted = true
					// }

					// Get on recovering test
					// time.Sleep(time.Second)
					// dataClient := worker.NewClient(shortcut.Conns[1].Server)
					// dataClient.Writer.WriteMultiBulkSize(4)
					// dataClient.Writer.WriteBulkString(protocol.CMD_GET)
					// dataClient.Writer.WriteBulkString("dummy request id")
					// dataClient.Writer.WriteBulkString("1")
					// dataClient.Writer.WriteBulkString("obj-10")
					// dataClient.Writer.Flush()

					// dataClient.Reader.ReadBulkString() // cmd
					// dataClient.Reader.ReadBulkString() // reqid
					// dataClient.Reader.ReadBulkString() // chunk id
					// dataClient.Reader.ReadBulkString() // stream

					// <-ended

					// // Second Invocation
					// log.Info("Second Invocation")
					// input.Cmd = "ping"
					// input.Status[0] = protocol.Meta{
					// 	1, 3, 1178, 110, "8ecfe3b5ccf81b28fcb008ebec3d38b1507a52a186a920542f602f4a964d7eba", 3, 1178, 358, "",
					// }
					// invokes <- &input
					// // ctrlClient = worker.NewClient(cutConnection(0))
					// // cutConnection(1)
					// // consumeDataPongs(shortcut.Conns[1:]...)
					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")
					// // // Do nothing
					// // // ready <- struct{}{}
					// // ctrlClient.Writer.WriteCmd(protocol.CMD_DATA)
					// // ctrlClient.Writer.Flush()

					// // // Simulate disconnection between request and response.
					// // ctrlClient = worker.NewClient(cutConnection(0, false))
					// // readPong(ctrlClient.Reader)
					// // log.Info("Ctrl PONG received.")

					// // // data
					// // // OK
					// // for line := 2; line > 0; line-- {
					// // 	str, _ := ctrlClient.Reader.ReadBulkString()
					// // 	fmt.Println(str)
					// // }

					// // // Get on recovering test
					// // dataClient := worker.NewClient(shortcut.Conns[1].Server)
					// // dataClient.Writer.WriteMultiBulkSize(4)
					// // dataClient.Writer.WriteBulkString(protocol.CMD_GET)
					// // dataClient.Writer.WriteBulkString("dummy request id")
					// // dataClient.Writer.WriteBulkString("1")
					// // dataClient.Writer.WriteBulkString("obj-9")
					// // dataClient.Writer.Flush()

					// // dataClient.Reader.ReadBulkString() // cmd
					// // dataClient.Reader.ReadBulkString() // reqid
					// // dataClient.Reader.ReadBulkString() // chunk id
					// // dataClient.Reader.ReadBulkString() // stream

					// <-ended

					// // Backup switching memory leak test
					// // 1 Backup
					// log.Info("Backup 2")
					// input.Cmd = "ping"
					// input.Status = protocol.Status{
					// 	input.Status[0],
					// 	protocol.Meta{
					// 		2, 2, 553, 50, "cd04184c7e969140666a1a27dc253538b654b3c05628c307d41f2b3749eb4e21", 2, 553, 263, "bak=0&baks=1",
					// 	},
					// }
					// invokes <- &input

					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")

					// <-ended

					// log.Info("Store size: %d", Store.Len())

					// log.Info("Backup 3")
					// input.Cmd = "ping"
					// input.Status = protocol.Status{
					// 	input.Status[0],
					// 	protocol.Meta{
					// 		3, 2, 545, 50, "dcfde038dc254250531da9a38315ddfaa9c42b7b91023fd76a0b68140b386a57", 2, 545, 255, "bak=0&baks=1",
					// 	},
					// }
					// invokes <- &input

					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")

					// <-ended

					// log.Info("Store size: %d", Store.Len())

					// log.Info("Backup 4")
					// input.Cmd = "ping"
					// input.Status = protocol.Status{
					// 	input.Status[0],
					// 	protocol.Meta{
					// 		4, 2, 550, 50, "a4b380736d3a37cccbfb7b8512355430915d4809e813c83c6ac427134c846da0", 2, 550, 254, "bak=0&baks=1",
					// 	},
					// }
					// invokes <- &input

					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")

					// <-ended

					// log.Info("Store size: %d", Store.Len())

					// log.Info("Backup 5")
					// input.Cmd = "ping"
					// input.Status = protocol.Status{
					// 	input.Status[0],
					// 	protocol.Meta{
					// 		4, 2, 541, 50, "58f0b21ecdb37a048f73a48e333c3f92e006e14808550684a1583c77e628ef86", 2, 541, 253, "bak=0&baks=1",
					// 	},
					// }
					// invokes <- &input

					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")

					// <-ended

					// log.Info("Store size: %d", Store.Len())

					// log.Info("Backup 6")
					// input.Cmd = "ping"
					// input.Status = protocol.Status{
					// 	input.Status[0],
					// 	protocol.Meta{
					// 		5, 2, 542, 50, "1ba25926d17c1af95334088abe6e15f066d731d843646bbcff117d9c4be64750", 2, 542, 258, "bak=0&baks=1",
					// 	},
					// }
					// invokes <- &input

					// readPong(ctrlClient.Reader)
					// log.Info("Ctrl PONG received.")

					// <-ended

					log.Info("Store size: %d", Store.Len())
					Store.(*storage.Storage).ClearBackup()
					log.Info("Store size after cleanup: %d", Store.Len())

					// End of invocations
					close(invokes)

					alldone.Done()
				}()
			}

			// Lambda Function
			alldone.Add(1)
			go func() {
				for input := range invokes {
					d := time.Now().Add(time.Duration(*timeout) * time.Second)
					ctx, cancel := context.WithDeadline(ctx, d)

					start := time.Now()
					lambdacontext.FunctionName = fmt.Sprintf("node%d", input.Id)
					log.Info("Start dummy node: %s", lambdacontext.FunctionName)
					output, err := HandleRequest(ctx, *input)
					if err != nil {
						log.Error("Error: %v", err)
					} else {
						log.Info("Output: %v", output)
					}

					cancel()
					log.Trace("Bill duration for dryrun: %v", time.Since(start))
					ended <- ctx
				}
				alldone.Done()
			}()

			// Wait()
			go func() {
				alldone.Wait()
				close(exit)
			}()

			// Simulate data operation on each invocation
			for {
				select {
				case <-ready:
					session := lambdaLife.GetOrCreateSession()
					session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR_EXTEND, "dryrun")
					session.Timeout.Busy("dryrun")
					if tips.Get(protocol.TIP_SERVING_KEY) != "" {
						if _, _, ret := Store.Get(tips.Get(protocol.TIP_SERVING_KEY)); ret.Error() != nil {
							log.Error("Error on get %s: %v", tips.Get(protocol.TIP_SERVING_KEY), ret.Error())
						} else {
							log.Trace("Delay to serve requested key %s", tips.Get(protocol.TIP_SERVING_KEY))
						}
					}
					for i := 0; i < *numToInsert; i++ {
						val := make([]byte, *sizeToInsert)
						rand.Read(val)
						if ret := Store.Set(fmt.Sprintf("obj-%d-%d", input.Id, int(input.Status[0].DiffRank)+i), "0", val); ret.Error() != nil {
							log.Error("Error on set obj-%d: %v", i, ret.Error())
						}
					}
					// Let ping request running without session timeout
					if *statusAsPayload {
						time.Sleep(10 * time.Second)
					}
					session.Timeout.DoneBusyWithReset(lambdaLife.TICK_ERROR, "dryrun")
				case <-exit:
					// if *memprofile != "" {
					// 	f, err := os.Create(*memprofile)
					// 	if err != nil {
					// 		log.Error("could not create memory profile: ", err)
					// 	}
					// 	defer f.Close() // error handling omitted for example
					// 	runtime.GC()    // get up-to-date statistics
					// 	if err := pprof.WriteHeapProfile(f); err != nil {
					// 		log.Error("could not write memory profile: ", err)
					// 	}
					// }
					return
				}
			}
		} // else: continue to try lambda.Start
	}

	// log.Debug("Routings on launching: %d", runtime.NumGoroutine())
	lambda.Start(HandleRequest)
}
