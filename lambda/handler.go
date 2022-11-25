package main

import (
	"context"
	"encoding/json"
	"errors"

	"runtime"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo/resp"

	// "runtime/pprof"

	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/net"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	"github.com/mason-leap-lab/infinicache/lambda/handlers"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/migrator"
	"github.com/mason-leap-lab/infinicache/lambda/storage"
	"github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/types"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

var (
	ExpectedGOMAXPROCS = 2
	DefaultStatus      = protocol.Status{}

	log = store.Log
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

	store.Lifetime = lambdaLife.New(LIFESPAN)
	store.Server = worker.NewWorker(store.Lifetime.Id())
	store.Server.SetHeartbeater(handlers.Pong)

	collector.S3Bucket = S3_COLLECTOR_BUCKET
	collector.Lifetime = store.Lifetime
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
	memoryLimit := uint64(lambdaLife.MemoryLimitInMB) * 1000000
	if store.Store == nil || store.Store.Id() != input.Id {
		store.Persist = nil
		store.Lineage = nil
		if input.IsRecoveryEnabled() {
			s := storage.NewLineageStorage(input.Id, memoryLimit)
			store.Store = s
			store.Persist = s
			store.Lineage = s
		} else if input.IsPersistencyEnabled() {
			s := storage.NewPersistentStorage(input.Id, memoryLimit)
			store.Store = s
			store.Persist = s
		} else {
			store.Store = storage.NewStorage(input.Id, memoryLimit)
		}
	}
	if store.Persist != nil {
		store.Persist.ConfigS3(S3_BACKUP_BUCKET, "")
	}

	// Initialize session.
	store.Lifetime.RebornIfDead() // Reset if necessary. This is essential for debugging, and useful if deployment pool is not large enough.
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
	handlers.Pong.Issue(input.Cmd == protocol.CMD_PING)
	// Setup of the session is done.
	session.Setup.Done()

	// Update global parameters
	collector.Prefix = input.Prefix
	log.Level = input.Log
	store.Store.(types.Loggable).ConfigLogger(log.Level, log.Color)
	lambdaLife.Immortal = !input.IsReplicaEnabled()

	log.Info("New lambda invocation: %v", input.Cmd)

	// migration triggered lambda
	if input.Cmd == protocol.CMD_MIGRATE && !migrateHandler(&input, session) {
		return DefaultStatus, nil
	}

	store.Server.SetManualAck(true)

	// Check connection
	proxyAddr := input.ProxyAddr // So far, ProxyAddr is used for shortcut connection only.
	var wopts worker.WorkerOptions
	if proxyAddr == nil {
		proxyAddr = net.StrAddr(input.Proxy)
	} else {
		wopts.DryRun = true
	}
	wopts.LogLevel = log.Level
	if started, err := store.Server.StartOrResume(proxyAddr, &wopts); err != nil {
		return DefaultStatus, err
	} else if started {
		store.Lifetime.Reborn()
	}

	// Extend timeout for expecting requests except invocation with cmd "warmup".
	if input.Cmd == protocol.CMD_WARMUP {
		session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR, input.Cmd)
	} else {
		session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR_EXTEND, input.Cmd)
	}

	// Start data collector
	if collector.Prefix != "" {
		go collector.Collect(session)
	}

	// Check lineage consistency and recovery if necessary
	var recoverErrs []<-chan error
	flags := protocol.PONG_FOR_CTRL | protocol.PONG_ON_INVOKING
	var payload []byte
	if store.Lineage == nil {
		// PONG represents the node is ready to serve, no fast recovery required.
		handlers.Pong.SendWithFlags(flags, payload)
	} else {
		log.Debug("Input meta: %v", input.Status)
		if len(input.Status.Metas) == 0 {
			return store.Lineage.Status(false).ProtocolStatus(), errors.New("no node status found in the input")
		}

		// Preprocess protocol meta and check consistency
		metas := make([]*types.LineageMeta, len(input.Status.Metas))
		var err error
		var inconsistency int
		for i := 0; i < len(metas); i++ {
			metas[i], err = types.LineageMetaFromProtocol(&input.Status.Metas[i])
			if err != nil {
				return store.Lineage.Status(false).ProtocolStatus(), err
			}

			ret, err := store.Lineage.Validate(metas[i])
			if err != nil {
				return store.Lineage.Status(false).ProtocolStatus(), err
			} else if !ret.IsConsistent() {
				if input.IsBackingOnly() && i == 0 {
					// if i == 0 {
					// In backing only mode, we will not try to recover main repository.
					// And any data loss will be regarded as signs of reclaimation.
					flags |= protocol.PONG_RECLAIMED
					if metas[i].ServingKey() != "" {
						// Invalidate extended timeout.
						session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR, input.Cmd)
					}
				} else {
					inconsistency++
				}
			} else if ret == types.LineageValidationConsistentWithHistoryTerm {
				flags |= protocol.PONG_WITH_PAYLOAD | protocol.PONG_RECONCILE
				payload, _ = binary.Marshal(store.Lineage.Status(true).ShortStatus())
			}
		}

		// Recover if inconsistent
		if inconsistency == 0 {
			// PONG represents the node is ready to serve, no fast recovery required.
			handlers.Pong.SendWithFlags(flags, payload)
		} else {
			session.Timeout.Busy("fast recover")
			recoverErrs = make([]<-chan error, 0, inconsistency)

			// Meta 0 is always the main meta
			if !input.IsBackingOnly() && !metas[0].Consistent {
				fast, chanErr := store.Lineage.Recover(metas[0])
				// PONG represents the node is ready to serve, request fast recovery.
				if fast {
					flags |= protocol.PONG_RECOVERY
				}
				handlers.Pong.SendWithFlags(flags, payload)
				recoverErrs = append(recoverErrs, chanErr)
			} else {
				handlers.Pong.SendWithFlags(flags, payload)
			}

			// Recovery backup
			for i := 1; i < len(metas); i++ {
				if !metas[i].Consistent {
					_, chanErr := store.Lineage.Recover(metas[i])
					recoverErrs = append(recoverErrs, chanErr)
				}
			}
		}
	}

	// Start tracking
	if store.Persist != nil {
		store.Persist.StartTracker()
	}

	store.Server.SetManualAck(false)

	if input.Cmd == protocol.CMD_WARMUP {
		handlers.Pong.Cancel() // No timeout will be reported.
	}

	// Wait until recovered to avoid timeout on recovery.
	if recoverErrs != nil {
		waitForRecovery(recoverErrs...)
		// Release busy first. It's no harm if recoveredHandler failed or timeout on executing recoveredHandler, next invocation will cover it.
		session.Timeout.DoneBusy("fast recover")

		// Signal proxy the recover procedure is done.
		// Normally the recovery of main repository is longer than backup, so we just wait all is done.
		if flags&protocol.PONG_RECOVERY > 0 {
			err := recoveredHandler(ctx)
			if err != nil {
				log.Error("Error on notify recovery done: %v", err)
				// Continue...
			}
		}
	}

	// Adaptive timeout control
	log.Debug("Waiting for timeout...")
	status := wait(session, store.Lifetime)

	if DRY_RUN {
		store.Server.Close()
	} else {
		store.Server.Pause()
	}
	meta := finalize(status)
	log.Debug("Output meta: %v", meta)
	if store.IsDebug() {
		log.Debug("All go routing cleared(%d)", runtime.NumGoroutine())
	}
	log.Info("Function returns at %v, interrupted: %v", session.Timeout.Since(), session.Timeout.Interrupted())
	log.Debug("served: %d, interrupted: %d, effective: %.2f MB, mem: %.2f MB, max: %.2f MB, stored: %.2f MB, backed: %.2f MB",
		session.Timeout.Since(), session.Timeout.Interrupted(),
		float64(store.Store.Meta().Effective())/1000000, float64(store.Store.Meta().System())/1000000, float64(store.Store.Meta().Waterline())/1000000,
		float64(store.Store.Meta().Size())/1000000, float64(store.Store.Meta().(*storage.StorageMeta).BackupSize())/1000000)
	return *meta, nil
}

func waitForRecovery(chs ...<-chan error) {
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
		go func(ch <-chan error) {
			waitForRecovery(ch)
			wg.Done()
		}(ch)
	}
	wg.Wait()
}

func wait(session *lambdaLife.Session, lifetime *lambdaLife.Lifetime) (status types.LineageStatus) {
	defer session.CleanUp.Wait()

	if store.Lineage != nil {
		session.Timeout.Confirm = func(timeout *lambdaLife.Timeout) bool {
			// Commit and wait, error will be logged.
			// Confirming will not block further operations. Don't stop tracking.
			store.Lineage.Commit()
			return true
		}
	}

	select {
	case <-session.WaitDone():
		// Usually, timeout is used to quit.
		// On system closing, the lineage should have been unset.
		if store.Lineage != nil {
			log.Error("Seesion aborted faultly when persistence is enabled.")
			status = store.Lineage.Status(false)
		}
		return
	case <-session.Timeout.C():
		// There's no turning back.
		session.Timeout.Halt()

		// Migration should be reviewed
		// if Lifetime.IsTimeUp() && Store.Len() > 0 {
		// 	// Time to migrate
		// 	// Check of number of keys in store is necessary. As soon as there is any value
		// 	// in the store and time up, we should start migration.

		// 	// Initiate migration
		// 	session.Migrator = migrator.NewClient()
		// 	log.Info("Initiate migration.")
		// 	initiator := func() error { return initMigrateHandler() }
		// 	for err := session.Migrator.Initiate(initiator); err != nil; {
		// 		log.Warn("Fail to initiaiate migration: %v", err)
		// 		if err == types.ErrProxyClosing {
		// 			return
		// 		}

		// 		log.Warn("Retry migration")
		// 		err = session.Migrator.Initiate(initiator)
		// 	}
		// 	log.Debug("Migration initiated.")
		// } else {

		// Finalize. Stop tracker after timeout is triggered and irreversable.
		// This is quick usually.
		if store.Persist != nil {
			store.Persist.StopTracker()
		}
		if store.Lineage != nil {
			status = store.Lineage.Status(false)
		}
		byeHandler(session, status)
		session.Done()
		log.Info("Lambda timeout, return(%v).", session.Timeout.Since())
		// }
	}

	return
}

func pingHandler(w resp.ResponseWriter, c *resp.Command) {
	// Drain payload anyway.
	datalinks, _ := c.Arg(0).Int()
	reportedAt, _ := c.Arg(1).Int()
	payload := c.Arg(2).Bytes()

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
	if grant := handlers.Pong.Issue(true); !grant {
		// The only reason for pong response is not being granted is because it conflicts with PONG issued on invocation,
		// which means this PING is a legacy from last invocation.
		log.Debug("PING ignored: request to issue a PONG is denied.")
		return
	}

	log.Debug("PING")
	store.Server.VerifyDataLinks(int(datalinks), time.Unix(0, reportedAt))
	cancelPong := false

	// Deal with payload
	if len(payload) > 0 {
		session.Timeout.Busy(c.Name)
		skip := true
		cancelPong = true // For ping with payload, no immediate incoming request expected except certain tips has been set.

		// For now, we expecting one meta per ping only.
		// Possible reasons for the meta are:
		// 1. A node is reclaimed and triggers backup.
		// 2. The lineage of current node need to be reconciled and we receive the reconcile confirmation.
		// Priority gives to the reason with smaller number, and we deal with one reason at a time.
		// TODO: If neccessary, support multiple metas per ping.
		var pmeta protocol.Meta
		if err := binary.Unmarshal(payload, &pmeta); err != nil {
			log.Warn("Error on parse payload of the ping: %v", err)
		} else if store.Lineage == nil {
			log.Warn("Recovery is requested but lineage is not available.")
		} else {
			log.Debug("PING meta: %v", pmeta)

			// For now, only backup request supported.
			if meta, err := types.LineageMetaFromProtocol(&pmeta); err != nil {
				log.Warn("Error on get meta: %v", err)
			} else if ret, err := store.Lineage.Validate(meta); err != nil {
				log.Warn("Error on check consistency: %v", err)
			} else {
				if meta.ServingKey() != "" || meta.Id == store.Store.Id() {
					// 1. Serving key is set and immediate incoming request is expected.
					// 2. This is normal ping with piggybacked reconcile confirmation.
					cancelPong = false
				}
				if !ret.IsConsistent() {
					_, chanErr := store.Lineage.Recover(meta)
					// Check for immediate error.
					select {
					case err := <-chanErr:
						log.Warn("Failed to recover: %v", err)
					default:
						skip = false
						session.Timeout.ResetWithExtension(lambdaLife.TICK, c.Name) // Update extension for backup
						cmd := c.Name
						go func() {
							defer session.Timeout.DoneBusy(cmd)
							waitForRecovery(chanErr)
						}()
					}
				} else if meta.Id != store.Store.Id() {
					log.Debug("Backup node(%d) consistent, skip.", meta.Id)
				} else {
					log.Debug("Term confirmed: %d", meta.Term)
				}
			}
		}

		if skip {
			if cancelPong {
				// No more request expected for a ping with payload (backup ping).
				session.Timeout.DoneBusyWithReset(lambdaLife.TICK_ERROR, c.Name)
			} else {
				session.Timeout.DoneBusy(c.Name)
			}
		}
	}

	// Finally we can send pong.
	flags := protocol.PONG_FOR_CTRL
	if store.Lineage != nil {
		status := store.Lineage.Status(true)
		if status != nil {
			flags |= protocol.PONG_WITH_PAYLOAD | protocol.PONG_RECONCILE
			payload, _ = binary.Marshal(status.ShortStatus())
		}
	}
	handlers.Pong.SendWithFlags(flags, payload)
	if cancelPong {
		handlers.Pong.Cancel() // Not really cancel the sending of a pong, notify no request is expected.
	}
}

func recoveredHandler(ctx context.Context) error {
	log.Debug("Sending recovered notification.")
	rsp, _ := store.Server.AddResponsesWithPreparer(protocol.CMD_RECOVERED, func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
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
	store.Server.Close()
	store.Lifetime.Reborn()

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
	adapter := session.Migrator.GetStoreAdapter(store.Store)
	store.Store = adapter

	// Reader will be avaiable after connecting and source being replaced
	go func(s *lambdaLife.Session) {
		// In-session gorouting
		s.Timeout.Busy(input.Cmd)
		defer s.Timeout.DoneBusy(input.Cmd)

		s.Migrator.Migrate(reader, store.Store)
		s.Migrator = nil
		store.Store = adapter.Restore()
	}(session)

	return true
}

// func initMigrateHandler() error {
// 	// init backup cmd
// 	rsp, _ := Server.AddResponsesWithPreparer("initMigrate", func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
// 		w.AppendBulkString(rsp.Cmd)
// 	})
// 	return rsp.Flush()
// }

func byeHandler(session *lambdaLife.Session, status types.LineageStatus) error {
	// init backup cmd
	if DRY_RUN {
		meta := finalize(status)
		rsp, _ := store.Server.AddResponsesWithPreparer("bye", func(rsp *worker.SimpleResponse, w resp.ResponseWriter) {
			w.AppendBulkString(rsp.Cmd)
			w.AppendBulkString(session.Sid)
			out, _ := json.Marshal(meta)
			w.AppendBulk(out)
		})
		return rsp.Flush()
	}

	// Disable
	return nil
}

func finalize(status types.LineageStatus) *protocol.Status {
	meta := status.ProtocolStatus()

	gcStart := time.Now()
	// Optimize memory usage and return statistics
	storeMeta := store.Store.Meta()
	storeMeta.Calibrate()
	log.Debug("GC takes %v", time.Since(gcStart))
	meta.Capacity = storeMeta.Capacity()
	meta.Mem = storeMeta.Waterline()
	meta.Effective = storeMeta.Effective()
	meta.Modified = storeMeta.Size()

	return &meta
}

func main() {
	// Define handlers
	store.Server.HandleFunc(protocol.CMD_TEST, store.Server.Handler(handlers.TestHandler))
	store.Server.HandleFunc(protocol.CMD_GET, store.Server.Handler(handlers.GetHandler))
	store.Server.HandleStreamFunc(protocol.CMD_SET, store.Server.StreamHandler(handlers.SetHandler))
	store.Server.HandleFunc(protocol.CMD_RECOVER, store.Server.Handler(handlers.RecoverHandler))
	store.Server.HandleFunc(protocol.CMD_DEL, store.Server.Handler(handlers.DelHandler))
	store.Server.HandleFunc(protocol.CMD_DATA, store.Server.Handler(handlers.DataHandler))
	store.Server.HandleFunc(protocol.CMD_PING, store.Server.Handler(pingHandler))
	store.Server.HandleFunc(protocol.CMD_MIGRATE, store.Server.Handler(handlers.MigrateHandler))
	store.Server.HandleFunc(protocol.CMD_MHELLO, store.Server.Handler(handlers.MHelloHandler))

	if DRY_RUN {
		dryrun()
		return
	}

	// log.Debug("Routings on launching: %d", runtime.NumGoroutine())
	lambda.Start(HandleRequest)
}
