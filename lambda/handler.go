package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo/resp"

	"math/rand"
	"net/url"
	"os"
	"runtime"

	// "runtime/pprof"

	"sync"
	"time"

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

	log = Log
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
	Server.SetHeartbeater(handlers.Pong)

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
	if Store == nil || Store.Id() != input.Id {
		Persist = nil
		Lineage = nil
		if input.IsRecoveryEnabled() {
			store := storage.NewLineageStorage(input.Id)
			Store = store
			Persist = store
			Lineage = store
		} else if input.IsPersistencyEnabled() {
			store := storage.NewPersistentStorage(input.Id)
			Store = store
			Persist = store
		} else {
			Store = storage.NewStorage(input.Id)
		}
	}
	if Persist != nil {
		Persist.ConfigS3(S3_BACKUP_BUCKET, "")
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
	handlers.Pong.Issue(input.Cmd == protocol.CMD_PING)
	// Setup of the session is done.
	session.Setup.Done()

	// Update global parameters
	collector.Prefix = input.Prefix
	log.Level = input.Log
	Store.(types.Loggable).ConfigLogger(log.Level, log.Color)
	lambdaLife.Immortal = !input.IsReplicaEnabled()

	log.Info("New lambda invocation: %v", input.Cmd)

	// migration triggered lambda
	if input.Cmd == protocol.CMD_MIGRATE && !migrateHandler(&input, session) {
		return DefaultStatus, nil
	}

	Server.SetManualAck(true)

	// Check connection
	proxyAddr := input.ProxyAddr // So far, ProxyAddr is used for shortcut connection only.
	var wopts *worker.WorkerOptions
	if proxyAddr == nil {
		proxyAddr = protocol.StrAddr(input.Proxy)
	} else {
		wopts = &worker.WorkerOptions{DryRun: DRY_RUN}
	}
	if started, err := Server.StartOrResume(proxyAddr, wopts); err != nil {
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

	// Check lineage consistency and recovery if necessary
	var recoverErrs []chan error
	flags := protocol.PONG_FOR_CTRL | protocol.PONG_ON_INVOKING
	if Lineage == nil {
		// PONG represents the node is ready to serve, no fast recovery required.
		handlers.Pong.SendWithFlags(flags)
	} else {
		log.Debug("Input meta: %v", input.Status)
		if len(input.Status) == 0 {
			return Lineage.Status().ProtocolStatus(), errors.New("no node status found in the input")
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
			handlers.Pong.SendWithFlags(flags)
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
				handlers.Pong.SendWithFlags(flags)
				recoverErrs = append(recoverErrs, chanErr)
			} else {
				handlers.Pong.SendWithFlags(flags)
			}

			// Recovery backup
			for i := 1; i < len(metas); i++ {
				if !metas[i].Consistent {
					_, chanErr := Lineage.Recover(metas[i])
					recoverErrs = append(recoverErrs, chanErr)
				}
			}
		}
	}

	// Start tracking
	if Persist != nil {
		Persist.StartTracker()
	}

	Server.SetManualAck(false)

	if input.Cmd == protocol.CMD_WARMUP {
		handlers.Pong.Cancel() // No timeout will be reported.
	}

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
	Server.Pause()
	log.Debug("Output meta: %v", meta)
	if IsDebug() {
		log.Debug("All go routing cleared(%d)", runtime.NumGoroutine())
	}
	gcStart := time.Now()
	runtime.GC()
	log.Debug("GC takes %v", time.Since(gcStart))
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
			if Persist != nil {
				Persist.StopTracker(commitOpt)
			}
			if Lineage != nil {
				status = Lineage.Status()
			}
			byeHandler()
			session.Done()
			log.Debug("Lambda timeout, return(%v).", session.Timeout.Since())
			return
		}
	}

	return
}

func pingHandler(w resp.ResponseWriter, c *resp.Command) {
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
	if grant := handlers.Pong.Issue(true); !grant {
		// The only reason for pong response is not being granted is because it conflicts with PONG issued on invocation,
		// which means this PING is a legacy from last invocation.
		log.Debug("PING ignored: request to issue a PONG is denied.")
		return
	}

	log.Debug("PING")
	handlers.Pong.SendWithFlags(protocol.PONG_FOR_CTRL)

	// Deal with payload
	if len(payload) > 0 {
		session.Timeout.Busy(c.Name)
		skip := true
		cancelPong := true // For ping with payload, no immediate incoming request expected except certain tips has been set.

		var pmeta protocol.Meta
		if err := binary.Unmarshal(payload, &pmeta); err != nil {
			log.Warn("Error on parse payload of the ping: %v", err)
		} else if Lineage == nil {
			log.Warn("Recovery is requested but lineage is not available.")
		} else {
			log.Debug("PING meta: %v", pmeta)

			// For now, only backup request supported.
			if meta, err := types.LineageMetaFromProtocol(&pmeta); err != nil {
				log.Warn("Error on get meta: %v", err)
			} else if consistent, err := Lineage.IsConsistent(meta); err != nil {
				log.Warn("Error on check consistency: %v", err)
			} else {
				if meta.ServingKey() != "" {
					cancelPong = false // Serving key is set and immediate incoming request is expected.
				}
				if !consistent {
					skip = false
					_, chanErr := Lineage.Recover(meta)
					session.Timeout.ResetWithExtension(lambdaLife.TICK, c.Name) // Update extension for backup
					go func() {
						defer session.Timeout.DoneBusy(c.Name)
						waitForRecovery(chanErr)
					}()
				} else {
					log.Debug("Backup node(%d) consistent, skip.", meta.Meta.Id)
				}
			}
		}

		if cancelPong {
			handlers.Pong.Cancel()
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
	Server.HandleFunc(protocol.CMD_TEST, Server.Handler(handlers.TestHandler))
	Server.HandleFunc(protocol.CMD_GET, Server.Handler(handlers.GetHandler))
	Server.HandleStreamFunc(protocol.CMD_SET, Server.StreamHandler(handlers.SetHandler))
	Server.HandleFunc(protocol.CMD_RECOVER, Server.Handler(handlers.RecoverHandler))
	Server.HandleFunc(protocol.CMD_DEL, Server.Handler(handlers.DelHandler))
	Server.HandleFunc(protocol.CMD_DATA, Server.Handler(handlers.DataHandler))
	Server.HandleFunc(protocol.CMD_PING, Server.Handler(pingHandler))
	Server.HandleFunc(protocol.CMD_MIGRATE, Server.Handler(handlers.MigrateHandler))
	Server.HandleFunc(protocol.CMD_MHELLO, Server.Handler(handlers.MHelloHandler))

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
		flag.StringVar(&input.Sid, "sid", "", "Session id")
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
			fmt.Fprintf(os.Stderr, "\tStart and insert objects with recovery support: ./lambda -dryrun -flags=256 -cksize=1000000 -hash=dummy -insert=100\n")
			fmt.Fprintf(os.Stderr, "\tExample output: [{1 2 972 100 hash 2 972 355 }]\n")
			fmt.Fprintf(os.Stderr, "\tStart with recovery: ./lambda -dryrun -flags=256 -hash=dummy -term=2 -updates=972 -snapshot=2 -snapshotupdates=972 -snapshotsize=355\n")
			fmt.Fprintf(os.Stderr, "\tStart without recovery: ./lambda -dryrun -flags=768\n")
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
			var shortcutCtrl *protocol.ShortcutConn
			if input.Proxy == "" {
				protocol.InitShortcut()
				shortcutCtrl = protocol.Shortcut.Prepare("ctrl", 0, 1)
				input.ProxyAddr = protocol.NewQueueAddr(shortcutCtrl.Address)
			}

			ready := make(chan struct{})
			invokes := make(chan *protocol.InputEvent, 1)
			ended := make(chan context.Context, 1)
			alldone := sync.WaitGroup{}
			exit := make(chan struct{})

			// Dummy Proxy
			if shortcutCtrl == nil {
				invokes <- &input
				close(invokes)
			} else {
				// writePing := func(writer *resp.RequestWriter, payload []byte) {
				// 	writer.WriteMultiBulkSize(2)
				// 	writer.WriteBulkString(protocol.CMD_PING)
				// 	writer.WriteBulk(payload)
				// 	writer.Flush()
				// }

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

				// consumeDataPongs := func(wait bool, shortcuts ...*protocol.ShortcutConn) {
				// 	var wg sync.WaitGroup
				// 	for _, shortcut := range shortcuts {
				// 		if wait {
				// 			wg.Add(1)
				// 		}
				// 		go func(shortcut *protocol.ShortcutConn) {
				// 			client := worker.NewClient(shortcut.Conns[0].Server, false)
				// 			readPong(client.Reader)
				// 			log.Info("Data PONG received %v", shortcut.Conns[0])
				// 			if wait {
				// 				wg.Done()
				// 			}
				// 		}(shortcut)
				// 	}
				// 	if wait {
				// 		wg.Wait()
				// 	}
				// }

				// changeConnection := func(shortcut *protocol.ShortcutConn, nick string, cut bool, permanent ...bool) net.Conn {
				// 	old := shortcut.Conns[0]
				// 	shortcut.Conns[0] = protocol.NewMockConn(shortcut.Address+nick, 0)
				// 	if cut {
				// 		if len(permanent) > 0 && permanent[0] {
				// 			old.Server.Close()
				// 		} else {
				// 			old.Close()
				// 		}
				// 	}
				// 	return shortcut.Conns[0].Server
				// }

				alldone.Add(1)
				// Proxy simulator
				go func() {
					var validated sync.WaitGroup
					log.Info("First Invocation")
					// First invocation
					invokes <- &input
					validated.Add(1)
					time.Sleep(5 * time.Millisecond) // Let lambda run

					// if *statusAsPayload {
					// 	pl, _ := binary.Marshal(payload)
					// 	writePing(ctrlClient.Writer, pl)
					// 	readPong(ctrlClient.Reader)
					// }

					ctrlClient := worker.NewClient(shortcutCtrl.Conns[0].Server, true)
					readPong(ctrlClient.Reader)
					log.Info("Ctrl PONG received %v", shortcutCtrl.Conns[0])
					ready <- struct{}{}

					// Dynamic data link test
					// // Prepare data connection 1 and consume pong
					// shourtcutData := protocol.Shortcut.Prepare("data", 0, 1)
					// input.ProxyAddr.(*protocol.QueueAddr).Push(shourtcutData.Address)
					// clients := make(chan *worker.Client, 1)

					// go func() {
					// 	// Zombie link test
					// 	changeConnection(shortcutCtrl, "-No2", false) // Prepare a new connection
					// 	time.Sleep(40 * time.Millisecond)             // Wait for ack timeout

					// 	ctrlClient := worker.NewClient(shortcutCtrl.Conns[0].Server, true)
					// 	readPong(ctrlClient.Reader)
					// 	log.Info("Ctrl PONG received %v", shortcutCtrl.Conns[0])
					// 	ready <- struct{}{}
					// 	select {
					// 	case clients <- ctrlClient:
					// 	default:
					// 	}
					// 	validated.Done()
					// }()

					// go func() {
					// 	consumeDataPongs(true, shourtcutData)
					// 	select {
					// 	case clients <- worker.NewClient(shourtcutData.Conns[0].Server, false):
					// 		// Prepare data connection 2 and consume pong
					// 		shourtcutData2 := protocol.Shortcut.Prepare("data", 1, 1)
					// 		input.ProxyAddr.(*protocol.QueueAddr).Push(shourtcutData2.Address)
					// 		consumeDataPongs(false, shourtcutData2)
					// 	default:
					// 	}
					// }()

					// // Get key (recovery if not load)
					// validated.Wait()
					// log.Info("Validated, %d", len(clients))
					// client := <-clients
					// log.Info("Client from ctrl: %v", client.Ctrl)
					// client.Writer.WriteMultiBulkSize(5)
					// client.Writer.WriteBulkString(protocol.CMD_GET)
					// client.Writer.WriteBulkString("dummy request id")
					// client.Writer.WriteBulkString("1")
					// client.Writer.WriteBulkString("obj-1-9")
					// client.Writer.WriteBulkString("100000")
					// client.Writer.Flush()

					// time.Sleep(5 * time.Millisecond) // Let lambda run

					// // Error response
					// // msg, _ := ctrlClient.Reader.ReadError()
					// // log.Error("error: %v", msg)

					// // Success response
					// client.Reader.ReadBulkString()            // cmd
					// client.Reader.ReadBulkString()            // reqid
					// client.Reader.ReadBulkString()            // chunk id
					// client.Reader.ReadInt()                   // recovery
					// data, _ := client.Reader.ReadBulkString() // stream
					// log.Info("Recovered data of size: %v", len(data))

					// client.Writer.WriteCmd(protocol.CMD_ACK)
					// client.Writer.Flush()

					// <-ended

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

					// // Get on recovering test
					// dataClient := worker.NewClient(shortcut.Conns[1].Server)
					// dataClient.Writer.WriteMultiBulkSize(4)
					// dataClient.Writer.WriteBulkString(protocol.CMD_GET)
					// dataClient.Writer.WriteBulkString("dummy request id")
					// dataClient.Writer.WriteBulkString("1")
					// dataClient.Writer.WriteBulkString("obj-9")
					// dataClient.Writer.Flush()

					// dataClient.Reader.ReadBulkString() // cmd
					// dataClient.Reader.ReadBulkString() // reqid
					// dataClient.Reader.ReadBulkString() // chunk id
					// dataClient.Reader.ReadBulkString() // stream

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

					<-ended

					if Lineage != nil {
						log.Info("Store size: %d", Store.Len())
						Lineage.(*storage.LineageStorage).ClearBackup()
						log.Info("Store size after cleanup: %d", Store.Len())
					}

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
