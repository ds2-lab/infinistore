package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/hidez8891/shm"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/net"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/invoker"
	lambdaLife "github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/storage"
	"github.com/mason-leap-lab/infinicache/lambda/store"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
	"github.com/mason-leap-lab/redeo/resp"
)

type option struct {
	name            string
	timeout         int
	memory          int
	numToInsert     int
	sizeToInsert    int
	concurrency     int
	buckets         int
	statusAsPayload bool
	cpuProfile      string
	memProfile      string
}

func dryrun() int {
	var input protocol.InputEvent
	opt := validateFlags(&input)
	if opt == nil {
		return 1
	}

	// Config Lambda compatible settings.
	lambdacontext.FunctionName = opt.name
	lambdaLife.MemoryLimitInMB = opt.memory

	// // Setup CPU profiling
	// if opt.cpuprofile != "" {
	// 	f, err := os.Create(opt.cpuprofile)
	// 	if err != nil {
	// 		log.Error("could not create CPU profile: %v", err)
	// 	}
	// 	defer f.Close() // error handling omitted for example
	// 	if err := pprof.StartCPUProfile(f); err != nil {
	// 		log.Error("could not start CPU profile: %v", err)
	// 	}
	// 	defer pprof.StopCPUProfile()
	// }

	// Config logger and storage
	log.Color = true
	log.Verbose = true
	storage.Concurrency = opt.concurrency
	storage.Buckets = opt.buckets

	ready := make(chan struct{})
	invokes := make(chan *protocol.InputEvent, 1)
	ended := make(chan context.Context, 1)
	alldone := sync.WaitGroup{}
	exit := make(chan struct{})

	if input.Proxy == "" {
		// Run tests as dummy proxy
		runTests(invokes, &input, ready, ended, &alldone)
	} else {
		invokes <- &input
		go func() {
			<-ended
			defer close(invokes)

			ctrl, err := shm.Open(opt.name, invoker.ShmSize)
			if err != nil {
				return
			}

			reset := make([]byte, 4)
			buffer := make([]byte, invoker.ShmSize)
			for {
				<-time.After(10 * time.Millisecond)
				_, err := ctrl.ReadAt(buffer, 0)
				if err != nil {
					return
				}
				// Clear what has been read.
				ctrl.WriteAt(reset, 0)

				read := binary.LittleEndian.Uint32(buffer[:4])
				if read == 0 {
					// No input available, retry.
					continue
				}

				if read+4 > uint32(len(buffer)) {
					// Invalid len, retry.
					continue
				} else if err := json.Unmarshal(buffer[4:4+read], &input); err != nil {
					continue
				}

				if input.Cmd == protocol.CMD_BYE {
					return
				}
				invokes <- &input
				<-ended
			}
		}()
	}

	// Start function serving
	alldone.Add(1)
	go func() {
		for input := range invokes {
			d := time.Now().Add(time.Duration(opt.timeout) * time.Second)
			ctx, cancel := context.WithDeadline(context.Background(), d)
			start := time.Now()

			log.Info("Start dummy node: %s, sid: %s", lambdacontext.FunctionName, input.Sid)
			output, err := HandleRequest(ctx, *input)
			if err != nil {
				log.Error("Error: %v", err)
			} else {
				log.Info("Output: %v", output)
			}

			cancel()
			log.Trace("Bill duration for dryrun: %v", time.Since(start))

			// Signal simulation to proceed.
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
			setup(&input, opt)
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
			log.Info("Terminated: dummy node %s", lambdacontext.FunctionName)
			return 0
		}
	}
}

func validateFlags(input *protocol.InputEvent) *option {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "Help info?")

	flag.BoolVar(&DRY_RUN, "dryrun", false, "Dryrun on local.")

	input.Sid = "dummysid"
	input.Status = protocol.Status{Metas: make([]protocol.Meta, 1, 3)} // Init with at least one meta
	flag.StringVar(&input.Sid, "sid", "", "Session id")
	flag.StringVar(&input.Cmd, "cmd", "warmup", "Command to trigger")
	flag.Uint64Var(&input.Id, "id", 1, "Node id")
	flag.StringVar(&input.Proxy, "proxy", "", "Proxy address:port")
	flag.StringVar(&input.Prefix, "prefix", "log/dryrun", "Experiment data prefix")
	flag.IntVar(&input.Log, "log", logger.LOG_LEVEL_ALL, "Log level")
	flag.Uint64Var(&input.Flags, "flags", 0, "Flags to customize node behavior, see common/types/types.go")
	flag.Uint64Var(&input.Status.Metas[0].Term, "term", 1, "Lineage.Term")
	flag.Uint64Var(&input.Status.Metas[0].Updates, "updates", 0, "Lineage.Updates")
	flag.Float64Var(&input.Status.Metas[0].DiffRank, "diffrank", 0, "Difference rank")
	flag.StringVar(&input.Status.Metas[0].Hash, "hash", "", "Lineage.Hash")
	flag.Uint64Var(&input.Status.Metas[0].SnapshotTerm, "snapshot", 0, "Snapshot.Term")
	flag.Uint64Var(&input.Status.Metas[0].SnapshotUpdates, "snapshotupdates", 0, "Snapshot.Updates")
	flag.Uint64Var(&input.Status.Metas[0].SnapshotSize, "snapshotsize", 0, "Snapshot.Size")
	flag.StringVar(&input.Status.Metas[len(input.Status.Metas)-1].Tip, "tip", "", "Tips in http query format: bak=1&baks=10")

	// More meta
	strMetas := flag.String("metas", "", "Extra metas")

	// More args
	opt := &option{}
	flag.StringVar(&opt.name, "name", "", "Function name")
	flag.IntVar(&opt.timeout, "timeout", 900, "Execution timeout")
	flag.IntVar(&opt.memory, "mem", 3096, "Memory limit in MB")
	flag.IntVar(&opt.numToInsert, "insert", 0, "Number of random chunks to be inserted on launch")
	flag.IntVar(&opt.sizeToInsert, "cksize", 100000, "Size of random chunks to be inserted on launch")
	flag.IntVar(&opt.concurrency, "c", 5, "Concurrency of recovery")
	flag.IntVar(&opt.buckets, "b", 1, "Number of buckets used to persist.")
	flag.BoolVar(&opt.statusAsPayload, "payload", false, "Status will be passed as payload of ping")
	flag.StringVar(&opt.cpuProfile, "cpuprofile", "", "write cpu profile to `file`")
	flag.StringVar(&opt.memProfile, "memprofile", "", "write memory profile to `file`")

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
		return nil
	}

	if opt.name == "" {
		opt.name = fmt.Sprintf("node-%d", input.Id)
	}
	input.Status.Metas[0].Id = input.Id

	var metas []protocol.Meta
	json.Unmarshal([]byte(*strMetas), &metas)
	if len(metas) > 0 {
		input.Status.Metas = append(input.Status.Metas, metas...)
	}

	var payload *protocol.Meta
	if opt.statusAsPayload {
		payload = &protocol.Meta{}
		*payload = input.Status.Metas[0]
		input.Id++
		input.Status.Metas[0] = protocol.Meta{
			Id:   input.Id,
			Term: 1,
		}
	}
	return opt
}

func runTests(invokes chan *protocol.InputEvent, input *protocol.InputEvent, ready chan struct{}, ended chan context.Context, alldone *sync.WaitGroup) {
	// Create a dummy ctrl connection for driving test.
	net.InitShortcut()
	shortcutCtrl := net.Shortcut.Prepare("ctrl", 0, 1)
	input.ProxyAddr = net.NewQueueAddr(shortcutCtrl.Address)

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
		invokes <- input
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
		// validated.Wait()
		// log.Info("Validated, %d", len(clients))
		// client := <-clients

		// // Get key (recovery if not load)
		// client := ctrlClient
		// log.Info("Client from ctrl: %v", client.Ctrl)
		// client.Writer.WriteMultiBulkSize(5)
		// client.Writer.WriteBulkString(protocol.CMD_GET)
		// client.Writer.WriteBulkString("dummy request id")
		// client.Writer.WriteBulkString("1")
		// client.Writer.WriteBulkString("obj-1-9")
		// client.Writer.WriteBulkString("100000")
		// client.Writer.Flush()

		// // Error response
		// if ret, _ := client.Reader.PeekType(); ret == resp.TypeError {
		// 	msg, _ := client.Reader.ReadError()
		// 	log.Error("error: %v", msg)
		// } else {
		// 	// Success response
		// 	client.Reader.ReadBulkString()            // cmd
		// 	client.Reader.ReadBulkString()            // reqid
		// 	client.Reader.ReadBulkString()            // chunk id
		// 	client.Reader.ReadInt()                   // recovery
		// 	data, _ := client.Reader.ReadBulkString() // stream
		// 	log.Info("Recovered data of size: %v", len(data))
		// 	client.Reader.ReadInt() // TTL

		// 	client.Writer.WriteCmd(protocol.CMD_ACK)
		// 	client.Writer.Flush()

		// 	// Recover after delete test.
		// 	// // Delete request
		// 	// client.Writer.WriteMultiBulkSize(4)
		// 	// client.Writer.WriteBulkString(protocol.CMD_DEL)
		// 	// client.Writer.WriteBulkString("dummy request id")
		// 	// client.Writer.WriteBulkString("1")
		// 	// client.Writer.WriteBulkString("obj-1-9")
		// 	// client.Writer.Flush()

		// 	// time.Sleep(5 * time.Millisecond) // Let lambda run

		// 	// client.Reader.ReadBulkString() // cmd
		// 	// client.Reader.ReadBulkString() // reqid
		// 	// client.Reader.ReadBulkString() // chunk id
		// 	// client.Reader.ReadInt()        // TTL
		// 	// client.Writer.WriteCmd(protocol.CMD_ACK)
		// 	// client.Writer.Flush()
		// 	// log.Info("Deleted")

		// 	// // Recover
		// 	// client.Writer.WriteMultiBulkSize(6)
		// 	// client.Writer.WriteBulkString(protocol.CMD_RECOVER)
		// 	// client.Writer.WriteBulkString("dummy request id")
		// 	// client.Writer.WriteBulkString("1")
		// 	// client.Writer.WriteBulkString("obj-1-9")
		// 	// client.Writer.WriteBulkString(protocol.CMD_GET)
		// 	// client.Writer.WriteBulkString("100000")
		// 	// client.Writer.Flush()

		// 	// time.Sleep(5 * time.Millisecond) // Let lambda run

		// 	// client.Reader.ReadBulkString()           // cmd
		// 	// client.Reader.ReadBulkString()           // reqid
		// 	// client.Reader.ReadBulkString()           // chunk id
		// 	// client.Reader.ReadInt()                  // recovery
		// 	// data, _ = client.Reader.ReadBulkString() // stream
		// 	// log.Info("Recovered data of size: %v", len(data))
		// 	// client.Reader.ReadInt() // TTL

		// 	// client.Writer.WriteCmd(protocol.CMD_ACK)
		// 	// client.Writer.Flush()
		// }

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

		if store.Lineage != nil {
			log.Info("Store size: %d", store.Store.Len())
			store.Lineage.(*storage.LineageStorage).ClearBackup()
			log.Info("Store size after cleanup: %d", store.Store.Len())
		}

		// End of invocations
		close(invokes)

		alldone.Done()
	}()
}

func setup(input *protocol.InputEvent, opt *option) {
	session := lambdaLife.GetOrCreateSession()
	session.Timeout.ResetWithExtension(lambdaLife.TICK_ERROR_EXTEND, "dryrun")
	session.Timeout.Busy("dryrun")

	tips, err := url.ParseQuery(input.Status.Metas[len(input.Status.Metas)-1].Tip)
	if err != nil {
		log.Warn("Invalid tips(%s) in protocol meta: %v", input.Status.Metas[len(input.Status.Metas)-1].Tip, err)
	}
	if tips.Get(protocol.TIP_SERVING_KEY) != "" {
		if _, _, ret := store.Store.Get(tips.Get(protocol.TIP_SERVING_KEY)); ret.Error() != nil {
			log.Error("Error on get %s: %v", tips.Get(protocol.TIP_SERVING_KEY), ret.Error())
		} else {
			log.Trace("Delay to serve requested key %s", tips.Get(protocol.TIP_SERVING_KEY))
		}
	}
	for i := 0; i < opt.numToInsert; i++ {
		val := make([]byte, opt.sizeToInsert)
		rand.Read(val)
		if ret := store.Store.Set(fmt.Sprintf("obj-%d-%d", input.Id, int(input.Status.Metas[0].DiffRank)+i), "0", val); ret.Error() != nil {
			log.Error("Error on set obj-%d: %v", i, ret.Error())
		}
	}
	// Let ping request running without session timeout
	if opt.statusAsPayload {
		time.Sleep(10 * time.Second)
	}
	session.Timeout.DoneBusyWithReset(lambdaLife.TICK_ERROR, "dryrun")
}
