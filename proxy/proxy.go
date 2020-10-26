package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	syslog "log"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/dashboard"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/server"
	"github.com/mason-leap-lab/infinicache/proxy/server/cluster"
)

var (
	options = &global.Options
	log     = &logger.ColorLogger{Color: true, Level: logger.LOG_LEVEL_INFO}
)

func init() {
	global.Log = log
	if config.ServerPublicIp != "" {
		global.ServerIp = config.ServerPublicIp
	}
}

func main() {
	var done sync.WaitGroup
	checkUsage(options)
	if options.Debug {
		log.Level = logger.LOG_LEVEL_ALL
	}
	log.Color = !options.NoColor
	if options.LogFile != "" {
		logFile, err := os.OpenFile(path.Join(options.LogPath, options.LogFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		// logFile, err := os.OpenFile(path.Join(options.LogPath, options.LogFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			syslog.Panic(err)
		}
		defer logFile.Close()

		syslog.SetOutput(logFile)
	}

	// CPU profiling by default
	//defer profile.Start().Stop()

	// Initialize collector
	collector.Create(path.Join(options.LogPath, options.Prefix))

	clientLis, err := net.Listen("tcp", fmt.Sprintf(":%d", global.BasePort))
	if err != nil {
		log.Error("Failed to listen clients: %v", err)
		os.Exit(1)
		return
	}
	lambdaLis, err := net.Listen("tcp", fmt.Sprintf(":%d", global.BasePort+1))
	if err != nil {
		log.Error("Failed to listen lambdas: %v", err)
		os.Exit(1)
		return
	}
	log.Info("Start listening to clients(port 6378) and lambdas(port 6379)")

	// Register signals
	sig := make(chan os.Signal, 1)
	// Start Dashboard
	var dash *dashboard.Dashboard
	if !options.NoDashboard {
		dash = dashboard.NewDashboard()
		defer dash.Close()
		go func() {
			dash.Start()
			sig <- syscall.SIGINT
		}()
	} else {
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGABRT)
	}

	// initial proxy server
	srv := redeo.NewServer(nil)
	prxy := server.New(false)
	redis := server.NewRedisAdapter(srv, prxy, options.D, options.P)
	if dash != nil {
		dash.ConfigCluster(prxy.GetStatsProvider(), cluster.ExpireBucketsNum)
	}

	// config server
	srv.HandleStreamFunc(protocol.CMD_SET_CHUNK, prxy.HandleSet)
	srv.HandleFunc(protocol.CMD_GET_CHUNK, prxy.HandleGet)
	srv.HandleCallbackFunc(prxy.HandleCallback)

	// Log goroutine
	done.Add(1)
	go func() {
		<-sig
		log.Info("Receive signal, killing server...")
		close(sig)

		collector.Stop()

		// // Close server
		log.Info("Closing server...")
		srv.Close(clientLis)

		// // Collect data
		log.Info("Collecting data...")
		prxy.CollectData()

		prxy.Close(lambdaLis)
		redis.Close()
		done.Done()
	}()

	// initiate lambda store proxy
	go prxy.Serve(lambdaLis)
	prxy.WaitReady()
	if dash != nil {
		dash.ClusterView.Update()
	}

	// Pid is only written after ready
	err = ioutil.WriteFile(options.Pid, []byte(fmt.Sprintf("%d", os.Getpid())), 0640)
	if err != nil {
		log.Warn("Failed to write PID: %v", err)
	} else {
		defer os.Remove(options.Pid)
	}

	// Start serving (blocking)
	err = srv.ServeAsync(clientLis)
	if err != nil {
		select {
		case <-sig:
			// Normal close
		default:
			log.Error("Error on serve clients: %v", err)
		}
		srv.Release()
	}
	log.Info("Server closed.")

	// Wait for data collection
	done.Wait()
	prxy.Release()
	if dash != nil {
		dash.Update()
		time.Sleep(time.Second)
	}
	return
}

func checkUsage(options *global.CommandlineOptions) {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	flag.BoolVar(&options.Debug, "debug", false, "Enable debug and print debug logs.")
	flag.StringVar(&options.Prefix, "prefix", "log", "Prefix for data files.")
	flag.IntVar(&options.D, "d", 10, "The number of data chunks for build-in redis client.")
	flag.IntVar(&options.P, "p", 2, "The number of parity chunks for build-in redis client.")
	flag.BoolVar(&options.NoDashboard, "disable-dashboard", true, "Disable dashboard")
	showDashboard := flag.Bool("enable-dashboard", false, "Enable dashboard")
	flag.BoolVar(&options.NoColor, "disable-color", false, "Disable color log")
	flag.StringVar(&options.Pid, "pid", "/tmp/infinicache.pid", "Path to the pid.")
	flag.StringVar(&options.LogPath, "base", "", "Path to the log file.")
	flag.StringVar(&options.LogFile, "log", "", "File name of the log. If dashboard is not disabled, the default value is \"log\".")
	flag.BoolVar(&options.Evaluation, "enable-evaluation", false, "Enable evaluation settings.")
	flag.IntVar(&options.NumBackups, "numbak", 0, "EVALUATION ONLY: The number of backups used per node.")
	flag.BoolVar(&options.NoFirstD, "disable-first-d", false, "EVALUATION ONLY: Disable first-d optimization.")
	flag.Uint64Var(&options.FuncCapacity, "funcap", 0, "EVALUATION ONLY: Preset capacity(MB) of function instance.")

	flag.Parse()
	options.NoDashboard = !*showDashboard

	if printInfo {
		fmt.Fprintf(os.Stderr, "Usage: ./proxy [options]\n")
		fmt.Fprintf(os.Stderr, "Available options:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if !options.NoDashboard {
		if options.LogFile == "" {
			options.LogFile = "log"
		}
		options.NoColor = true
	}

	if options.Evaluation && options.FuncCapacity == 0 {
		fmt.Fprintf(os.Stderr, "Since evaluation is enabled, please specify the capacity of function instance with option \"-funcap\".\n")
		os.Exit(0);
	}
}
