package main

import (
	"fmt"
	"io/ioutil"
	syslog "log"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/dashboard"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/server"
)

var (
	options  = &global.Options
	log      = &logger.ColorLogger{Color: true, Level: logger.LOG_LEVEL_INFO}
	sig      = make(chan os.Signal, 1)
	dash     *dashboard.Dashboard
	logFile  *os.File
	stdErr   *os.File = os.Stderr
	panicErr interface{}
)

func init() {
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT)
	global.Log = log
	if config.ServerPublicIp != "" {
		global.ServerIp = config.ServerPublicIp
	}
}

func main() {
	defer finalize(false) // We need to call finalize in every goroutine.

	var done sync.WaitGroup
	global.CheckUsage(options)
	if options.Debug {
		log.Level = logger.LOG_LEVEL_ALL
	}
	log.Color = !options.NoColor
	if options.LogFile != "" {
		logFile, panicErr = os.OpenFile(path.Join(options.LogPath, options.LogFile), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		// logFile, panicErr = os.OpenFile(path.Join(options.LogPath, options.LogFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if panicErr != nil {
			panic(panicErr)
		}

		syslog.SetOutput(logFile)
		os.Stderr = logFile
	}

	// CPU profiling by default
	//defer profile.Start().Stop()

	// Initialize collector
	collector.Create(path.Join(options.LogPath, options.Prefix))

	clientLis, err := net.Listen("tcp", fmt.Sprintf(":%d", global.BasePort))
	if err != nil {
		log.Error("Failed to listen clients: %v", err)
		return
	}
	lambdaLis, err := net.Listen("tcp", fmt.Sprintf(":%d", global.BasePort+1))
	if err != nil {
		log.Error("Failed to listen lambdas: %v", err)
		return
	}
	log.Info("Start listening to clients(port 6378) and lambdas(port 6379)")

	// Start Dashboard
	if !options.NoDashboard {
		dash = dashboard.NewDashboard()
		go func() {
			defer finalize(false)
			dash.Start()
			sig <- syscall.SIGINT
		}()
	}

	// initial proxy server
	srv := redeo.NewServer(nil)
	prxy := server.New()
	redis := server.NewRedisAdapter(srv, prxy, options.D, options.P)
	if dash != nil {
		dash.ConfigCluster(prxy.GetStatsProvider(), config.NumAvailableBuckets+1) // Show all unexpired instances + 1 expired bucket.
	}

	// config server
	srv.HandleStreamFunc(protocol.CMD_SET_CHUNK, prxy.HandleSetChunk)
	srv.HandleFunc(protocol.CMD_GET_CHUNK, prxy.HandleGetChunk)
	srv.HandleCallbackFunc(prxy.HandleCallback)

	// Log goroutine
	done.Add(1)
	go func() {
		<-sig
		log.Info("Receive signal, killing server...")
		close(sig)

		collector.Stop()

		// Close server
		log.Info("Closing server...")
		srv.Close(clientLis)

		// Uncomment me: on long running microbenchmarking.
		// Collect data
		// log.Info("Collecting data...")
		// prxy.CollectData()

		prxy.Close(lambdaLis)
		redis.Close()
		done.Done()
	}()

	// initiate lambda store proxy
	go func() {
		defer finalize(true)
		prxy.Serve(lambdaLis)
	}()
	prxy.WaitReady()
	if dash != nil {
		dash.Update()
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
	log.Debug("Server returned: %v", err)
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
}

func finalize(fix bool) {
	// Dashboard must be closed in any circumstance.
	if dash != nil {
		dash.Close()
		dash = nil
	}

	// If fixable, try close server normally.
	if fix {
		if err := recover(); err != nil {
			log.Error("%v", err)
			return
		}
	}

	// Rest will be cleared from main routine.
	if logFile != nil {
		os.Stderr = stdErr
		syslog.SetOutput(os.Stdout)
		logFile.Close()
		logFile = nil
	}
}
