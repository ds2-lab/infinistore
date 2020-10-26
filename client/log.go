package client

import (
	"fmt"
	"os"
	"time"

	"github.com/ScottMansfield/nanolog"
)

var (
	logRec    nanolog.Handle
	logDec    nanolog.Handle
	logClient nanolog.Handle
	nlogger   func(nanolog.Handle, ...interface{}) error
)

type logEntry struct {
	Cmd        string
	ReqId      string
	Begin      time.Time
	ReqLatency time.Duration
	RecLatency time.Duration
	Duration   time.Duration
	AllGood    bool
	Corrupted  bool
}

func init() {
	//LogClient = nanolog.AddLogger("%s All goroutine has finished. Duration is %s")
	logRec = nanolog.AddLogger("chunk id is %i, " +
		"Client send RECEIVE req timeStamp is %s " +
		"Client Peek ChunkId time is %s" +
		"Client read ChunkId time is %s " +
		"Client Peek chunkBody time is %s " +
		"Client read chunkBody time is %s " +
		"RECEIVE goroutine duration time is %s ")
	logDec = nanolog.AddLogger("DataStatus is %b, Decoding time is %s")
	// cmd, reqId, Begin, duration, get/set req latency, rec latency, decoding latency
	logClient = nanolog.AddLogger("%s,%s,%i64,%i64,%i64,%i64,%i64,%b,%b")
}

// CreateLog Enabling evaluation log in client lib.
func CreateLog(opts map[string]interface{}) {
	path := opts["file"].(string) + "_bench.clog"
	nanoLogout, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		panic(err)
	}
	SetLogger(nanolog.Log)
}

// FlushLog Flush logs to the file.y
func FlushLog() {
	if err := nanolog.Flush(); err != nil {
		fmt.Println("log flush err")
	}
}

// SetLogger set customized evaluation logger
func SetLogger(l func(nanolog.Handle, ...interface{}) error) {
	nlogger = l
}

func nanoLog(handle nanolog.Handle, args ...interface{}) error {
	if nlogger != nil {
		return nlogger(handle, args...)
	}
	return nil
}
