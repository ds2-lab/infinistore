package client

import (
	"fmt"
	"os"
	"time"

	"github.com/ScottMansfield/nanolog"
)

var (
	logClient nanolog.Handle
	nlogger   func(nanolog.Handle, ...interface{}) error
)

func init() {
	// cmd, reqId, begin, duration, get/set req latency, rec latency, decoding latency, all good, corrupted, size
	logClient = nanolog.AddLogger("%s,%s,%i64,%i64,%i64,%i64,%i64,%b,%b,%i")
}

type logEntry struct {
	Cmd           string
	ReqId         string
	Start         time.Time
	ReqLatency    time.Duration
	RecLatency    time.Duration
	CodingLatency time.Duration
	Duration      time.Duration
	AllGood       bool
	Corrupted     bool
}

func (e *logEntry) Begin(reqId string) {
	e.ReqId = reqId
	e.Start = time.Now()
}

func (e *logEntry) Since() time.Duration {
	return time.Since(e.Start)
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
