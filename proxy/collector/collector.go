package collector

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ScottMansfield/nanolog"

	"github.com/mason-leap-lab/infinicache/proxy/global"
)

const (
	LogTypeCluster      = "cluster"
	LogTypeBucketRotate = "bucket"
)

var (
	Enable bool

	LogChunk        nanolog.Handle
	LogEndtoEnd     nanolog.Handle
	LogCluster      nanolog.Handle
	LogBucketRotate nanolog.Handle

	// LogRequestStart Component of LogChunk, fields: cmd, reqId, chunk, startedAt
	LogRequestStart nanolog.Handle = 10001
	// LogRequestValidation Component of LogChunk, fields: validatedAt
	LogRequestValidation nanolog.Handle = 10002
	// LogRequestFuncResponse Component of LogChunk, fields: firstByteReceivedAt, headerReceivedAt, bodyReceivedAt, recoveryFlag
	LogRequestFuncResponse nanolog.Handle = 10003
	// LogRequestProxyResponse Component of LogChunk, fields: headerSentAt, bodySentAt, allSentAt, endedAt
	LogRequestProxyResponse nanolog.Handle = 10004
	// LogRequestAbadon Abandon request log entry.
	LogRequestAbandon nanolog.Handle = 10009

	ErrUnexpectedEntry = errors.New("unexpected log entry")

	ticker       *time.Ticker
	stopped      bool
	lastActivity = time.Now()
	pool         = sync.Pool{
		New: func() interface{} {
			return &DataEntry{}
		},
	}
)

func init() {
	// cmd, reqId, chunk, start, duration, firstByte, header from lambda, header to client, obsolete1, obsolete2, streaming, ping, status
	LogChunk = nanolog.AddLogger("%s,%s,%s,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64")
	// cmd, status, bytes, start, duration
	LogEndtoEnd = nanolog.AddLogger("%s,%s,%i64,%i64,%i64")
	// type(cluster), time, total, actives, degraded, expired
	LogCluster = nanolog.AddLogger("%s,%i64,%i,%i,%i,%i")
	// type(bucket), time, migrated, remain, degraded, expired
	LogBucketRotate = nanolog.AddLogger("%s,%i64,%i,%i,%i,%i")
}

func Create(prefix string) {
	// get local time
	//location, _ := time.LoadLocation("EST")
	// Set up nanoLog writer
	//nanoLogout, err := os.Create("/tmp/proxy/" + *prefix + "_proxy.clog")
	nanoLogout, err := os.Create(prefix + "_proxy.clog")
	if err != nil {
		panic(err)
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		panic(err)
	}

	Enable = true
	go func() {
		ticker = time.NewTicker(1 * time.Second)
		for {
			<-ticker.C
			if stopped || time.Since(lastActivity) >= 10*time.Second {
				if err := nanolog.Flush(); err != nil {
					global.Log.Warn("Failed to save data: %v", err)
				}
			}
			if stopped {
				return
			}
		}
	}()
}

func Stop() {
	stopped = true
	if ticker != nil {
		ticker.Stop()
	}
}

func Flush() error {
	return nanolog.Flush()
}

type DataEntry struct {
	cmd           string
	reqId         string
	chunkId       string
	start         int64
	duration      int64
	firstByte     int64
	lambda2Server int64
	server2Client int64
	readBulk      int64
	appendBulk    int64
	flush         int64
	validate      int64
	status        int64
	mu            sync.Mutex
}

func (e *DataEntry) reset() *DataEntry {
	e.cmd = ""
	e.reqId = ""
	e.chunkId = ""
	e.start = 0
	e.duration = 0
	e.firstByte = 0
	e.lambda2Server = 0
	e.server2Client = 0
	e.readBulk = 0
	e.appendBulk = 0
	e.flush = 0
	e.validate = 0
	return e
}

func Collect(handle nanolog.Handle, args ...interface{}) error {
	lastActivity = time.Now()
	return nanolog.Log(handle, args...)
}

func CollectRequest(handle nanolog.Handle, e interface{}, args ...interface{}) (interface{}, error) {
	lastActivity = time.Now()
	if !Enable {
		return nil, nil
	}

	if handle == LogRequestStart {
		entry := pool.Get().(*DataEntry).reset()
		entry.cmd = args[0].(string)
		entry.reqId = args[1].(string)
		entry.chunkId = args[2].(string)
		entry.start = args[3].(int64)
		return entry, nil
	}

	entry, ok := e.(*DataEntry)
	if !ok {
		return nil, ErrUnexpectedEntry
	}
	switch handle {
	case LogRequestValidation:
		entry.validate = args[0].(int64)
		return entry, nil
	case LogRequestFuncResponse:
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.firstByte = args[0].(int64) - entry.start
		entry.lambda2Server = args[1].(int64)
		entry.readBulk = args[2].(int64)
		if len(args) > 3 {
			entry.status = args[3].(int64)
		}
		if entry.duration == 0 {
			return entry, nil
		}
		// Duration has been set, because the request is late and finished. Continue and complete LogChunk
	case LogRequestProxyResponse:
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.server2Client = args[0].(int64)
		entry.appendBulk = args[1].(int64)
		entry.flush = args[2].(int64)
		entry.duration = args[3].(int64) - entry.start
		// For normal request, firstByte must be nonzero.
		if entry.firstByte == 0 {
			// Late request can be never sent to the lambda, so the firstByte is zero. These ignored request will not be logged.
			return entry, nil
		}
		// All done. Continue and complete LogChunk
	case LogRequestAbandon:
		pool.Put(entry)
		return nil, nil
	default:
		return nil, ErrUnexpectedEntry
	}

	defer pool.Put(entry)
	return nil, nanolog.Log(LogChunk, fmt.Sprintf("%schunk", entry.cmd), entry.reqId, entry.chunkId,
		entry.start, entry.duration,
		entry.firstByte, entry.lambda2Server, entry.server2Client,
		entry.readBulk, entry.appendBulk, entry.flush, entry.validate, entry.status)
}
