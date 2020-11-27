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
	Enable           bool
	LogServer2Client nanolog.Handle
	//LogServer        nanolog.Handle
	//LogServerBufio   nanolog.Handle
	LogProxy           nanolog.Handle = 10001
	LogChunk           nanolog.Handle = 10002
	LogStart           nanolog.Handle = 10003
	LogLambda          nanolog.Handle = 10004
	LogValidate        nanolog.Handle = 10005
	LogEndtoEnd        nanolog.Handle = 20000
	LogCluster         nanolog.Handle
	LogBucketRotate    nanolog.Handle
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
	// cmd, reqId, chunk, start, duration, firstByte, header from lambda, header to client, obsolete1, obsolete2, streaming, ping
	LogChunk = nanolog.AddLogger("%s,%s,%s,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64")
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

func CollectRequest(handle nanolog.Handle, entry *DataEntry, args ...interface{}) (*DataEntry, error) {
	lastActivity = time.Now()
	if !Enable {
		return nil, nil
	}

	if handle == LogStart {
		entry := pool.Get().(*DataEntry).reset()
		entry.cmd = args[0].(string)
		entry.reqId = args[1].(string)
		entry.chunkId = args[2].(string)
		entry.start = args[3].(int64)
		return entry, nil
	} else if handle == LogValidate {
		entry.validate = args[3].(int64)
		return entry, nil
	} else if handle == LogProxy {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.firstByte = args[3].(int64) - entry.start
		entry.lambda2Server = args[4].(int64)
		entry.readBulk = args[5].(int64)
		// For late request, duration must be set.
		if entry.duration == 0 {
			return entry, nil
		}
	} else if handle == LogServer2Client {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.server2Client = args[3].(int64)
		entry.appendBulk = args[4].(int64)
		entry.flush = args[5].(int64)
		entry.duration = args[6].(int64) - entry.start
		// For normal request, firstByte must be set.
		if entry.firstByte == 0 {
			return entry, nil
		}
	} else {
		return nil, ErrUnexpectedEntry
	}

	pool.Put(entry)
	return nil, nanolog.Log(LogChunk, fmt.Sprintf("%schunk", entry.cmd), entry.reqId, entry.chunkId,
		entry.start, entry.duration,
		entry.firstByte, entry.lambda2Server, entry.server2Client,
		entry.readBulk, entry.appendBulk, entry.flush, entry.validate)
}
