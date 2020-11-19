package collector

import (
	"bytes"
	"strconv"
	"time"
)

// For requests
type RequestEntry struct {
	Time      time.Time
	Op        int
	Status    string
	ReqId     string
	ChunkId   string
	DHandling time.Duration // Duration for lambda to handle request.
	DResponse time.Duration // Duration for lambda to transmit response.
	Duration  time.Duration // Duration for lambda side latency.
	DPost     time.Duration // Duration for post handling
	Session   string
}

func AddRequest(ts time.Time, op int, status, reqId, chunk string, d1, d2, d, dp time.Duration, session string) {
	if Enables&COLLECT_REQUEST > 0 {
		Send(&RequestEntry{ts, op, status, reqId, chunk, d1, d2, d, dp, session})
	}
}

func (e *RequestEntry) WriteTo(buf *bytes.Buffer) {
	buf.WriteString(strconv.FormatInt(e.Time.UnixNano(), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.Op))
	buf.WriteRune(',')
	buf.WriteString(e.Status)
	buf.WriteRune(',')
	buf.WriteString(e.ReqId)
	buf.WriteRune(',')
	buf.WriteString(e.ChunkId)
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.DHandling), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.DResponse), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.Duration), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.DPost), 10))
	buf.WriteRune(',')
	buf.WriteString(e.Session)
}
