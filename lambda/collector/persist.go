package collector

import (
	"bytes"
	"strconv"
	"time"
)

// For requests
type PersistEntry struct {
	Time         time.Time
	Op           int
	Type         int
	Id           uint64
	BackupId     int
	DLineage     time.Duration // Duration for lambda to handle request.
	DObjects     time.Duration // Duration for lambda to transmit response.
	Duration     time.Duration // Duration for lambda side latency.
	BytesLineage int
	BytesObjects int
	Objects      int
	Session      string
}

func AddRecovery(ts time.Time, op int, t int, id uint64, backKey int, d1, d2, d time.Duration, b1, b2, o int) {
	if addPersist(ts, op, t, id, backKey, d1, d2, d, b1, b2, o) {
		SaveWithOption(true) // Wait to be saved.
	}
}

func AddCommit(ts time.Time, op int, t int, id uint64, backKey int, d1, d2, d time.Duration, b1, b2 int) {
	addPersist(ts, op, t, id, backKey, d1, d2, d, b1, b2, 0)
}

func addPersist(ts time.Time, op int, t int, id uint64, backKey int, d1, d2, d time.Duration, b1, b2, o int) bool {
	if Enables&COLLECT_PERSIST > 0 {
		Send(&PersistEntry{ts, op, t, id, backKey, d1, d2, d, b1, b2, o, Session.Id})
		return true
	} else {
		return false
	}
}

func (e *PersistEntry) WriteTo(buf *bytes.Buffer) {
	buf.WriteString(strconv.FormatInt(e.Time.UnixNano(), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.Op))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.Type))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatUint(e.Id, 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.BackupId))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.DLineage), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.DObjects), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.FormatInt(int64(e.Duration), 10))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.BytesLineage))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.BytesObjects))
	buf.WriteRune(',')
	buf.WriteString(strconv.Itoa(e.Objects))
	buf.WriteRune(',')
	buf.WriteString(e.Session)
}
