package collector

import (
	"bytes"
	"strconv"
	"time"
)

// For requests
type PersistEntry struct {
	Op             int
	Backup         bool
	Id             uint64
	BackupId       int
	DLineage       time.Duration    // Duration for lambda to handle request.
	DObjects       time.Duration    // Duration for lambda to transmit response.
	Duration       time.Duration    // Duration for lambda side latency.
	BytesLineage   int
	BytesObjects   int
	Objects        int
	Session        string
}

func AddRecovery(op int, backup bool, id uint64, backKey int, d1, d2, d time.Duration, b1, b2, o int) {
	if addPersist(op, backup, id, backKey, d1, d2, d, b1, b2, o) {
		Save()
	}
}

func AddCommit(op int, backup bool, id uint64, backKey int, d1, d2, d time.Duration, b1, b2 int) {
	addPersist(op, backup, id, backKey, d1, d2, d, b1, b2, 0)
}

func addPersist(op int, backup bool, id uint64, backKey int, d1, d2, d time.Duration, b1, b2, o int) bool {
	if Enables & COLLECT_PERSIST > 0 {
		Send(&PersistEntry{op, backup, id, backKey, d1, d2, d, b1, b2, o, Session.Id})
		return true
	} else {
		return false
	}
}

func (e *PersistEntry) WriteTo(buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(e.Op))
	buf.WriteRune(',')
	if e.Backup {
		buf.WriteString("1")
	} else {
		buf.WriteString("0")
	}
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
