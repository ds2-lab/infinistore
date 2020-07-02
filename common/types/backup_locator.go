package types

import (
	// "log"
	"strconv"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

var (
	ConsistentConfig = consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            backupHasher{},
	}
)

type backupMember int

func (m backupMember) String() string {
	return strconv.Itoa(int(m))
}

type backupHasher struct{}

func (h backupHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// Backups for a instace. If not specified, all operation are not thread safe.
type BackupLocator struct {
	enable bool
	ring   *consistent.Consistent
}

func (b *BackupLocator) Reset(num int) {
	if num == 0 {
		b.enable = false
		return
	}

	b.enable = true
	if b.ring == nil || len(b.ring.GetMembers()) != num {
		members := make([]consistent.Member, num)
		for i := 0; i < num; i++ {
			members[i] = backupMember(i)
		}
		b.ring = consistent.New(members, ConsistentConfig)
	}
}

func (b *BackupLocator) Locate(key string) (int, bool) {
	if !b.enable {
		return 0, false
	}

	return int(b.ring.LocateKey([]byte(key)).(backupMember)), true
}
