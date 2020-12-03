package types

import (
	// "github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// var (
// 	ConsistentConfig = consistent.Config{
// 		PartitionCount:    1001,
// 		ReplicationFactor: 1,
// 		Load:              1.01,
// 		Hasher:            backupHasher{},
// 	}
// )

// type backupMember int

// func (m backupMember) String() string {
// 	return strconv.Itoa(int(m))
// }

// type backupHasher struct{}

// func (h backupHasher) Sum64(data []byte) uint64 {
// 	return xxhash.Sum64(data)
// }

// Backups for a instace. If not specified, all operation are not thread safe.
type BackupLocator struct {
	enable  bool
	backups int
	// ring    *consistent.Consistent
}

func (b *BackupLocator) Reset(num int) {
	if num == 0 {
		b.enable = false
		return
	}

	b.backups = num
	b.enable = true
	// if b.ring == nil || len(b.ring.GetMembers()) != num {
	// 	members := make([]consistent.Member, num)
	// 	for i := 0; i < num; i++ {
	// 		members[i] = backupMember(i)
	// 	}
	// 	b.ring = consistent.New(members, ConsistentConfig)
	// }
}

func (b *BackupLocator) Locate(key string) (int, int, bool) {
	if !b.enable {
		return 0, 0, false
	}

	// Simple hashing. Default implementation
	return int(xxhash.Sum64([]byte(key)) % uint64(b.backups)), b.backups, true

	// Consistent hashing
	// return int(b.ring.LocateKey([]byte(key)).(backupMember)), true
}

func (b *BackupLocator) LocateByHash(hash uint64) (int, int, bool) {
	if !b.enable {
		return 0, 0, false
	}

	// Simple hashing. Default implementation
	return int(hash % uint64(b.backups)), b.backups, true
}
