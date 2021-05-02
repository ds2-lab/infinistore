package lambdastore

import (
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type Backer interface {
	ReserveBacking() bool
	StartBacking(*Instance, int, int) bool
	StopBacking(*Instance)
}

type BackerGetter interface {
	toBacker(*Instance) Backer
	toInstance(Backer, int) *Instance
}

type BackupIterator struct {
	backend *Backups
	i       int
	len     int
	skipped int
	backups []Backer
}

func (iter *BackupIterator) Len() int {
	return iter.len
}

func (iter *BackupIterator) Next() bool {
	for iter.i++; iter.i < len(iter.backups); iter.i++ {
		if iter.backups[iter.i] != nil {
			return true
		} else {
			iter.skipped++
		}
	}
	return false
}

func (iter *BackupIterator) Value() (int, interface{}) {
	return iter.i - iter.skipped, iter.backend.getInstance(iter.backups[iter.i], iter.i)
}

// Backups for a instace. If not specified, all operation are not thread safe.
type Backups struct {
	backups    []Backer
	candidates []*Instance
	required   int
	availables int
	locator    protocol.BackupLocator
	adapter    BackerGetter
}

func (b *Backups) Reset(num int, candidates []*Instance) {
	if cap(b.backups) < num {
		backups := make([]Backer, num)
		if len(b.backups) > 0 {
			copy(backups[:len(b.backups)], b.backups)
		}
		b.backups = backups
	}
	b.required = num
	b.candidates = candidates
}

func (b *Backups) Len() int {
	return b.required
}

func (b *Backups) Availables() int {
	return b.availables
}

func (b *Backups) Iter() *BackupIterator {
	return &BackupIterator{backend: b, i: -1, len: b.availables, backups: b.backups}
}

func (b *Backups) Reserve(fallback *Instance) int {
	if b.required == 0 {
		return 0
	}

	// Reset backups
	if len(b.backups) < b.required {
		b.backups = b.backups[:b.required] // Reset to full size
	}
	b.availables = 0

	// Reserve backups so we can know how many backups are available
	// To minimize backups' zigzag, holes are keep in place
	changes := 0
	alters := len(b.backups) // Alternates start from "alters"
	failures := 0            // Right most continous unavailables
	for i := 0; i < len(b.backups); i++ {
		if b.getBacker(b.candidates[i]).ReserveBacking() {
			changes += b.promoteCandidate(i, i)
			failures = 0
			continue
		}
		// Nothing left
		if alters >= len(b.candidates) {
			changes += b.addToBackups(i, fallback)
			failures++
			continue
		}
		// Try find whatever possible
		for ; alters < len(b.candidates); alters++ {
			if b.getBacker(b.candidates[alters]).ReserveBacking() {
				changes += b.promoteCandidate(i, alters)
				failures = 0
				break
			}
		}
		// Nothing left
		if alters >= len(b.candidates) {
			changes += b.addToBackups(i, fallback)
			failures++
		} else {
			// Advance alters
			alters++
		}
	}

	b.locator.Reset(len(b.backups))

	// Try shrink backups to eliminate unnecessary checks
	if failures > 0 {
		b.backups = b.backups[:len(b.backups)-failures]
	}

	return changes
}

func (b *Backups) Invalidate() {
	// b.required = 0
	b.availables = 0
	b.backups = b.backups[:0]
}

// Helper functions
func (b *Backups) Start(target *Instance) int {
	if len(b.backups) == 0 {
		return 0
	}

	backups := b.backups
	for i, backup := range backups {
		if backup != nil {
			backup.StartBacking(target, i, b.required)
		}
	}
	return b.availables
}

func (b *Backups) StartByIndex(i int, target *Instance) (*Instance, bool) {
	// Copy slide node for thread safety.
	backups := b.backups
	if i >= len(backups) || backups[i] == nil {
		return nil, false
	} else {
		go backups[i].StartBacking(target, i, b.required) // Use go routing to avoid deadlock
		return b.getInstance(backups[i], i), true
	}
}

func (b *Backups) Stop(target *Instance) {
	for _, backup := range b.backups {
		if backup != nil {
			go backup.StopBacking(target) // Use go routing to avoid deadlock
		}
	}
	// Ensure a second call to ResumeServing is safe
	b.Invalidate()
}

// This function is thread safe
func (b *Backups) GetByKey(key string) (*Instance, bool) {
	loc, required, ok := b.locator.Locate(key)
	if !ok {
		return nil, false
	}

	return b.getByLocation(loc, required)
}

func (b *Backups) GetByHash(hash uint64) (*Instance, bool) {
	loc, required, ok := b.locator.LocateByHash(hash)
	if !ok {
		return nil, false
	}

	return b.getByLocation(loc, required)
}

func (b *Backups) getByLocation(loc int, required int) (*Instance, bool) {
	// Copy pointer to ensure the instance will not change
	backups := b.backups[:required]
	backup := backups[loc]
	if backup == nil {
		return nil, false
	}

	return b.getInstance(backup, loc), true
}

func (b *Backups) promoteCandidate(dest int, src int) int {
	changes := b.addToBackups(dest, b.candidates[src])
	if dest != src {
		// Exchange candidates to keep the next elections get a stable result
		b.candidates[dest], b.candidates[src] = b.candidates[src], b.candidates[dest]
	}
	return changes
}

func (b *Backups) addToBackups(dest int, ins *Instance) int {
	change := b.getInstance(b.backups[dest], dest) != ins
	// fmt.Printf("Compared backups %v - %v: %v\n", b.describeInstance(b.getInstance(b.backups[dest], dest)), b.describeInstance(ins), change)
	b.backups[dest] = b.getBacker(ins)
	if ins != nil {
		b.availables++
	}
	if change {
		return 1
	} else {
		return 0
	}
}

func (b *Backups) getBacker(ins *Instance) Backer {
	if ins == nil {
		return nil
	}
	if b.adapter != nil {
		return b.adapter.toBacker(ins)
	}
	return ins
}

func (b *Backups) getInstance(backer Backer, i int) *Instance {
	if backer == nil {
		return nil
	}
	if b.adapter != nil {
		return b.adapter.toInstance(backer, i)
	}
	return backer.(*Instance)
}

// func (b *Backups) describeInstance(ins *Instance) string {
// 	if ins == nil {
// 		return "<nil>"
// 	} else {
// 		return strconv.FormatUint(ins.Id(), 10)
// 	}
// }
