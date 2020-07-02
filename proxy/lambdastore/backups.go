package lambdastore

import (
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type BackupReserver func(*Instance) bool

type BackupIterator struct {
	i       int
	len     int
	skipped int
	backups []*Instance
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
	return iter.i - iter.skipped, iter.backups[iter.i]
}

// Backups for a instace. If not specified, all operation are not thread safe.
type Backups struct {
	Reserver BackupReserver

	backups    []*Instance
	candidates []*Instance
	availables int
	locator    protocol.BackupLocator
}

func (b *Backups) Reset(num int, candidates []*Instance) {
	if cap(b.backups) < num {
		backups := make([]*Instance, len(b.backups), num)
		if len(b.backups) > 0 {
			copy(backups, b.backups)
		}
		b.backups = backups
	}
	b.candidates = candidates
}

func (b *Backups) Len() int {
	return cap(b.backups)
}

func (b *Backups) Availables() int {
	return b.availables
}

func (b *Backups) Iter() *BackupIterator {
	return &BackupIterator{i: -1, len: b.availables, backups: b.backups}
}

func (b *Backups) Reserve(fallback *Instance) int {
	if cap(b.backups) == 0 {
		return 0
	}

	// Reset backups
	b.backups = b.backups[:cap(b.backups)] // Reset to full size
	b.availables = 0

	// Reserve backups so we can know how many backups are available
	changes := 0
	alters := len(b.backups) // Alternates start from "alters"
	failures := 0            // Continous unavailables
	for i := 0; i < len(b.backups); i++ {
		if b.reserve(b.candidates[i]) {
			changes += b.promoteCandidate(i, i)
			failures = 0
			continue
		}
		// Nothing left
		if alters == len(b.candidates) {
			changes += b.addToBackups(i, fallback)
			failures++
			continue
		}
		// Try find whatever possible
		for ; alters < len(b.candidates); alters++ {
			if b.reserve(b.candidates[alters]) {
				changes += b.promoteCandidate(i, alters)
				failures = 0
				break
			}
		}
		// Nothing left
		if alters == len(b.candidates) {
			changes += b.addToBackups(i, fallback)
			failures++
		}
	}

	// Try shrink backups to eliminate unnecessary checkes
	if failures > 0 {
		b.backups = b.backups[:cap(b.backups)-failures]
	}

	b.locator.Reset(len(b.backups))

	return changes
}

func (b *Backups) Invalidate() {
	b.availables = 0
	b.backups = b.backups[:0]
}

// Helper functions
func (b *Backups) Start(target *Instance) int {
	if len(b.backups) == 0 {
		return 0
	}
	for i, backup := range b.backups {
		if backup != nil {
			backup.StartBacking(target, i, cap(b.backups))
		}
	}
	return b.availables
}

func (b *Backups) StartByIndex(i int, target *Instance) (*Instance, bool) {
	if i >= len(b.backups) || b.backups[i] == nil {
		return nil, false
	} else {
		b.backups[i].StartBacking(target, i, cap(b.backups))
		return b.backups[i], true
	}
}

func (b *Backups) Stop(target *Instance) {
	for _, backup := range b.backups {
		if backup != nil {
			backup.StopBacking(target)
		}
	}
	// Ensure a second call to ResumeServing is safe
	b.Invalidate()
}

// This function is thread safe
func (b *Backups) GetByKey(key string) (*Instance, bool) {
	loc, ok := b.locator.Locate(key)
	if !ok {
		return nil, false
	}

	// Copy pointer to ensure the instance will not change
	backup := b.backups[loc]
	if backup == nil {
		return nil, false
	}

	return backup, true
}

func (b *Backups) reserve(ins *Instance) bool {
	if b.Reserver == nil {
		return ins.ReserveBacking()
	} else {
		return b.Reserver(ins)
	}
}

func (b *Backups) promoteCandidate(dest int, src int) int {
	if dest != src {
		// Exchange candidates to keep the next elections get a stable result
		b.candidates[dest], b.candidates[src] = b.candidates[src], b.candidates[dest]
	}
	return b.addToBackups(dest, b.candidates[dest])
}

func (b *Backups) addToBackups(dest int, ins *Instance) int {
	change := b.backups[dest] != ins
	b.backups[dest] = ins
	if ins != nil {
		b.availables++
	}
	if change {
		return 1
	} else {
		return 0
	}
}
