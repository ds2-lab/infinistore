package lambdastore

import (
	"github.com/mason-leap-lab/go-utils/mapreduce"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type Backer interface {
	ReserveBacking() error
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
	instance   *Instance
	backups    []Backer
	candidates []*Instance
	required   int
	availables int
	locator    protocol.BackupLocator
	adapter    BackerGetter
	log        logger.ILogger
}

func NewBackups(ins *Instance, backups []Backer) *Backups {
	baks := &Backups{
		instance:   ins,
		backups:    make([]Backer, len(backups)),
		required:   len(backups),
		availables: len(backups),
		log:        ins.log,
	}
	copy(baks.backups, backups)
	baks.locator.Reset(len(baks.backups))
	return baks
}

func NewBackupsFromInstances(ins *Instance, backups []*Instance, adapter BackerGetter) *Backups {
	baks := &Backups{
		instance:   ins,
		backups:    make([]Backer, len(backups)),
		required:   len(backups),
		availables: len(backups),
		adapter:    adapter,
		log:        ins.log,
	}
	for i, ins := range backups {
		baks.backups[i] = baks.getBacker(ins)
	}
	baks.locator.Reset(len(baks.backups))
	return baks
}

func (b *Backups) ResetCandidates(required int, candidates []*Instance) {
	if cap(b.backups) < required {
		backups := make([]Backer, required)
		if len(b.backups) > 0 {
			copy(backups[:len(b.backups)], b.backups)
		}
		b.backups = backups
	}
	b.required = required
	b.candidates = candidates
	if b.log == nil {
		b.log = logger.NilLogger
	}
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

func (b *Backups) Reserve(fallback mapreduce.Iterator) int {
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
	closed := 0              // Number of candidates to be removed
	for i := 0; i < len(b.backups); i++ {
		if b.candidates[i] != nil {
			if err := b.getBacker(b.candidates[i]).ReserveBacking(); err == nil {
				changes += b.promoteCandidate(i, i)
				failures = 0
				continue
			} else if err == ErrInstanceClosed {
				b.candidates[i] = nil // remove from candidate list
			}
		}
		// Try find whatever possible
		for ; alters < len(b.candidates)-closed; alters++ {
			if err := b.getBacker(b.candidates[alters]).ReserveBacking(); err == nil {
				changes += b.promoteCandidate(i, alters)
				failures = 0
				// Switch demoted to tail if it is closed
				if b.candidates[alters] == nil {
					b.candidates[len(b.candidates)-closed-1], b.candidates[alters] = b.candidates[alters], b.candidates[len(b.candidates)-closed-1]
					closed++
					alters--
				}
				break
			} else if err == ErrInstanceClosed {
				// Switch candidate to tail if it is closed
				b.candidates[len(b.candidates)-closed-1], b.candidates[alters] = nil, b.candidates[len(b.candidates)-closed-1]
				closed++
				alters--
			}
		}
		if closed > 0 {
			b.candidates = b.candidates[:len(b.candidates)-closed]
			closed = 0
		}

		if alters < len(b.candidates) {
			// Found: advance alters
			alters++
		} else {
			// Nothing left
			changes += b.addToBackups(i, nil)
			failures++
		}
	}

	// Try fallback after all candidates tested, so duplicated fallback will not success.
	failovers := 0
	if fallback != nil {
		newFailures := 0

		for i := 0; i < len(b.backups); i++ {
			if b.backups[i] != nil {
				newFailures = 0
				continue
			} else if fallback == nil {
				newFailures++
				continue
			}

			found := false
			for j := 0; j < b.required; j++ { // Only try a limited time
				if !fallback.Next() {
					// Stucked, stop trying.
					b.log.Warn("%d failovers found before stop trying: attempt %d", failovers, j)
					break
				}

				_, cand := fallback.Value()
				candidate := cand.(*Instance)
				if candidate.Name() == b.instance.Name() {
					continue
				} else if err := candidate.ReserveBacking(); err == nil {
					b.candidates[i] = candidate
					b.addToBackups(i, candidate)
					newFailures = 0
					failovers++
					found = true
					break
				} // try next
			}

			if !found {
				newFailures++
			}
		}
		failures = newFailures
	}
	if failovers > 0 {
		b.log.Info("%d failover backups added.", failovers)
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

func (b *Backups) Locator() *protocol.BackupLocator {
	return &b.locator
}

// This function is thread safe
func (b *Backups) GetByKey(key string) (*Instance, bool) {
	loc, required, ok := b.locator.Locate(key)
	if !ok {
		return nil, false
	}

	return b.GetByLocation(loc, required)
}

func (b *Backups) GetByHash(hash uint64) (*Instance, bool) {
	loc, required, ok := b.locator.LocateByHash(hash)
	if !ok {
		return nil, false
	}

	return b.GetByLocation(loc, required)
}

func (b *Backups) GetByLocation(loc int, required int) (*Instance, bool) {
	if loc >= len(b.backups) || loc > required {
		return nil, false
	}

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
