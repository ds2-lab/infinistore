package lambdastore

import (
	"log"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	readinessReady    = testBacker(0)
	readinessBusy     = testBacker(1)
	readinessReserved = testBacker(2)
)

type testBacker int

func (b *testBacker) Id() uint64 {
	return uint64(*b)
}

func (b *testBacker) ReserveBacking() error {
	if *b == testBacker(readinessReady) {
		*b = testBacker(readinessReserved)
		return nil
	}
	return ErrReservationFailed
}

func (b *testBacker) StartBacking(ins *Instance, i int, all int) bool {
	if *b == testBacker(readinessReserved) {
		*b = testBacker(readinessBusy)
		return true
	}
	return false
}

func (b *testBacker) StopBacking(ins *Instance) {
	*b = testBacker(readinessReady)
}

type testBackups struct {
	*Backups
	readiness []testBacker
}

func (b *testBackups) toBacker(ins *Instance) Backer {
	return &b.readiness[ins.Id()]
}

func (b *testBackups) toInstance(backer Backer, i int) *Instance {
	return b.Backups.candidates[i]
}

func newBackups(bak, cand int) *testBackups {
	backups := &Backups{}
	ret := &testBackups{Backups: backups}
	ret.Backups.adapter = ret

	candidates := make([]*Instance, cand)
	for i := 0; i < cand; i++ {
		candidates[i] = NewInstance("Test", uint64(i))
	}
	backups.ResetCandidates(bak, candidates)
	return ret
}

func setTestCase(backups *testBackups, readiness []testBacker) {
	backups.readiness = readiness
}

func testAllReserved(backups *testBackups, readiness []testBacker) {
	iter := backups.Iter()
	iterated := 0
	Expect(iter.Len()).To(Equal(backups.Availables()))
	for iter.Next() {
		i, backup := iter.Value()
		// if !readiness[backup.Id()] {
		// 	log.Printf("detect false: %d:%d", i, backup.Id())
		// }
		Expect(i).To(Equal(iterated))
		Expect(readiness[backup.(*Instance).Id()]).To(Equal(readinessReserved))
		iterated++
	}
}

func getRandomReadiness(cand int, maxf int) []testBacker {
	readiness := make([]testBacker, cand)
	failures := maxf
	if maxf > 0 {
		failures = rand.Intn(maxf)
	}
	for i := 0; i < failures; i++ {
		readiness[i] = testBacker(readinessBusy)
	}
	rand.Shuffle(cand, func(i int, j int) {
		readiness[j], readiness[i] = readiness[i], readiness[j]
	})
	return readiness
}

var _ = Describe("Backups", func() {
	rand.Seed(time.Now().UnixNano())

	It("should all backups be reserved", func() {
		backups := newBackups(5, 7)
		var readiness []testBacker

		// Fill up
		readiness = []testBacker{readinessReady, readinessReady, readinessReady, readinessBusy, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(5))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]
		backups.Start(nil)
		backups.Stop(nil)

		// Keep consistent
		readiness = []testBacker{readinessReady, readinessReady, readinessReady, readinessReady, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(0))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]
		backups.Start(nil)
		backups.Stop(nil)

		// Used up
		readiness = []testBacker{readinessReady, readinessReady, readinessBusy, readinessReady, readinessReady, readinessBusy, readinessReady}
		setTestCase(backups, readiness)
		changed := backups.Reserve(nil)
		Expect(changed).To(Equal(2))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 3, 6, 4] [2, 5]
		backups.Start(nil)
		backups.Stop(nil)

		// Test hole
		readiness = []testBacker{readinessReady, readinessBusy, readinessBusy, readinessReady, readinessReady, readinessBusy, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(4))
		testAllReserved(backups, readiness)
		// status: [0, 1(nil), 3, 6, 4] [2, 5]
		backups.Start(nil)
		backups.Stop(nil)

		// Hole be filled
		readiness = []testBacker{readinessReady, readinessBusy, readinessBusy, readinessReady, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 5, 3, 6, 4] [2, 1]
		backups.Start(nil)
		backups.Stop(nil)

		// Test smaller length
		readiness = []testBacker{readinessBusy, readinessReady, readinessBusy, readinessBusy, readinessBusy, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(3))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(3))
		testAllReserved(backups, readiness)
		// status: [1, 5, 3(nil), 6], [4] [2, 0]
		backups.Start(nil)
		backups.Stop(nil)
	})

	It("should all backups be reserved: random simulation", func() {
		backups := newBackups(20, 40)
		changes := 0
		for i := 0; i < 10000; i++ {
			readiness := getRandomReadiness(40, 4)
			setTestCase(backups, readiness)
			changes += backups.Reserve(nil)
			testAllReserved(backups, readiness)
			// log.Printf("%d passed", i)
		}

		avg := float64(changes) / 10000
		log.Printf("%f changes", avg)
		Expect(avg).To(BeNumerically("<", 1))
	})

	It("should be able to fill backups with more one redundant candidates", func() {
		backups := newBackups(20, 40)
		readiness := getRandomReadiness(40, 0)
		readiness[backups.candidates[1].Id()] = readinessBusy
		readiness[backups.candidates[4].Id()] = readinessBusy
		setTestCase(backups, readiness)

		backups.Reserve(nil)
		testAllReserved(backups, readiness)
	})

	It("should GetByKey be safe in case the length of backups is smaller than required", func() {
		required := 20
		backups := newBackups(required, required)
		readiness := getRandomReadiness(required, 0)
		readiness[backups.candidates[required-1].Id()] = readinessBusy
		setTestCase(backups, readiness)

		backups.Reserve(nil)
		Expect(len(backups.backups)).To(Equal(required - 1))

		ins, ok := backups.GetByHash(uint64(required - 1))
		Expect(ins).To(BeNil())
		Expect(ok).To(BeFalse())
	})

	It("should start again after stoped", func() {
		required := 20
		backups := newBackups(required, required)
		readiness := getRandomReadiness(required, 0)
		readiness[backups.candidates[required-1].Id()] = readinessBusy
		setTestCase(backups, readiness)

		backups.Reserve(nil)
		Expect(len(backups.backups)).To(Equal(required - 1))

		ins, ok := backups.GetByHash(uint64(required - 1))
		Expect(ins).To(BeNil())
		Expect(ok).To(BeFalse())
	})
})

// func dumpInstance(all []*Instance) {
// 	ids := make([]uint64, len(all))
// 	for i := 0; i < len(all); i++ {
// 		ids[i] = all[i].Id()
// 	}
// 	fmt.Println(ids)
// }
