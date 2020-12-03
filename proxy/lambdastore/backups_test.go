package lambdastore

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	readinessReady    = 0
	readinessBusy     = 1
	readinessReserved = 2
)

func newBackups(bak, cand int) *Backups {
	backups := &Backups{}
	candidates := make([]*Instance, cand)
	for i := 0; i < cand; i++ {
		candidates[i] = NewInstance("Test", uint64(i))
	}
	backups.Reset(bak, candidates)
	return backups
}

func setTestCase(backups *Backups, readiness []int) {
	backups.Reserver = func(ins *Instance) bool {
		// log.Printf("Test readiness %d: %v\n", ins.Id(), readiness[ins.Id()])
		if readiness[ins.Id()] == readinessReady {
			readiness[ins.Id()] = readinessReserved
			return true
		}
		return false
	}
}

func testAllReserved(backups *Backups, readiness []int) {
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

func getRandomReadiness(cand int, maxf int) []int {
	readiness := make([]int, cand)
	failures := maxf
	if maxf > 0 {
		failures = rand.Intn(maxf)
	}
	for i := 0; i < failures; i++ {
		readiness[i] = readinessBusy
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
		var readiness []int

		// Fill up
		readiness = []int{readinessReady, readinessReady, readinessReady, readinessBusy, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(5))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]

		// Keep consistent
		readiness = []int{readinessReady, readinessReady, readinessReady, readinessReady, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(0))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]

		// Used up
		readiness = []int{readinessReady, readinessReady, readinessBusy, readinessReady, readinessReady, readinessBusy, readinessReady}
		setTestCase(backups, readiness)
		changed := backups.Reserve(nil)
		Expect(changed).To(Equal(2))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 3, 6, 4] [2, 5]

		// Test hole
		readiness = []int{readinessReady, readinessBusy, readinessBusy, readinessReady, readinessReady, readinessBusy, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(4))
		testAllReserved(backups, readiness)
		// status: [0, 1(nil), 3, 6, 4] [2, 5]

		// Hole be filled
		readiness = []int{readinessReady, readinessBusy, readinessBusy, readinessReady, readinessReady, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 5, 3, 6, 4] [2, 1]

		// Test smaller length
		readiness = []int{readinessBusy, readinessReady, readinessBusy, readinessBusy, readinessBusy, readinessReady, readinessReady}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(3))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(3))
		testAllReserved(backups, readiness)
		// status: [1, 5, 3(nil), 6], [4] [2, 0]
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
})

func dumpInstance(all []*Instance) {
	ids := make([]uint64, len(all))
	for i := 0; i < len(all); i++ {
		ids[i] = all[i].Id()
	}
	fmt.Println(ids)
}
