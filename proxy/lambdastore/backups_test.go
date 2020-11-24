package lambdastore_test

import (
	"log"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
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

func setTestCase(backups *Backups, readiness []bool) {
	backups.Reserver = func(ins *Instance) bool {
		// log.Printf("Test readiness %d: %v\n", ins.Id(), readiness[ins.Id()])
		return readiness[ins.Id()]
	}
}

func testAllReserved(backups *Backups, readiness []bool) {
	iter := backups.Iter()
	iterated := 0
	Expect(iter.Len()).To(Equal(backups.Availables()))
	for iter.Next() {
		i, backup := iter.Value()
		// if !readiness[backup.Id()] {
		// 	log.Printf("detect false: %d:%d", i, backup.Id())
		// }
		Expect(i).To(Equal(iterated))
		Expect(readiness[backup.(*Instance).Id()]).To(Equal(true))
		iterated++
	}
}

func getRandomReadiness(cand int, maxf int) []bool {
	failures := rand.Intn(maxf)
	readiness := make([]bool, cand)
	for i := failures; i < cand; i++ {
		readiness[i] = true
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
		var readiness []bool

		// Fill up
		readiness = []bool{true, true, true, false, true, true, true}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(5))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]

		// Keep consistent
		readiness = []bool{true, true, true, true, true, true, true}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(0))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 2, 5, 4] [3, 6]

		// Used up
		readiness = []bool{true, true, false, true, true, false, true}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(2))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 1, 3, 6, 4] [2, 5]

		// Test hole
		readiness = []bool{true, false, false, true, true, false, true}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(4))
		testAllReserved(backups, readiness)
		// status: [0, 1(nil), 3, 6, 4] [2, 5]

		// Hole be filled
		readiness = []bool{true, false, false, true, true, true, true}
		setTestCase(backups, readiness)
		Expect(backups.Reserve(nil)).To(Equal(1))
		Expect(backups.Len()).To(Equal(5))
		Expect(backups.Availables()).To(Equal(5))
		testAllReserved(backups, readiness)
		// status: [0, 5, 3, 6, 4] [2, 1]

		// Test smaller length
		readiness = []bool{false, true, false, false, false, true, true}
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
})
