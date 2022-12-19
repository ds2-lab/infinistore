package stats

import (
	"testing"
)

func NewMovingSumN(n int, window int) *MovingStats {
	movingSum := NewMovingSum(int64(window))
	for i := 1; i <= n; i++ {
		movingSum.Add(float64(i))
	}
	return movingSum
}

func TestSum(t *testing.T) {
	movingSum := NewMovingSumN(1, 5)
	if movingSum.Value() != 1 {
		t.Logf("wrong sum of 1 with window 5, want: %v, got: %v", 1, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(2)
	if movingSum.Value() != 3 {
		t.Logf("wrong sum of 1-2 with window 5, want: %v, got: %v", 3, movingSum.Value())
		t.Fail()
	}

	movingSum = NewMovingSumN(4, 5)
	if movingSum.Value() != 10 {
		t.Logf("wrong sum of 1-4 with window 5, want: %v, got: %v", 10, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(5)
	if movingSum.Value() != 15 {
		t.Logf("wrong sum of 1-5 with window 5, want: %v, got: %v", 15, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(6)
	if movingSum.Value() != 20 {
		t.Logf("wrong sum of 1-6 with window 5, want: %v, got: %v", 20, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(7)
	if movingSum.Value() != 25 {
		t.Logf("wrong sum of 1-7 with window 5, want: %v, got: %v", 25, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(8)
	if movingSum.Value() != 30 {
		t.Logf("wrong sum of 1-8 with window 5, want: %v, got: %v", 30, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(9)
	if movingSum.Value() != 35 {
		t.Logf("wrong sum of 1-9 with window 5, want: %v, got: %v", 35, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(10)
	if movingSum.Value() != 40 {
		t.Logf("wrong sum of 1-10 with window 5, want: %v, got: %v", 40, movingSum.Value())
		t.Fail()
	}

	movingSum.Add(11)
	if movingSum.Value() != 45 {
		t.Logf("wrong sum of 1-11 with window 5, want: %v, got: %v", 45, movingSum.Value())
		t.Fail()
	}
}

func TestLast(t *testing.T) {
	movingSum := NewMovingSumN(1, 5)
	if movingSum.Last() != 1 {
		t.Logf("wrong last of 1 with window 5, want: %v, got: %v", 1, movingSum.Last())
		t.Fail()
	}

	movingSum.Add(2)
	if movingSum.Last() != 2 {
		t.Logf("wrong last of 1-2 with window 5, want: %v, got: %v", 2, movingSum.Last())
		t.Fail()
	}

	movingSum = NewMovingSumN(4, 5)
	if movingSum.Last() != 4 {
		t.Logf("wrong last of 1-4 with window 5, want: %v, got: %v", 4, movingSum.Last())
		t.Fail()
	}

	movingSum.Add(5)
	if movingSum.Last() != 5 {
		t.Logf("wrong last of 1-5 with window 5, want: %v, got: %v", 5, movingSum.Last())
		t.Fail()
	}

	movingSum.Add(6)
	if movingSum.Last() != 6 {
		t.Logf("wrong last of 1-6 with window 5, want: %v, got: %v", 6, movingSum.Last())
		t.Fail()
	}
}
