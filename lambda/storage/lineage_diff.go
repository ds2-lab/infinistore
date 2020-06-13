package storage

import (
	"math"

	"github.com/mason-leap-lab/infinicache/lambda/types"
)

var (
	// SimpleDifferenceRankSignificanceRatio diffrank parameter
	SimpleDifferenceRankSignificanceRatio = 1.0
)

// LineageDifferenceRank Metric used to decide if a fast recovery should be requested.
type LineageDifferenceRank interface {
	Reset(float64)
	AddOp(*types.LineageOp)
	IsSignificant(float64) bool
	Rank() float64
}

// SimpleDifferenceRank only count the number of object stored.
// Only set operations are considered. The significant change is considered
// if difference of operations is larger than the number of backups, which means
//    changes / backups >= 1
// For now, we assume # of backups will be around 10. It is reasonable to change
// to a smaller value if backup increases.
type SimpleDifferenceRank struct {
	backups float64
	num     float64
}

// NewSimpleDifferenceRank Create a SimpleDifferenceRank instance.
func NewSimpleDifferenceRank(backups int) *SimpleDifferenceRank {
	return &SimpleDifferenceRank{float64(backups), 0}
}

// Reset Reset rank
func (dr *SimpleDifferenceRank) Reset(rank float64) {
	dr.num = rank
}

// AddOp Notify rank that an operation is performed.
func (dr *SimpleDifferenceRank) AddOp(op *types.LineageOp) {
	switch op.Op {
	case types.OP_SET:
		dr.num++
	}
}

// IsSignificant Determind if specified rank is significant different from the caller.
func (dr *SimpleDifferenceRank) IsSignificant(rank float64) bool {
	// The feature is disabled for backups == 0
	if dr.backups < 1 {
		return false
	}
	return (math.Abs(rank-dr.num) >= dr.backups*SimpleDifferenceRankSignificanceRatio) && // Minimum requirement.
		(math.Abs(rank-dr.num)*dr.backups > dr.num) // Utilization requirement, assumming backups are empty.
}

// Rank Get the rank in numeric form.
func (dr *SimpleDifferenceRank) Rank() float64 {
	return dr.num
}
