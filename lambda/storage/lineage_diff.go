package storage

import (
	"math"

	"github.com/mason-leap-lab/infinicache/lambda/types"
)

var (
	SimpleDifferenceRankSignificanceRatio = 1.0
)

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
	num float64
}

func NewSimpleDifferenceRank(backups int) *SimpleDifferenceRank {
	return &SimpleDifferenceRank{ float64(backups), 0 }
}

func (dr *SimpleDifferenceRank) Reset(rank float64) {
	dr.num = rank
}

func (dr *SimpleDifferenceRank) AddOp(op *types.LineageOp) {
	switch (op.Op) {
	case types.OP_SET:
		dr.num += 1
	}
}

func (dr *SimpleDifferenceRank) IsSignificant(rank float64) bool {
	return (math.Abs(rank - dr.num) >= dr.backups * SimpleDifferenceRankSignificanceRatio) && // Minimum requirement.
		(math.Abs(rank - dr.num) * dr.backups >= dr.num)  // Utilization requirement, assumming backups are empty.
}

func (dr *SimpleDifferenceRank) Rank() float64 {
	return dr.num
}
