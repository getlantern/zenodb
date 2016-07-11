package tdb

import (
	"time"
)

// FullMerge implements method from gorocksdb.MergeOperator.
func (t *table) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	seq := sequence(existingValue)
	truncateBefore := t.truncateBefore()
	for _, operand := range operands {
		val := tsvalue(operand)
		seq = seq.plus(val, t.Resolution, truncateBefore)
	}
	if log.IsTraceEnabled() {
		log.Tracef("Merge result: %v", seq)
	}
	return []byte(seq), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (t *table) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	// TODO: see if partial merge is possible/useful in the case of merging
	// individual periods
	return nil, false
}

// Transform implements method from gorocksdb.SliceTransform.
func (t *table) Transform(src []byte) []byte {
	return nil
}

// InDomain implements method from gorocksdb.SliceTransform.
func (t *table) InDomain(src []byte) bool {
	return len(src) > 1
}

// InRange implements method from gorocksdb.SliceTransform.
func (t *table) InRange(src []byte) bool {
	return false
}

// Name implements method from gorocksdb.MergeOperator and
// gorocksdb.SliceTransform.
func (t *table) Name() string {
	return "default"
}

func (t *table) truncateBefore() time.Time {
	return t.clock.Now().Add(-1 * t.retentionPeriod)
}
