package tdb

import (
	"time"
)

// FullMerge implements method from gorocksdb.MergeOperator.
func (t *table) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	es := sequence(existingValue)
	for _, operand := range operands {
		os := sequence(operand)
		if os.isValid() {
			if !es.isValid() {
				es = os
			} else {
				es = os.append(es, t.Resolution, t.truncateBefore())
			}
		}
	}
	return []byte(es), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (t *table) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	left := sequence(leftOperand)
	right := sequence(rightOperand)
	if !left.isValid() {
		return right, true
	}
	if !right.isValid() {
		return left, true
	}
	return right.append(left, t.Resolution, t.truncateBefore()), true
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
