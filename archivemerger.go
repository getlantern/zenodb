package tdb

type archiveMerger struct {
	t *table
}

// FullMerge implements method from gorocksdb.MergeOperator.
func (m *archiveMerger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	t := m.t
	seq := sequence(existingValue)
	truncateBefore := t.truncateBefore()
	for _, operand := range operands {
		val := tsvalue(operand)
		seq = seq.plus(val, t.Resolution, truncateBefore)
	}
	if m.t.log.IsTraceEnabled() {
		m.t.log.Tracef("Merge result: %v", seq)
	}
	return []byte(seq), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (m *archiveMerger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	// TODO: see if partial merge is possible/useful in the case of merging
	// individual periods
	return nil, false
}

// Name implements method from gorocksdb.MergeOperator.
func (m *archiveMerger) Name() string {
	return "default"
}
