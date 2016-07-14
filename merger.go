package tdb

import (
	"github.com/getlantern/tdb/expr"
)

type merger struct {
	t *table
}

// FullMerge implements method from gorocksdb.MergeOperator.
func (m *merger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	acs := m.t.getAccumulators()
	defer m.t.putAccumulators(acs)

	field := fieldFor(key)
	var accum expr.Accumulator
	for i, candidate := range m.t.Fields {
		if candidate.Name == field {
			accum = acs[i]
			break
		}
	}
	if accum == nil {
		m.t.log.Errorf("Unable to find accumulator for field %v, not merging value", field)
		return nil, false
	}

	t := m.t
	seq := sequence(existingValue)
	truncateBefore := t.truncateBefore()
	for _, operand := range operands {
		tsp := tsparams(operand)
		seq = seq.update(tsp, accum, m.t.Resolution, truncateBefore)
	}

	if log.IsTraceEnabled() {
		log.Tracef("Merge result: %v", seq.String(accum))
	}

	return []byte(seq), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (m *merger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return nil, false
}

// Name implements method from gorocksdb.MergeOperator.
func (m *merger) Name() string {
	return "default"
}
