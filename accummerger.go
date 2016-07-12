package tdb

import (
	"fmt"

	"github.com/getlantern/tdb/expr"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type accumMerger struct {
	t *table
}

// FullMerge implements method from gorocksdb.MergeOperator.
func (m *accumMerger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	acs := m.t.accumulators(existingValue)

	for _, operand := range operands {
		err := applyOperand(acs, operand)
		if err != nil {
			log.Error(err)
			return existingValue, false
		}
	}

	return serializeAccumulators(acs), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (m *accumMerger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	acs := m.t.accumulators(nil)

	err := applyOperand(acs, leftOperand)
	if err != nil {
		log.Error(err)
		return nil, false
	}
	err = applyOperand(acs, rightOperand)
	if err != nil {
		log.Error(err)
		return nil, false
	}

	return serializeAccumulators(acs), true
}

func (t *table) accumulators(existingValue []byte) []expr.Accumulator {
	acs := make([]expr.Accumulator, 0, len(t.Fields))

	for _, field := range t.Fields {
		ac := field.Accumulator()
		if len(existingValue) > 0 {
			ac.InitFrom(existingValue)
		}
		acs = append(acs, ac)
	}

	return acs
}

func applyOperand(acs []expr.Accumulator, operand []byte) error {
	_params := make(map[string]float64, 0)
	err := msgpack.Unmarshal(operand, &_params)
	if err != nil {
		return fmt.Errorf("Unable to unmarshal params for merge: %v", err)
	}

	params := expr.FloatMap(_params)
	for _, ac := range acs {
		ac.Update(params)
	}

	return nil
}

func serializeAccumulators(acs []expr.Accumulator) []byte {
	var out []byte
	for _, ac := range acs {
		out = append(out, ac.Bytes()...)
	}
	return out
}

// Name implements method from gorocksdb.MergeOperator.
func (m *accumMerger) Name() string {
	return "default"
}
