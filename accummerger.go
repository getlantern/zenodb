package tdb

import (
	"bytes"
	"github.com/getlantern/tdb/expr"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

type accumMerger struct {
	t *table
}

// FullMerge implements method from gorocksdb.MergeOperator.
func (m *accumMerger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	acs := m.t.accumulators(existingValue)

	for _, operand := range operands {
		paramsArray, err := paramsFromBytes(operand)
		if err != nil {
			log.Errorf("Unable to apply operand: %v", err)
			return existingValue, false
		}
		for _, _params := range paramsArray {
			params := expr.FloatMap(_params)
			for _, ac := range acs {
				ac.Update(params)
			}
		}
	}

	return serializeAccumulators(acs), true
}

// PartialMerge implements method from gorocksdb.MergeOperator.
func (m *accumMerger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return append(leftOperand, rightOperand...), true
}

func (t *table) accumulators(existingValue []byte) []expr.Accumulator {
	acs := make([]expr.Accumulator, 0, len(t.Fields))

	for _, field := range t.Fields {
		ac := field.Accumulator()
		if len(existingValue) > 0 {
			existingValue = ac.InitFrom(existingValue)
		}
		acs = append(acs, ac)
	}

	return acs
}

func serializeAccumulators(acs []expr.Accumulator) []byte {
	var out []byte
	for _, ac := range acs {
		out = append(out, ac.Bytes()...)
	}
	return out
}

func paramsAsBytes(params map[string]float64) ([]byte, error) {
	b, err := msgpack.Marshal(params)
	if err != nil {
		return nil, err
	}
	return b, err
}

func paramsFromBytes(b []byte) ([]map[string]float64, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(b))
	var paramsArray []map[string]float64
	for {
		params := make(map[string]float64, 0)
		err := dec.Decode(&params)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		paramsArray = append(paramsArray, params)
	}
	return paramsArray, nil
}

// Name implements method from gorocksdb.MergeOperator.
func (m *accumMerger) Name() string {
	return "default"
}
