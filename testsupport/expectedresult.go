package testsupport

import (
	"fmt"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"

	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	log = golog.LoggerFor("expectedresult")
)

type ExpectedResult []ExpectedRow

func (er ExpectedResult) Assert(t *testing.T, label string, md *common.QueryMetaData, rows []*core.FlatRow) bool {
	t.Helper()
	if !assert.Len(t, rows, len(er), label+" | Wrong number of rows") {
		return false
	}
	ok := true
	for i, erow := range er {
		row := rows[i]
		if !erow.Assert(t, label, md.FieldNames, row, i+1) {
			ok = false
		}
	}
	return ok
}

func (er ExpectedResult) TryAssert(md *common.QueryMetaData, rows []*core.FlatRow) bool {
	if len(rows) != len(er) {
		return false
	}
	for i, erow := range er {
		row := rows[i]
		if !erow.TryAssert(md.FieldNames, row, i+1) {
			return false
		}
	}
	return true
}

type ExpectedRow struct {
	TS   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

func (erow ExpectedRow) Assert(t *testing.T, label string, fieldNames []string, row *core.FlatRow, idx int) bool {
	t.Helper()
	if !assert.Equal(t, erow.TS.In(time.UTC), encoding.TimeFromInt(row.TS).In(time.UTC), label+" | Row %d - wrong timestamp", idx) {
		return false
	}

	dims := row.Key.AsMap()
	if !assert.Len(t, dims, len(erow.Dims), label+" | Row %d - wrong number of dimensions in result", idx) {
		return false
	}
	for k, v := range erow.Dims {
		if !assert.Equal(t, v, dims[k], label+" | Row %d - mismatch on dimension %v", idx, k) {
			return false
		}
	}

	if !assert.Len(t, row.Values, len(erow.Vals), label+" | Row %d - wrong number of values in result. %v", idx, erow.FieldDiff(fieldNames)) {
		return false
	}

	ok := true
	for i, v := range row.Values {
		fieldName := fieldNames[i]
		if !AssertFloatWithin(t, 0.01, erow.Vals[fieldName], v, fmt.Sprintf(label+" | Row %d - mismatch on field %v", idx, fieldName)) {
			ok = false
		}
	}
	return ok
}

func (erow ExpectedRow) TryAssert(fieldNames []string, row *core.FlatRow, idx int) bool {
	if !erow.TS.In(time.UTC).Equal(encoding.TimeFromInt(row.TS).In(time.UTC)) {
		return false
	}

	dims := row.Key.AsMap()
	if len(dims) != len(erow.Dims) {
		return false
	}
	for k, v := range erow.Dims {
		if v != dims[k] {
			return false
		}
	}

	if len(row.Values) != len(erow.Vals) {
		return false
	}

	for i, v := range row.Values {
		fieldName := fieldNames[i]
		if !FuzzyEquals(0.01, erow.Vals[fieldName], v) {
			return false
		}
	}

	return true
}

func (erow ExpectedRow) FieldDiff(fieldNames []string) string {
	diff := ""
	first := true
	for expected := range erow.Vals {
		found := false
		for _, name := range fieldNames {
			if name == expected {
				found = true
				break
			}
		}
		if !found {
			if !first {
				diff += "   "
			}
			first = false
			diff += expected + "(m)"
		}
	}
	for _, name := range fieldNames {
		_, found := erow.Vals[name]
		if !found {
			if !first {
				diff += "   "
			}
			first = false
			diff += name + "(e)"
		}
	}
	return diff
}
