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

func (er ExpectedResult) Assert(t *testing.T, md *common.QueryMetaData, rows []*core.FlatRow) bool {
	if !assert.Len(t, rows, len(er)) {
		return false
	}
	ok := true
	for i, erow := range er {
		row := rows[i]
		if !erow.Assert(t, md.FieldNames, row, i+1) {
			ok = false
		}
	}
	return ok
}

type ExpectedRow struct {
	TS   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

func (erow ExpectedRow) Assert(t *testing.T, fieldNames []string, row *core.FlatRow, idx int) bool {
	if !assert.Equal(t, erow.TS.In(time.UTC), encoding.TimeFromInt(row.TS).In(time.UTC), "Row %d - wrong timestamp", idx) {
		return false
	}

	dims := row.Key.AsMap()
	if !assert.Len(t, dims, len(erow.Dims), "Row %d - wrong number of dimensions in result", idx) {
		return false
	}
	for k, v := range erow.Dims {
		if !assert.Equal(t, v, dims[k], "Row %d - mismatch on dimension %v", idx, k) {
			return false
		}
	}

	if !assert.Len(t, row.Values, len(erow.Vals), "Row %d - wrong number of values in result. %v", idx, erow.FieldDiff(fieldNames)) {
		return false
	}

	ok := true
	for i, v := range row.Values {
		fieldName := fieldNames[i]
		if !AssertFloatWithin(t, 0.01, erow.Vals[fieldName], v, fmt.Sprintf("Row %d - mismatch on field %v", idx, fieldName)) {
			ok = false
		}
	}
	return ok
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
