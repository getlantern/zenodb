package expr

import (
	"github.com/oxtoacart/tdb/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAvg(t *testing.T) {
	avg := Avg(Calc("a + b"))
	fields1 := map[string]values.Value{
		"a": values.Float(1),
		"b": values.Float(2),
	}
	fields2 := map[string]values.Value{
		"a": values.Float(3),
		"b": values.Float(4),
	}
	assert.EqualValues(t, 5, avg(fields1).Plus(avg(fields2)).Val())
}
