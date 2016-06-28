package expr

import (
	"github.com/oxtoacart/tdb/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMin(t *testing.T) {
	min := Min(Calc("a + b"))
	fields1 := map[string]values.Value{
		"a": values.Float(1),
		"b": values.Float(2),
	}
	fields2 := map[string]values.Value{
		"a": values.Float(3),
		"b": values.Float(4),
	}
	assert.EqualValues(t, 3, min(fields1).Plus(min(fields2)).Val())
}

func TestMax(t *testing.T) {
	max := Max(Calc("a + b"))
	fields1 := map[string]values.Value{
		"a": values.Float(1),
		"b": values.Float(2),
	}
	fields2 := map[string]values.Value{
		"a": values.Float(3),
		"b": values.Float(4),
	}
	assert.EqualValues(t, 7, max(fields1).Plus(max(fields2)).Val())
}
