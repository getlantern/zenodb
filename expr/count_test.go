package expr

import (
	"github.com/oxtoacart/tdb/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCount(t *testing.T) {
	count := Count(Calc("a + b"))
	fields1 := Map{
		"a": values.Float(1),
		"b": values.Float(2),
	}
	fields2 := Map{
		"a": values.Float(3),
		"b": values.Float(4),
	}
	assert.EqualValues(t, 2, count(fields1).Plus(count(fields2)).Val())
}
