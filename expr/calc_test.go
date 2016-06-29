package expr

import (
	"github.com/oxtoacart/tdb/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCalc(t *testing.T) {
	c := Calc("b > 0 ? a / b")
	fields1 := Map{
		"a": values.Float(4.4),
		"b": values.Float(2.2),
	}
	fields2 := Map{
		"a": values.Float(2.2),
		"b": values.Float(0),
	}
	assert.EqualValues(t, 2, c(fields1))
	assert.EqualValues(t, 0, c(fields2))
}
