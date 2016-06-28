package expr

import (
	"github.com/oxtoacart/tdb/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCalc(t *testing.T) {
	c := Calc("a + b")
	fields := map[string]values.Value{
		"a": values.Float(2.2),
		"b": values.Float(3.3),
	}
	assert.EqualValues(t, 5.5, c(fields))
}
