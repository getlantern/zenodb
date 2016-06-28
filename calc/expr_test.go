package calc

import (
	"github.com/oxtoacart/tdb/values"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimple(t *testing.T) {
	b, err := Expr("a + b")
	if assert.NoError(t, err) {
		assert.Equal(t, values.Float(0), b.Initial())
		otherFields := map[string]interface{}{
			"a": 2.2,
			"b": 3.3,
		}
		result, err := b.Add(values.Float(1.1), otherFields)
		if assert.NoError(t, err) {
			assert.EqualValues(t, 6.6, result)
		}
	}
}
