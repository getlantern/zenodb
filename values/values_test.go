package values

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloat(t *testing.T) {
	assert.True(t, floatEquals(3.3, Float(1.1).Plus(Float(2.2)).Val()))
}

func TestInt(t *testing.T) {
	assert.EqualValues(t, 3, Int(1).Plus(Int(2)).Val())
}

// from https://gist.github.com/cevaris/bc331cbe970b03816c6b
const epsilon = 0.00000001

func floatEquals(a, b float64) bool {
	if (a-b) < epsilon && (b-a) < epsilon {
		return true
	}
	return false
}
