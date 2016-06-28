package values

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloat(t *testing.T) {
	v := Float(1.1)
	assert.True(t, floatEquals(3.3, v.Add(2.2).Val()))
}

// from https://gist.github.com/cevaris/bc331cbe970b03816c6b
const epsilon = 0.00000001

func floatEquals(a, b float64) bool {
	if (a-b) < epsilon && (b-a) < epsilon {
		return true
	}
	return false
}
