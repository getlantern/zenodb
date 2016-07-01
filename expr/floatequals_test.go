package expr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const epsilon float64 = 0.00000001

// floatEquals does a fuzzy comparison of floats.
func assertFloatEquals(t *testing.T, a, b float64) {
	assert.True(t, fuzzyEquals(epsilon, a, b), fmt.Sprintf("Floats did not match.  Expected: %f  Actual: %f", a, b))
}
