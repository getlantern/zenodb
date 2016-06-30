package expr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const epsilon float64 = 0.00000001

// floatEquals does a fuzzy comparison of floats.
// courtesy of https://gist.github.com/cevaris/bc331cbe970b03816c6b
func assertFloatEquals(t *testing.T, a, b float64) {
	equals := (a-b) < epsilon && (b-a) < epsilon
	assert.True(t, equals, fmt.Sprintf("Floats did not match.  Expected: %f  Actual: %f", a, b))
}
