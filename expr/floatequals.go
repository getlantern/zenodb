package expr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const epsilon float64 = 0.00001

// AssertFloatEquals does a fuzzy comparison of floats.
func AssertFloatEquals(t *testing.T, a, b float64) bool {
	return assert.True(t, fuzzyEquals(epsilon, a, b), fmt.Sprintf("Floats did not match.  Expected: %f  Actual: %f", a, b))
}

// AssertFloatWithin checks whether a given float is within e error (decimal) of
// another float
func AssertFloatWithin(t *testing.T, e, expected float64, actual float64, msg string) bool {
	return assert.True(t, fuzzyEquals(e, expected, actual), fmt.Sprintf("%v -- Floats not within %f of each other. Expected: %f  Actual: %f", msg, e, expected, actual))
}

// courtesy of https://gist.github.com/cevaris/bc331cbe970b03816c6b
func fuzzyEquals(e, a, b float64) bool {
	if a == b {
		return true
	}
	d := b - a
	q := d / ((a + b) / 2)
	return q >= -1*e && q <= e
}
