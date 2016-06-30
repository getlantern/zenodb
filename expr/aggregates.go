package expr

import (
	"math"
)

// Sum creates an Expr that obtains its value by summing the given expressions
// or fields.
func Sum(expr interface{}) Expr {
	return aggregate(expr, 0, func(current float64, next float64) float64 {
		return current + next
	})
}

// Min creates an Expr that obtains its value by taking the minimum of the given
// expressions or fields.
func Min(expr interface{}) Expr {
	return aggregate(expr, math.MaxFloat64, func(current float64, next float64) float64 {
		if next < current {
			return next
		}
		return current
	})
}

// Max creates an Expr that obtains its value by taking the maximum of the given
// expressions or fields.
func Max(expr interface{}) Expr {
	return aggregate(expr, -1*math.MaxFloat64, func(current float64, next float64) float64 {
		if next > current {
			return next
		}
		return current
	})
}

// Count creates an Expr that counts the number of values.
func Count(expr interface{}) Expr {
	return aggregate(expr, 0, func(current float64, next float64) float64 {
		return current + 1
	})
}
