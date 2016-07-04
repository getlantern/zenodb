package expr

import (
	"math"
)

// SUM creates an Expr that obtains its value by summing the given expressions
// or fields.
func SUM(expr interface{}) Expr {
	return aggregate("SUM", expr, 0, func(current float64, next float64) float64 {
		return current + next
	})
}

// MIN creates an Expr that obtains its value by taking the minimum of the given
// expressions or fields.
func MIN(expr interface{}) Expr {
	return aggregate("MIN", expr, math.MaxFloat64, func(current float64, next float64) float64 {
		if next < current {
			return next
		}
		return current
	})
}

// MAX creates an Expr that obtains its value by taking the maximum of the given
// expressions or fields.
func MAX(expr interface{}) Expr {
	return aggregate("MAX", expr, -1*math.MaxFloat64, func(current float64, next float64) float64 {
		if next > current {
			return next
		}
		return current
	})
}

// COUNT creates an Expr that counts the number of values.
func COUNT(expr interface{}) Expr {
	return aggregate("COUNT", expr, 0, func(current float64, next float64) float64 {
		return current + 1
	})
}
