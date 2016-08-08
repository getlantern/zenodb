package expr

import (
	"github.com/Knetic/govaluate"
)

// SUM creates an Expr that obtains its value by summing the given expressions
// or fields.
func SUM(expr interface{}, cond *govaluate.EvaluableExpression) Expr {
	return newAggregate("SUM", exprFor(expr), cond, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	})
}

// COUNT creates an Expr that counts the number of values.
func COUNT(expr interface{}, cond *govaluate.EvaluableExpression) Expr {
	return newAggregate("COUNT", exprFor(expr), cond, func(wasSet bool, current float64, next float64) float64 {
		return current + 1
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	})
}
