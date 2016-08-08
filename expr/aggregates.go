package expr

// SUM creates an Expr that obtains its value by summing the given expressions
// or fields.
func SUM(expr interface{}) Expr {
	return &aggregate{"SUM", exprFor(expr), func(wasSet bool, current float64, next float64) float64 {
		return current + next
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	}}
}

// COUNT creates an Expr that counts the number of values.
func COUNT(expr interface{}) Expr {
	return &aggregate{"COUNT", exprFor(expr), func(wasSet bool, current float64, next float64) float64 {
		return current + 1
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	}}
}
