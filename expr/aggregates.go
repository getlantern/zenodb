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

// MIN creates an Expr that keeps track of the minimum value of the wrapped
// expression or field.
func MIN(expr interface{}) Expr {
	return &aggregate{"MIN", exprFor(expr), func(wasSet bool, current float64, next float64) float64 {
		if !wasSet {
			return next
		}
		if next < current {
			return next
		}
		return current
	}, func(wasSet bool, current float64, next float64) float64 {
		if !wasSet {
			return next
		}
		if next < current {
			return next
		}
		return current
	}}
}

// MAX creates an Expr that keeps track of the maximum value of the wrapped
// expression or field.
func MAX(expr interface{}) Expr {
	return &aggregate{"MAX", exprFor(expr), func(wasSet bool, current float64, next float64) float64 {
		if !wasSet {
			return next
		}
		if next > current {
			return next
		}
		return current
	}, func(wasSet bool, current float64, next float64) float64 {
		if !wasSet {
			return next
		}
		if next > current {
			return next
		}
		return current
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
