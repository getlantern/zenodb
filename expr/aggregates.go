package expr

func init() {
	registerAggregate("SUM", func(wasSet bool, current float64, next float64) float64 {
		return current + next
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	})

	registerAggregate("MIN", func(wasSet bool, current float64, next float64) float64 {
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
	})

	registerAggregate("MAX", func(wasSet bool, current float64, next float64) float64 {
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
	})

	registerAggregate("COUNT", func(wasSet bool, current float64, next float64) float64 {
		return current + 1
	}, func(wasSet bool, current float64, next float64) float64 {
		return current + next
	})
}

// SUM creates an Expr that obtains its value by summing the given expressions
// or fields.
func SUM(expr interface{}) Expr {
	return aggregateFor("SUM", expr)
}

// MIN creates an Expr that keeps track of the minimum value of the wrapped
// expression or field.
func MIN(expr interface{}) Expr {
	return aggregateFor("MIN", expr)
}

// MAX creates an Expr that keeps track of the maximum value of the wrapped
// expression or field.
func MAX(expr interface{}) Expr {
	return aggregateFor("MAX", expr)
}

// COUNT creates an Expr that counts the number of values.
func COUNT(expr interface{}) Expr {
	return aggregateFor("COUNT", expr)
}
