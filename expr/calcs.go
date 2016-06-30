package expr

// Add creates an Expr that obtains its value by adding right and left.
func Add(left interface{}, right interface{}) Expr {
	return calc(left, right, func(left float64, right float64) float64 {
		return left + right
	})
}

// Sub creates an Expr that obtains its value by subtracting right from left.
func Sub(left interface{}, right interface{}) Expr {
	return calc(left, right, func(left float64, right float64) float64 {
		return left - right
	})
}

// Mult creates an Expr that obtains its value by multiplying right and left.
func Mult(left interface{}, right interface{}) Expr {
	return calc(left, right, func(left float64, right float64) float64 {
		return left * right
	})
}

// Div creates an Expr that obtains its value by dividing left by right. If
// right is 0, this returns 0.
func Div(left interface{}, right interface{}) Expr {
	return calc(left, right, func(left float64, right float64) float64 {
		if right == 0 {
			return 0
		}
		return left / right
	})
}
