package expr

func init() {
	registerCond("<", func(left float64, right float64) bool {
		return left < right
	})

	registerCond("<=", func(left float64, right float64) bool {
		return left <= right
	})

	registerCond("=", func(left float64, right float64) bool {
		return left == right
	})

	registerCond("<>", func(left float64, right float64) bool {
		return left != right
	})

	registerCond(">=", func(left float64, right float64) bool {
		return left >= right
	})

	registerCond(">", func(left float64, right float64) bool {
		return left > right
	})

	registerCond("AND", func(left float64, right float64) bool {
		return left > 0 && right > 0
	})

	registerCond("OR", func(left float64, right float64) bool {
		return left > 0 || right > 0
	})
}

type compareFN func(left float64, right float64) bool

// registerCond registers a binary expression that performs a comparison using
// the given compareFN and that returns 0 or 1 depending on whether or not the
// comparison evaluates to true.
func registerCond(cond string, compare compareFN) {
	registerBinaryExpr(cond, func(left float64, right float64) float64 {
		if compare(left, right) {
			return 1
		}
		return 0
	})
}

// LT tests whether left is less than right
func LT(left interface{}, right interface{}) Expr {
	return binaryExprFor("<", left, right)
}

// LTE tests whether left is less than or equal to the right
func LTE(left interface{}, right interface{}) Expr {
	return binaryExprFor("<=", left, right)
}

// EQ tests whether left equals right
func EQ(left interface{}, right interface{}) Expr {
	return binaryExprFor("=", left, right)
}

// NEQ tests whether left is different from right
func NEQ(left interface{}, right interface{}) Expr {
	return binaryExprFor("<>", left, right)
}

// GTE tests whether left is greater than or equal to right
func GTE(left interface{}, right interface{}) Expr {
	return binaryExprFor(">=", left, right)
}

// GT tests whether left is greater than right
func GT(left interface{}, right interface{}) Expr {
	return binaryExprFor(">", left, right)
}

// AND tests whether left and right is true
func AND(left interface{}, right interface{}) Expr {
	return binaryExprFor("AND", left, right)
}

// OR tests whether left or right is true
func OR(left interface{}, right interface{}) Expr {
	return binaryExprFor("OR", left, right)
}
