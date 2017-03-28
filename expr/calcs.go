package expr

import (
	"math"
)

func init() {
	registerBinaryExpr("+", func(left float64, right float64) float64 {
		return left + right
	})

	registerBinaryExpr("-", func(left float64, right float64) float64 {
		return left - right
	})

	registerBinaryExpr("*", func(left float64, right float64) float64 {
		return left * right
	})

	registerBinaryExpr("/", func(left float64, right float64) float64 {
		if right == 0 {
			if left == 0 {
				return 0
			}
			return math.MaxFloat64
		}
		return left / right
	})
}

// ADD creates an Expr that obtains its value by adding right and left.
func ADD(left interface{}, right interface{}) Expr {
	return binaryExprFor("+", left, right)
}

// SUB creates an Expr that obtains its value by subtracting right from left.
func SUB(left interface{}, right interface{}) Expr {
	return binaryExprFor("-", left, right)
}

// MULT creates an Expr that obtains its value by multiplying right and left.
func MULT(left interface{}, right interface{}) Expr {
	return binaryExprFor("*", left, right)
}

// DIV creates an Expr that obtains its value by dividing left by right. If
// right is 0, this returns 0.
func DIV(left interface{}, right interface{}) Expr {
	return binaryExprFor("/", left, right)
}
