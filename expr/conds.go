package expr

import "fmt"

// LT tests whether left is less than right
func LT(left interface{}, right interface{}) Cond {
	return cond("<", left, right, func(left float64, right float64) bool {
		return left < right
	})
}

// LTE tests whether left is less than or equal to the right
func LTE(left interface{}, right interface{}) Cond {
	return cond("<=", left, right, func(left float64, right float64) bool {
		return left <= right
	})
}

// EQ tests whether left equals right
func EQ(left interface{}, right interface{}) Cond {
	return cond("=", left, right, func(left float64, right float64) bool {
		return left == right
	})
}

// NEQ tests whether left is different from right
func NEQ(left interface{}, right interface{}) Cond {
	return cond("<>", left, right, func(left float64, right float64) bool {
		return left != right
	})
}

// FUZZY_EQ tests whether left equals right after rounding by epsilon
func FUZZY_EQ(left interface{}, right interface{}, epsilon float64) Cond {
	return cond("=?", left, right, func(left float64, right float64) bool {
		return fuzzyEquals(epsilon, left, right)
	})
}

// GTE tests whether left is greater than or equal to right
func GTE(left interface{}, right interface{}) Cond {
	return cond(">=", left, right, func(left float64, right float64) bool {
		return left >= right
	})
}

// GT tests whether left is greater than right
func GT(left interface{}, right interface{}) Cond {
	return cond(">", left, right, func(left float64, right float64) bool {
		fmt.Printf("%f > %v\n", left, right)
		return left > right
	})
}

// AND tests whether left and right is true
func AND(left interface{}, right interface{}) Cond {
	return cond("AND", left, right, func(left float64, right float64) bool {
		return left > 0 && right > 0
	})
}

// OR tests whether left or right is true
func OR(left interface{}, right interface{}) Cond {
	return cond("OR", left, right, func(left float64, right float64) bool {
		return left > 0 || right > 0
	})
}
