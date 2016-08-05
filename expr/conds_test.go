package expr

import (
	"testing"
)

func TestLT(t *testing.T) {
	doTestCond(t, LT("a", "b"), false)
	doTestCond(t, LT("b", "a"), true)
}

func TestLTE(t *testing.T) {
	doTestCond(t, LTE("a", "b"), false)
	doTestCond(t, LTE("b", "a"), true)
	doTestCond(t, LTE("b", "b"), true)
}

func TestEQ(t *testing.T) {
	doTestCond(t, EQ("a", "b"), false)
	doTestCond(t, EQ("b", "a"), false)
	doTestCond(t, EQ("b", "b"), true)
}

func TestNEQ(t *testing.T) {
	doTestCond(t, NEQ("a", "b"), true)
	doTestCond(t, NEQ("b", "a"), true)
	doTestCond(t, NEQ("b", "b"), false)
}

func TestFUZZY_EQ(t *testing.T) {
	doTestCond(t, FUZZY_EQ("a", "b", .0001), false)
	doTestCond(t, FUZZY_EQ("a", "b", .001), true)
}

func TestGTE(t *testing.T) {
	doTestCond(t, GTE("a", "b"), true)
	doTestCond(t, GTE("b", "a"), false)
	doTestCond(t, GTE("b", "b"), true)
}

func TestGT(t *testing.T) {
	doTestCond(t, GT("a", "b"), true)
	doTestCond(t, GT("b", "a"), false)
}

func TestAND(t *testing.T) {
	doTestCond(t, AND(GT("a", "b"), GT("b", "a")), false)
	doTestCond(t, AND(GT("a", "b"), GT("a", "b")), true)
}

func TestOR(t *testing.T) {
	doTestCond(t, OR(GT("a", "b"), GT("b", "a")), true)
	doTestCond(t, OR(GT("b", "a"), GT("b", "a")), false)
}

func doTestCond(t *testing.T, e Expr, expected bool) {
	params := Map{
		"a": 1.001,
		"b": 1.0,
	}

	b := make([]byte, e.EncodedWidth())
	_, val, _ := e.Update(b, params)
	expectedFloat := float64(0)
	if expected {
		expectedFloat = 1
	}
	assertFloatEquals(t, expectedFloat, val)
}
