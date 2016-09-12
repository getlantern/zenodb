package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLT(t *testing.T) {
	doTestCond(t, LT(SUM("a"), SUM("b")), false)
	doTestCond(t, LT(SUM("b"), SUM("a")), true)
}

func TestLTE(t *testing.T) {
	doTestCond(t, LTE(SUM("a"), SUM("b")), false)
	doTestCond(t, LTE(SUM("b"), SUM("a")), true)
	doTestCond(t, LTE(SUM("b"), SUM("b")), true)
}

func TestEQ(t *testing.T) {
	doTestCond(t, EQ(SUM("a"), SUM("b")), false)
	doTestCond(t, EQ(SUM("b"), SUM("a")), false)
	doTestCond(t, EQ(SUM("b"), SUM("b")), true)
}

func TestNEQ(t *testing.T) {
	doTestCond(t, NEQ(SUM("a"), SUM("b")), true)
	doTestCond(t, NEQ(SUM("b"), SUM("a")), true)
	doTestCond(t, NEQ(SUM("b"), SUM("b")), false)
}

func TestFUZZY_EQ(t *testing.T) {
	doTestCond(t, FUZZY_EQ(SUM("a"), SUM("b"), .0001), false)
	doTestCond(t, FUZZY_EQ(SUM("a"), SUM("b"), .001), true)
}

func TestGTE(t *testing.T) {
	doTestCond(t, GTE(SUM("a"), SUM("b")), true)
	doTestCond(t, GTE(SUM("b"), SUM("a")), false)
	doTestCond(t, GTE(SUM("b"), SUM("b")), true)
}

func TestGT(t *testing.T) {
	doTestCond(t, GT(SUM("a"), SUM("b")), true)
	doTestCond(t, GT(SUM("b"), SUM("a")), false)
}

func TestAND(t *testing.T) {
	doTestCond(t, AND(GT(SUM("a"), SUM("b")), GT(SUM("b"), SUM("a"))), false)
	doTestCond(t, AND(GT(SUM("a"), SUM("b")), GT(SUM("a"), SUM("b"))), true)
}

func TestOR(t *testing.T) {
	doTestCond(t, OR(GT(SUM("a"), SUM("b")), GT(SUM("b"), SUM("a"))), true)
	doTestCond(t, OR(GT(SUM("a"), SUM("b")), GT("unknown", SUM("a"))), true)
	doTestCond(t, OR(GT(SUM("b"), SUM("a")), GT(SUM("b"), SUM("a"))), false)
}

func doTestCond(t *testing.T, e Expr, expected bool) {
	params := Map{
		"a": 1.001,
		"b": 1.0,
	}

	b := make([]byte, e.EncodedWidth())
	_, val, updated := e.Update(b, params, nil)
	if !assert.True(t, updated) {
		return
	}
	expectedFloat := float64(0)
	if expected {
		expectedFloat = 1
	}
	assertFloatEquals(t, expectedFloat, val)
	readVal, wasSet, _ := e.Get(b)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expectedFloat, readVal)
	}
}
