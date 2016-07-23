package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLT(t *testing.T) {
	doTestCond(t, LT("a", "b"), []string{"a", "b"}, false)
	doTestCond(t, LT("b", "a"), []string{"a", "b"}, true)
}

func TestLTE(t *testing.T) {
	doTestCond(t, LTE("a", "b"), []string{"a", "b"}, false)
	doTestCond(t, LTE("b", "a"), []string{"a", "b"}, true)
	doTestCond(t, LTE("b", "b"), []string{"b"}, true)
}

func TestEQ(t *testing.T) {
	doTestCond(t, EQ("a", "b"), []string{"a", "b"}, false)
	doTestCond(t, EQ("b", "a"), []string{"a", "b"}, false)
	doTestCond(t, EQ("b", "b"), []string{"b"}, true)
}

func TestNEQ(t *testing.T) {
	doTestCond(t, NEQ("a", "b"), []string{"a", "b"}, true)
	doTestCond(t, NEQ("b", "a"), []string{"a", "b"}, true)
	doTestCond(t, NEQ("b", "b"), []string{"b"}, false)
}

func TestFUZZY_EQ(t *testing.T) {
	doTestCond(t, FUZZY_EQ("a", "b", .0001), []string{"a", "b"}, false)
	doTestCond(t, FUZZY_EQ("a", "b", .001), []string{"a", "b"}, true)
}

func TestGTE(t *testing.T) {
	doTestCond(t, GTE("a", "b"), []string{"a", "b"}, true)
	doTestCond(t, GTE("b", "a"), []string{"a", "b"}, false)
	doTestCond(t, GTE("b", "b"), []string{"b"}, true)
}

func TestGT(t *testing.T) {
	doTestCond(t, GT("a", "b"), []string{"a", "b"}, true)
	doTestCond(t, GT("b", "a"), []string{"a", "b"}, false)
}

func TestAND(t *testing.T) {
	doTestCond(t, AND(GT("a", "b"), GT("b", "a")), []string{"a", "b"}, false)
	doTestCond(t, AND(GT("a", "b"), GT("a", "b")), []string{"a", "b"}, true)
}

func TestOR(t *testing.T) {
	doTestCond(t, OR(GT("a", "b"), GT("b", "a")), []string{"a", "b"}, true)
	doTestCond(t, OR(GT("b", "a"), GT("b", "a")), []string{"a", "b"}, false)
}

func doTestCond(t *testing.T, e Expr, expectedDepends []string, expected bool) {
	params := Map{
		"a": 1.001,
		"b": 1.0,
	}

	assert.Equal(t, expectedDepends, e.DependsOn())
	b := make([]byte, e.EncodedWidth())
	_, val, _ := e.Update(b, params)
	expectedFloat := float64(0)
	if expected {
		expectedFloat = 1
	}
	assertFloatEquals(t, expectedFloat, val)
}
