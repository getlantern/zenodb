package expr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShiftRegular(t *testing.T) {
	params := Map{
		"a": 4.4,
	}
	s := msgpacked(t, SHIFT(SUM(FIELD("a")), 1*time.Hour))
	b1 := make([]byte, s.EncodedWidth()*2)
	b2 := make([]byte, s.EncodedWidth()*2)
	b3 := make([]byte, s.EncodedWidth()*2)
	_, val, _ := s.Update(b1, params, nil)
	assert.EqualValues(t, 4.4, val)
	s.Update(b2, params, nil)
	val, _, _ = s.Get(b2)
	assert.EqualValues(t, 4.4, val)
	s.Merge(b3, b1, b2)
	val, _, _ = s.Get(b3)
	assert.EqualValues(t, 8.8, val)
}

func TestShiftSubMerge(t *testing.T) {
	res := 1 * time.Hour

	fa := msgpacked(t, SUM(FIELD("a")))
	fb := msgpacked(t, SUM(FIELD("b")))
	fs := msgpacked(t, SUB(
		SUM(FIELD("a")),
		SHIFT(SHIFT(SUM(FIELD("b")), res), res)))
	assert.EqualValues(t, 2*res, fs.Shift())

	a := make([]byte, fa.EncodedWidth())
	b := make([]byte, fb.EncodedWidth()*3)
	s := make([]byte, fs.EncodedWidth())

	params := Map{
		"a": 4,
		"b": 1,
	}
	fa.Update(a, params, nil)
	fb.Update(b[fb.EncodedWidth()*2:], params, nil)
	subs := fs.SubMergers([]Expr{fa, fb})
	subDatas := [][]byte{a, b}
	for i, sub := range subs {
		sub(s, subDatas[i], res, nil)
	}
	val, _, _ := fs.Get(s)
	assert.EqualValues(t, 3, val)
}
