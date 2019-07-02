package common

import (
	"testing"
	"time"

	"github.com/getlantern/wal"
	"github.com/stretchr/testify/assert"
)

func TestOffsetsBySourceAdvance(t *testing.T) {
	now := time.Now()
	nowPlusOne := now.Add(1 * time.Second)

	a := OffsetsBySource{
		0: wal.NewOffsetForTS(now),
		2: wal.NewOffsetForTS(nowPlusOne),
	}
	b := OffsetsBySource{
		0: wal.NewOffsetForTS(nowPlusOne),
		1: wal.NewOffsetForTS(now),
		2: wal.NewOffsetForTS(now),
	}
	expected := OffsetsBySource{
		0: wal.NewOffsetForTS(nowPlusOne),
		1: wal.NewOffsetForTS(now),
		2: wal.NewOffsetForTS(nowPlusOne),
	}
	var null OffsetsBySource
	assert.EqualValues(t, a, a, "OffsetsBySource should equal itself")
	assert.EqualValues(t, a, a.Advance(null), "Advancing by nil should yield the original OffsetsBySource")
	assert.EqualValues(t, a, null.Advance(a), "Advancing nil should yield the advanced OffsetsBySource")
	assert.EqualValues(t, null, null.Advance(null), "Advancing nil by nil should yield nil")
	assert.EqualValues(t, expected, a.Advance(b), "Advancing should yield superset")
	assert.EqualValues(t, expected, b.Advance(a), "Advancing should be commutative")
}

func TestOffsetsBySourceLimitAge(t *testing.T) {
	now := time.Now()
	nowPlusOne := now.Add(1 * time.Second)
	nowPlusTwo := now.Add(2 * time.Second)

	os := OffsetsBySource{
		0: wal.NewOffsetForTS(now),
		1: wal.NewOffsetForTS(nowPlusOne),
		2: wal.NewOffsetForTS((nowPlusTwo)),
	}

	expected := OffsetsBySource{
		0: wal.NewOffsetForTS(nowPlusOne),
		1: wal.NewOffsetForTS(nowPlusOne),
		2: wal.NewOffsetForTS((nowPlusTwo)),
	}

	assert.EqualValues(t, expected, os.LimitAge(wal.NewOffsetForTS(nowPlusOne)))
}
