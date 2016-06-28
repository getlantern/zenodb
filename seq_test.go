package tdb

import (
	"github.com/oxtoacart/tdb/values"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildSequence(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	res := time.Minute
	b := &bucket{
		start: ts.Add(10 * res),
		val:   values.Float(6),
		prev: &bucket{
			start: ts.Add(7 * res),
			val:   values.Float(5),
			prev: &bucket{
				start: ts.Add(5 * res),
				val:   values.Float(4),
			},
		},
	}

	b2 := &bucket{
		start: ts.Add(3 * res),
		val:   values.Float(3),
		prev: &bucket{
			start: ts.Add(1 * res),
			val:   values.Float(2),
			prev: &bucket{
				start: ts,
				val:   values.Float(1),
			},
		},
	}

	seq := b.toSequence(res).append(b2.toSequence(res), res)
	assert.Equal(t, ts.Add(10*res), seq.start().In(time.UTC))
	assert.Equal(t, 11, seq.numBuckets())
	for i := time.Duration(-1); i <= 12; i++ {
		actual := int(seq.valueAtTime(ts.Add(i*res), res))
		t.Logf("%d -> %d", i, actual)
		expected := 0
		switch i {
		case 10:
			expected = 6
		case 7:
			expected = 5
		case 5:
			expected = 4
		case 3:
			expected = 3
		case 1:
			expected = 2
		case 0:
			expected = 1
		}
		assert.Equal(t, expected, actual)
	}
}
