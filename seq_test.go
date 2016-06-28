package tdb

import (
	"github.com/oxtoacart/tdb/values"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildSequence(t *testing.T) {
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	res := time.Minute
	b := &bucket{
		start: epoch.Add(10 * res),
		val:   values.Float(6),
		prev: &bucket{
			start: epoch.Add(7 * res),
			val:   values.Float(5),
			prev: &bucket{
				start: epoch.Add(5 * res),
				val:   values.Float(4),
			},
		},
	}

	b2 := &bucket{
		start: epoch.Add(3 * res),
		val:   values.Float(3),
		prev: &bucket{
			start: epoch.Add(1 * res),
			val:   values.Float(2),
			prev: &bucket{
				start: epoch,
				val:   values.Float(1),
			},
		},
	}

	seq := b.toSequence(res).append(b2.toSequence(res), res, epoch)
	assert.Equal(t, epoch.Add(10*res), seq.start().In(time.UTC))
	assert.Equal(t, 11, seq.numBuckets())
	for i := time.Duration(-1); i <= 12; i++ {
		actual := int(seq.valueAtTime(epoch.Add(i*res), res))
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

	checkTruncation := func(offset time.Duration, length int, msg string) {
		merged := b.toSequence(res).append(b2.toSequence(res), res, epoch.Add(offset*res))
		if length == 0 {
			assert.Equal(t, emptySequence, merged, msg)
		} else {
			assert.Equal(t, length, merged.numBuckets(), msg)
		}
	}

	checkTruncation(1, 10, "Should partially truncate earlier sequence")
	checkTruncation(2, 9, "Should partially truncate earlier sequence")
	checkTruncation(3, 8, "Should partially truncate earlier sequence")
	checkTruncation(4, 6, "Should omit gap")
	checkTruncation(5, 6, "Should omit gap")
	checkTruncation(6, 5, "Should partially truncate later sequence")
	checkTruncation(7, 4, "Should partially truncate later sequence")
	checkTruncation(8, 3, "Should partially truncate later sequence")
	checkTruncation(9, 2, "Should partially truncate later sequence")
	checkTruncation(10, 1, "Should partially truncate later sequence")
	checkTruncation(11, 0, "Should omit empty sequence")
}
