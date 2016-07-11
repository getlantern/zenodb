package tdb

import (
	// . "github.com/getlantern/tdb/expr"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"
)

// func TestBuildSequence(t *testing.T) {
// 	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
// 	res := time.Minute
// 	b := &bucket{
// 		start: epoch.Add(10 * res),
// 		vals:  []Accumulator{CONST(6).Accumulator()},
// 		prev: &bucket{
// 			start: epoch.Add(7 * res),
// 			vals:  []Accumulator{CONST(5).Accumulator()},
// 			prev: &bucket{
// 				start: epoch.Add(5 * res),
// 				vals:  []Accumulator{CONST(4).Accumulator()},
// 			},
// 		},
// 	}
//
// 	b2 := &bucket{
// 		start: epoch.Add(3 * res),
// 		vals:  []Accumulator{CONST(3).Accumulator()},
// 		prev: &bucket{
// 			start: epoch.Add(1 * res),
// 			vals:  []Accumulator{CONST(2).Accumulator()},
// 			prev: &bucket{
// 				start: epoch,
// 				vals:  []Accumulator{CONST(1).Accumulator()},
// 			},
// 		},
// 	}
//
// 	seq := b.toSequences(res)[0].append(b2.toSequences(res)[0], res, epoch)
// 	assert.Equal(t, epoch.Add(10*res), seq.start().In(time.UTC))
// 	assert.Equal(t, 11, seq.numPeriods())
// 	for i := time.Duration(-1); i <= 12; i++ {
// 		actual := int(seq.valueAtTime(epoch.Add(i*res), res))
// 		t.Logf("%d -> %d", i, actual)
// 		expected := 0
// 		switch i {
// 		case 10:
// 			expected = 6
// 		case 7:
// 			expected = 5
// 		case 5:
// 			expected = 4
// 		case 3:
// 			expected = 3
// 		case 1:
// 			expected = 2
// 		case 0:
// 			expected = 1
// 		}
// 		assert.Equal(t, expected, actual)
// 	}
//
// 	checkTruncation := func(offset time.Duration, length int, msg string) {
// 		merged := b.toSequences(res)[0].append(b2.toSequences(res)[0], res, epoch.Add(offset*res))
// 		if length == 0 {
// 			assert.Equal(t, emptySequence, merged, msg)
// 		} else {
// 			assert.Equal(t, length, merged.numPeriods(), msg)
// 		}
// 	}
//
// 	checkTruncation(1, 10, "Should partially truncate earlier sequence")
// 	checkTruncation(2, 9, "Should partially truncate earlier sequence")
// 	checkTruncation(3, 8, "Should partially truncate earlier sequence")
// 	checkTruncation(4, 6, "Should omit gap")
// 	checkTruncation(5, 6, "Should omit gap")
// 	checkTruncation(6, 5, "Should partially truncate later sequence")
// 	checkTruncation(7, 4, "Should partially truncate later sequence")
// 	checkTruncation(8, 3, "Should partially truncate later sequence")
// 	checkTruncation(9, 2, "Should partially truncate later sequence")
// 	checkTruncation(10, 1, "Should partially truncate later sequence")
// 	checkTruncation(11, 0, "Should omit empty sequence")
// }

func TestSequence(t *testing.T) {
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	res := time.Minute

	checkWithTruncation := func(retainPeriods int) {
		t.Logf("Retention periods: %d", retainPeriods)
		retentionPeriod := res * time.Duration(retainPeriods)
		trunc := func(vals []float64, ignoreTrailingIndex int) []float64 {
			if len(vals) > retainPeriods {
				vals = vals[:retainPeriods]
				if len(vals)-1 == ignoreTrailingIndex {
					// Remove trailing zero to deal with append deep
					vals = vals[:retainPeriods-1]
				}
			}
			return vals
		}

		start := epoch
		var seq sequence

		doIt := func(ts time.Time, value float64, expected []float64) {
			if ts.After(start) {
				start = ts
			}
			truncateBefore := start.Add(-1 * retentionPeriod)
			seq = seq.set(newValue(ts, value), res, truncateBefore)
			checkValues(t, seq, trunc(expected, 4))
		}

		// Set something on an empty sequence
		doIt(epoch, 2, []float64{2})

		// Prepend
		doIt(epoch.Add(2*res), 1, []float64{1, 0, 2})

		// Append
		doIt(epoch.Add(-1*res), 3, []float64{1, 0, 2, 3})

		// Append deep
		doIt(epoch.Add(-3*res), 4, []float64{1, 0, 2, 3, 0, 4})

		// Update value
		doIt(epoch, 5, []float64{1, 0, 5, 3, 0, 4})
	}

	for i := 6; i >= 0; i-- {
		checkWithTruncation(i)
	}
}

func checkValues(t *testing.T, seq sequence, expected []float64) {
	if assert.Equal(t, len(expected), seq.numPeriods()) {
		for i, v := range expected {
			assert.EqualValues(t, v, seq.valueAt(i))
		}
	}
}
