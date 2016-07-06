package tdb

import (
	"math/rand"
	"sort"

	. "github.com/getlantern/tdb/expr"

	"testing"
)

func TestSort(t *testing.T) {
	k := 1000000
	entries := make([]*Entry, 0, k)
	for i := 0; i < k; i++ {
		entries = append(entries, &Entry{orderByValues: []Accumulator{CONST(rand.Float64()).Accumulator()}})
	}

	sort.Sort(orderedEntries(entries))

	var lastEntry *Entry
	for i, entry := range entries {
		if lastEntry != nil && entry.orderByValues[0].Get() < lastEntry.orderByValues[0].Get() {
			t.Fatalf("Out of order on %d", i)
		}
		lastEntry = entry
	}
}
