package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/tecbot/gorocksdb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type query struct {
	table    string
	fields   []string
	filter   *govaluate.EvaluableExpression
	from     time.Time
	to       time.Time
	onValues func(key map[string]interface{}, field string, vals []float64)
}

type QueryStats struct {
	Scanned      int64
	FilterPass   int64
	FilterReject int64
	ReadValue    int64
	DataValid    int64
	InTimeRange  int64
}

func (db *DB) runQuery(q *query) (*QueryStats, error) {
	stats := &QueryStats{}

	if q.from.IsZero() {
		return stats, fmt.Errorf("Please specify a from")
	}
	if len(q.fields) == 0 {
		return stats, fmt.Errorf("Please specify at least one field")
	}
	t := db.getTable(q.table)
	if t == nil {
		return stats, fmt.Errorf("Unknown table %v", q.table)
	}
	fields := make([][]byte, 0, len(q.fields))
	for _, field := range q.fields {
		fieldBytes, err := msgpack.Marshal(strings.ToLower(field))
		if err != nil {
			return stats, fmt.Errorf("Unable to marshal field: %v", err)
		}
		fields = append(fields, fieldBytes)
	}
	sort.Sort(lexicographical(fields))
	if q.to.IsZero() {
		q.to = t.clock.Now()
	}
	q.from = roundTime(q.from, t.resolution)
	if q.to.IsZero() {
		q.to = t.clock.Now()
	}
	q.to = roundTime(q.to, t.resolution)
	numPeriods := int(q.to.Sub(q.from)/t.resolution) + 1
	log.Tracef("Query will return %d periods", numPeriods)

	ro := gorocksdb.NewDefaultReadOptions()
	// Go ahead and fill the cache
	ro.SetFillCache(true)
	it := t.archiveByKey.NewIterator(ro)
	defer it.Close()

	for _, fieldBytes := range fields {
		for it.Seek(fieldBytes); it.ValidForPrefix(fieldBytes); it.Next() {
			stats.Scanned++
			k := it.Key()
			kr := bytes.NewReader(k.Data())
			dec := msgpack.NewDecoder(kr)
			storedField, err := dec.DecodeString()
			if err != nil {
				k.Free()
				return stats, fmt.Errorf("Unable to decode field: %v", err)
			}
			key := make(map[string]interface{})
			err = dec.Decode(&key)
			if err != nil {
				k.Free()
				return stats, fmt.Errorf("Unable to decode key: %v", err)
			}
			k.Free()

			if q.filter != nil {
				include, err := q.filter.Evaluate(key)
				if err != nil {
					return stats, fmt.Errorf("Unable to apply filter: %v", err)
				}
				inc, ok := include.(bool)
				if !ok {
					return stats, fmt.Errorf("Filter expression returned something other than a boolean: %v", include)
				}
				if !inc {
					stats.FilterReject++
					continue
				}
				stats.FilterPass++
			}

			v := it.Value()
			stats.ReadValue++
			seq := sequence(v.Data())
			vals := make([]float64, numPeriods)
			if seq.isValid() {
				stats.DataValid++
				seqStart := seq.start()
				if log.IsTraceEnabled() {
					log.Tracef("Sequence starts at %v and has %d periods", seqStart.In(time.UTC), seq.numPeriods())
				}
				includeKey := false
				if !seqStart.Before(q.from) {
					to := q.to
					if to.After(seqStart) {
						to = seqStart
					}
					startOffset := int(seqStart.Sub(to) / t.resolution)
					log.Tracef("Start offset %d", startOffset)
					copyPeriods := seq.numPeriods()
					for i := 0; i+startOffset < copyPeriods && i < numPeriods; i++ {
						includeKey = true
						val := seq.valueAt(i + startOffset)
						log.Tracef("Grabbing value %f", val)
						vals[i] = val
					}
				}
				if includeKey {
					stats.InTimeRange++
					q.onValues(key, storedField, vals)
				}
			}
			v.Free()
		}
	}
	return stats, nil
}

type lexicographical [][]byte

func (a lexicographical) Len() int           { return len(a) }
func (a lexicographical) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a lexicographical) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) < 0 }

func keysEqual(a map[string]interface{}, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
