package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/oxtoacart/bytemap"
	"github.com/tecbot/gorocksdb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type query struct {
	table      string
	fields     []string
	filter     *govaluate.EvaluableExpression
	to         time.Time
	toOffset   time.Duration
	from       time.Time
	fromOffset time.Duration
	onValues   func(key bytemap.ByteMap, field string, vals []float64)
}

type QueryStats struct {
	Scanned      int64
	FilterPass   int64
	FilterReject int64
	ReadValue    int64
	DataValid    int64
	InTimeRange  int64
	Runtime      time.Duration
}

func (db *DB) runQuery(q *query) (*QueryStats, error) {
	start := time.Now()
	stats := &QueryStats{}

	if q.from.IsZero() && q.fromOffset >= 0 {
		return stats, fmt.Errorf("Please specify a from or a negative fromOffset")
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
	now := t.clock.Now()
	if q.to.IsZero() {
		q.to = now
		if q.toOffset != 0 {
			q.to = q.to.Add(q.toOffset)
		}
	}
	if q.from.IsZero() {
		q.from = now.Add(q.fromOffset)
	}
	q.to = roundTime(q.to, t.resolution)
	q.from = roundTime(q.from, t.resolution)
	numPeriods := int(q.to.Sub(q.from) / t.resolution)
	log.Tracef("Query will return %d periods for range %v to %v", numPeriods, q.from, q.to)

	ro := gorocksdb.NewDefaultReadOptions()
	// Go ahead and fill the cache
	ro.SetFillCache(true)
	it := t.archiveByKey.NewIterator(ro)
	defer it.Close()

	for _, fieldBytes := range fields {
		for it.Seek(fieldBytes); it.ValidForPrefix(fieldBytes); it.Next() {
			stats.Scanned++
			k := it.Key()
			kr := bytes.NewBuffer(k.Data())
			kr.Reset()
			dec := msgpack.NewDecoder(kr)
			storedField, err := dec.DecodeString()
			if err != nil {
				k.Free()
				return stats, fmt.Errorf("Unable to decode field: %v", err)
			}
			key := bytemap.ByteMap(kr.Bytes())

			if q.filter != nil {
				include, err := q.filter.Eval(byteMapParams(key))
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
			k.Free()
			v.Free()
		}
	}

	stats.Runtime = time.Now().Sub(start)
	return stats, nil
}

type byteMapParams bytemap.ByteMap

func (bmp byteMapParams) Get(field string) (interface{}, error) {
	return bytemap.ByteMap(bmp).Get(field), nil
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
