package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tecbot/gorocksdb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Query struct {
	Table    string
	Fields   []string
	From     time.Time
	To       time.Time
	OnValues func(key map[string]interface{}, field string, vals []float64)
}

func (db *DB) RunQuery(q *Query) error {
	if q.From.IsZero() {
		return fmt.Errorf("Please specify a from")
	}
	if len(q.Fields) == 0 {
		return fmt.Errorf("Please specify at least one field")
	}
	t := db.getTable(q.Table)
	if t == nil {
		return fmt.Errorf("Unknown table %v", q.Table)
	}
	fields := make([][]byte, 0, len(q.Fields))
	for _, field := range q.Fields {
		fieldBytes, err := msgpack.Marshal(strings.ToLower(field))
		if err != nil {
			return fmt.Errorf("Unable to marshal field: %v", err)
		}
		fields = append(fields, fieldBytes)
	}
	sort.Sort(lexicographical(fields))
	if q.To.IsZero() {
		q.To = t.clock.Now()
	}
	from := roundTime(q.From, t.resolution)
	to := q.To
	if to.IsZero() {
		to = t.clock.Now()
	}
	to = roundTime(to, t.resolution)
	numPeriods := int(to.Sub(from)/t.resolution) + 1
	log.Tracef("Query will return %d periods", numPeriods)

	ro := gorocksdb.NewDefaultReadOptions()
	// Go ahead and fill the cache
	ro.SetFillCache(true)
	it := t.archiveByKey.NewIterator(ro)
	defer it.Close()
	for _, fieldBytes := range fields {
		scanned := 0
		read := 0
		for it.Seek(fieldBytes); it.ValidForPrefix(fieldBytes); it.Next() {
			scanned++
			k := it.Key()
			kr := bytes.NewReader(k.Data())
			dec := msgpack.NewDecoder(kr)
			storedField, err := dec.DecodeString()
			if err != nil {
				k.Free()
				return fmt.Errorf("Unable to decode field: %v", err)
			}
			key := make(map[string]interface{})
			err = dec.Decode(&key)
			if err != nil {
				k.Free()
				return fmt.Errorf("Unable to decode key: %v", err)
			}
			k.Free()

			v := it.Value()
			seq := sequence(v.Data())
			vals := make([]float64, numPeriods)
			if seq.isValid() {
				read++
				seqStart := seq.start()
				if log.IsTraceEnabled() {
					log.Tracef("Sequence starts at %v and has %d periods", seqStart.In(time.UTC), seq.numPeriods())
				}
				includeKey := false
				if !seqStart.Before(from) {
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
					q.OnValues(key, storedField, vals)
				}
			}
			v.Free()
		}
		log.Tracef("Query read/scanned %d/%d", read, scanned)
	}
	return nil
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
