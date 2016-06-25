package tdb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/boltdb/bolt"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type bucket struct {
	start  time.Time
	values []float64
	next   *bucket
	prev   *bucket
}

type series struct {
	t    *table
	head *bucket
	tail *bucket
	mx   sync.RWMutex
}

type table struct {
	bdb             *bolt.DB
	resolution      int64
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	fields          map[string]int
	derivedFields   map[string]*govaluate.EvaluableExpression
	series          map[string]*series
	stats           TableStats
	mx              sync.RWMutex
}

type TableStats struct {
	NumSeries  int
	NumBuckets int
}

type Point struct {
	Ts   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

type DB struct {
	dir    string
	tables map[string]*table
	mx     sync.RWMutex
}

func NewDB(dir string) *DB {
	return &DB{dir: dir, tables: make(map[string]*table)}
}

func (db *DB) CreateTable(name string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, derivedFields ...string) error {
	name = strings.ToLower(name)
	if len(derivedFields)%2 != 0 {
		return fmt.Errorf("derivedFields needs to be of the form [name] [expression] [name] [expression] ...")
	}
	bdb, err := bolt.Open(filepath.Join(db.dir, name), 0600, nil)
	if err != nil {
		return fmt.Errorf("Unable to open bolt database: %v", err)
	}
	numDerivedFields := len(derivedFields) / 2
	t := &table{
		bdb:             bdb,
		resolution:      int64(resolution),
		hotPeriod:       hotPeriod,
		retentionPeriod: retentionPeriod,
		fields:          make(map[string]int),
		derivedFields:   make(map[string]*govaluate.EvaluableExpression, numDerivedFields),
		series:          make(map[string]*series),
	}
	for i := 0; i < numDerivedFields; i++ {
		field := strings.ToLower(derivedFields[i*2])
		expression := strings.ToLower(derivedFields[i*2+1])
		expr, err := govaluate.NewEvaluableExpression(expression)
		if err != nil {
			return fmt.Errorf("Field %v has an invalid expression %v: %v", field, expression, err)
		}
		t.derivedFields[field] = expr
	}
	db.mx.Lock()
	defer db.mx.Unlock()
	if db.tables[name] != nil {
		return fmt.Errorf("Table %v already exists", name)
	}
	db.tables[name] = t
	return nil
}

func (db *DB) Insert(table string, point *Point) error {
	db.mx.RLock()
	t := db.tables[table]
	db.mx.RUnlock()
	if t == nil {
		return fmt.Errorf("Unknown table %v", t)
	}
	return t.Insert(point)
}

func (t *table) Insert(point *Point) error {
	key, err := point.key()
	if err != nil {
		return err
	}

	t.mx.Lock()
	// Update fields
	fieldsCopy := make(map[string]int, len(t.fields))
	for field := range point.Vals {
		field = strings.ToLower(field)
		idx, found := t.fields[field]
		if !found {
			// New field
			idx = len(t.fields)
			t.fields[field] = idx
		}
		fieldsCopy[field] = idx
	}
	s := t.series[key]
	if s == nil {
		s = &series{t: t}
		t.series[key] = s
		t.stats.NumSeries++
	}
	t.mx.Unlock()

	return s.insert(fieldsCopy, point)
}

func (s *series) insert(fields map[string]int, point *Point) error {
	start := time.Unix(0, point.Ts.UnixNano()/s.t.resolution*s.t.resolution)

	s.mx.Lock()
	defer s.mx.Unlock()

	if s.tail != nil && point.Ts.Before(s.tail.start.Add(time.Duration(s.t.resolution)).Add(-1*s.t.hotPeriod)) {
		return fmt.Errorf("Timestamp falls outside of hot period, insertion not allowed")
	}

	prev := s.tail
	var next *bucket
	for {
		if prev == nil || prev.start.Before(start) {
			// Add a new bucket
			b := &bucket{
				start:  start,
				values: point.values(fields),
				prev:   prev,
				next:   next,
			}
			if prev != nil {
				prev.next = b
			} else {
				s.head = b
			}
			if next != nil {
				next.prev = b
			} else {
				s.tail = b
			}
			return nil
		}
		if prev.start == start {
			// Insert to existing bucket
			prev.insert(fields, point)
		}
		next = prev
		prev = prev.prev
	}
}

func (b *bucket) insert(fields map[string]int, point *Point) error {
	for field, val := range point.Vals {
		fieldIdx := fields[strings.ToLower(field)]
		b.values[fieldIdx] = b.values[fieldIdx] + val
	}
	return nil
}

func (point *Point) key() (string, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(point.Dims)
	if err != nil {
		return "", fmt.Errorf("Unable to encode dims")
	}
	return buf.String(), nil
}

func (point *Point) values(fields map[string]int) []float64 {
	values := make([]float64, len(fields))
	for field, val := range point.Vals {
		values[fields[strings.ToLower(field)]] = val
	}
	return values
}
