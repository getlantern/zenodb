package tdb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/golog"
	"github.com/oxtoacart/vtime"
	"github.com/spaolacci/murmur3"
	"github.com/tecbot/gorocksdb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	log = golog.LoggerFor("tdb")
)

type bucket struct {
	start time.Time
	val   float64
	prev  *bucket
}

type insert struct {
	ts  time.Time
	key []byte
	val float64
}

type archiveRequest struct {
	key string
	b   *bucket
}

type partition struct {
	t       *table
	inserts chan *insert
	tail    map[string]*bucket
}

type TableStats struct {
	HotKeys         int64
	HotBuckets      int64
	ArchivedBuckets int64
}

type table struct {
	name            string
	clock           *vtime.Clock
	archiveByKey    *gorocksdb.DB
	resolution      time.Duration
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	derivedFields   map[string]*govaluate.EvaluableExpression
	partitions      []*partition
	toArchive       chan *archiveRequest
	stats           TableStats
	statsMutex      sync.RWMutex
}

type DB struct {
	dir         string
	tables      map[string]*table
	tablesMutex sync.RWMutex
}

type Point struct {
	Ts   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

func NewDB(dir string) *DB {
	return &DB{dir: dir, tables: make(map[string]*table)}
}

func (db *DB) CreateTable(name string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, derivedFields ...string) error {
	name = strings.ToLower(name)
	if len(derivedFields)%2 != 0 {
		return fmt.Errorf("derivedFields needs to be of the form [name] [expression] [name] [expression] ...")
	}

	numDerivedFields := len(derivedFields) / 2
	t := &table{
		name:            name,
		clock:           vtime.NewClock(time.Time{}),
		resolution:      resolution,
		hotPeriod:       hotPeriod,
		retentionPeriod: retentionPeriod,
		derivedFields:   make(map[string]*govaluate.EvaluableExpression, numDerivedFields),
		toArchive:       make(chan *archiveRequest, 100000),
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
	numCPU := runtime.NumCPU()
	t.partitions = make([]*partition, 0, numCPU)
	for i := 0; i < numCPU; i++ {
		p := &partition{
			t:       t,
			inserts: make(chan *insert, 100000/numCPU),
			tail:    make(map[string]*bucket),
		}
		t.partitions = append(t.partitions, p)
	}

	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()

	if db.tables[name] != nil {
		return fmt.Errorf("Table %v already exists", name)
	}

	var err error
	t.archiveByKey, err = t.createDatabase(db.dir, "bykey")
	if err != nil {
		return fmt.Errorf("Unable to create rocksdb database: %v", err)
	}
	db.tables[name] = t

	for i := 0; i < numCPU; i++ {
		go t.partitions[i].processInserts()
	}

	go t.archive()

	return nil
}

func (t *table) createDatabase(dir string, suffix string) (*gorocksdb.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbtopts.SetBlockCache(gorocksdb.NewLRUCache(32 * 1024 * 1024))
	filter := gorocksdb.NewBloomFilter(10)
	bbtopts.SetFilterPolicy(filter)
	opts.SetBlockBasedTableFactory(bbtopts)
	opts.SetCreateIfMissing(true)
	opts.SetMergeOperator(t)
	opts.SetComparator(t)
	return gorocksdb.OpenDb(opts, filepath.Join(dir, t.name+"_"+suffix))
}

func (db *DB) TableStats(table string) TableStats {
	db.tablesMutex.RLock()
	t := db.tables[table]
	db.tablesMutex.RUnlock()
	if t == nil {
		return TableStats{}
	}
	t.statsMutex.RLock()
	defer t.statsMutex.RUnlock()
	return t.stats
}

func (db *DB) Insert(table string, point *Point) error {
	db.tablesMutex.RLock()
	t := db.tables[table]
	db.tablesMutex.RUnlock()
	if t == nil {
		return fmt.Errorf("Unknown table %v", t)
	}

	return t.insert(point)
}

func (t *table) insert(point *Point) error {
	t.clock.Advance(point.Ts)
	for field, val := range point.Vals {
		key, err := point.keyFor(field)
		if err != nil {
			return err
		}
		h := int(murmur3.Sum32(key))
		p := h % len(t.partitions)
		t.partitions[p].inserts <- &insert{point.Ts, key, val}
	}

	return nil
}

func (p *partition) processInserts() {
	// TODO: base this on the passage of fake time
	archivePeriod := p.t.hotPeriod / 10
	log.Debugf("Archiving every %v", archivePeriod)
	archiveTicker := p.t.clock.NewTicker(archivePeriod)
	for {
		select {
		case insert := <-p.inserts:
			p.insert(insert)
		case <-archiveTicker.C:
			p.requestArchiving()
		}
	}
}

func (p *partition) insert(insert *insert) {
	key := string(insert.key)
	now := p.t.clock.Now()
	start := roundTime(insert.ts, p.t.resolution)
	if now.Sub(start) > p.t.hotPeriod {
		log.Trace("Discarding insert outside of hot period")
		return
	}
	b := p.tail[key]
	if b == nil || b.start.Before(start) {
		p.t.statsMutex.Lock()
		p.t.stats.HotBuckets++
		if b == nil {
			p.t.stats.HotKeys++
		}
		p.t.statsMutex.Unlock()
		b = &bucket{start, insert.val, b}
		p.tail[key] = b
		return
	}
	for {
		if b.start == start {
			// Update existing bucket
			b.val += insert.val
			return
		}
		if b.prev == nil || b.prev.start.Before(start) {
			// Insert new bucket
			p.t.statsMutex.Lock()
			p.t.stats.HotBuckets++
			p.t.statsMutex.Unlock()
			b.prev = &bucket{start, insert.val, b.prev}
			return
		}
		// Continue looking
		b = b.prev
	}
}

func (p *partition) requestArchiving() {
	now := p.t.clock.Now()
	log.Tracef("Requested archiving at %v", now)
	for key, b := range p.tail {
		if now.Sub(b.start) > p.t.hotPeriod {
			log.Tracef("Archiving full. %v / %v %v", b.start, now, b.prev != nil)
			delete(p.tail, key)
			p.t.statsMutex.Lock()
			p.t.stats.HotKeys--
			p.t.statsMutex.Unlock()
			p.t.toArchive <- &archiveRequest{key, b}
			continue
		}
		next := b
		for {
			b = b.prev
			if b == nil {
				break
			}
			log.Tracef("Checking %v", b.start)
			if now.Sub(b.start) > p.t.hotPeriod {
				log.Trace("Archiving partial")
				p.t.toArchive <- &archiveRequest{key, b}
				next.prev = nil
				break
			}
		}
	}
}

func (t *table) archive() {
	wo := gorocksdb.NewDefaultWriteOptions()

	batch := gorocksdb.NewWriteBatch()
	for req := range t.toArchive {
		key := []byte(req.key)
		batch.Merge(key, req.b.toSequence(t.resolution))
		count := int64(batch.Count())
		if count >= 100 {
			err := t.archiveByKey.Write(wo, batch)
			if err != nil {
				log.Errorf("Unable to write batch: %v", err)
			}
			t.statsMutex.Lock()
			t.stats.HotBuckets -= count
			t.stats.ArchivedBuckets += count
			t.statsMutex.Unlock()
			batch = gorocksdb.NewWriteBatch()
		}
	}
}

func (p *Point) keyFor(field string) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(p.Dims)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode dims: %v", err)
	}
	err = enc.Encode(field)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode field: %v", err)
	}
	return buf.Bytes(), nil
}

func roundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}

// FullMerge implements method from gorocksdb.MergeOperator
func (t *table) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	for _, operand := range operands {
		if operand != nil && len(operand) > size64bits*2 {
			if existingValue == nil || len(existingValue) < size64bits*2 {
				existingValue = sequence(operand)
			} else {
				existingValue = sequence(existingValue).append(sequence(operand), t.resolution)
			}
		}
	}
	return existingValue, true
}

// PartialMerge implements method from gorocksdb.MergeOperator
func (t *table) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return sequence(rightOperand).append(sequence(leftOperand), t.resolution), true
}

// Compare implements method from gorocksdb.Comparator, sorting in reverse
// lexicographical order.
func (t *table) Compare(a, b []byte) int {
	return bytes.Compare(b, a)
}

// Name implements method from gorocksdb.MergeOperator and from
// gorocksdb.Comparator.
func (t *table) Name() string {
	return t.name
}
