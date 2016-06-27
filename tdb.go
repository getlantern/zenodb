package tdb

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/golog"
	"github.com/oxtoacart/vtime"
	"github.com/tecbot/gorocksdb"
)

var (
	log = golog.LoggerFor("tdb")
)

type bucket struct {
	start time.Time
	val   float64
	prev  *bucket
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
	batchSize       int64
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

type DBOpts struct {
	Dir       string
	BatchSize int64
}

type DB struct {
	opts        *DBOpts
	tables      map[string]*table
	tablesMutex sync.RWMutex
}

func NewDB(opts *DBOpts) *DB {
	return &DB{opts: opts, tables: make(map[string]*table)}
}

func (db *DB) CreateTable(name string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, derivedFields ...string) error {
	name = strings.ToLower(name)
	if len(derivedFields)%2 != 0 {
		return fmt.Errorf("derivedFields needs to be of the form [name] [expression] [name] [expression] ...")
	}

	numDerivedFields := len(derivedFields) / 2
	t := &table{
		name:            name,
		batchSize:       db.opts.BatchSize,
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
	t.archiveByKey, err = t.createDatabase(db.opts.Dir, "bykey")
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
	return gorocksdb.OpenDb(opts, filepath.Join(dir, t.name+"_"+suffix))
}

func (db *DB) TableStats(table string) TableStats {
	t := db.getTable(table)
	if t == nil {
		return TableStats{}
	}
	t.statsMutex.RLock()
	defer t.statsMutex.RUnlock()
	return t.stats
}

func (db *DB) Now(table string) time.Time {
	t := db.getTable(table)
	if t == nil {
		return time.Time{}
	}
	return t.clock.Now()
}

func (db *DB) getTable(table string) *table {
	db.tablesMutex.RLock()
	t := db.tables[strings.ToLower(table)]
	db.tablesMutex.RUnlock()
	return t
}

func roundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}
