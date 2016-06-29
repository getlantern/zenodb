package tdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/getlantern/golog"
	"github.com/oxtoacart/tdb/expr"
	"github.com/oxtoacart/vtime"
	"github.com/tecbot/gorocksdb"
)

var (
	log = golog.LoggerFor("tdb")
)

type TableStats struct {
	InsertedPoints  int64
	DroppedPoints   int64
	HotKeys         int64
	ArchivedBuckets int64
}

type DerivedField struct {
	Name string
	Expr expr.Expr
}

type table struct {
	name            string
	batchSize       int64
	clock           *vtime.Clock
	archiveByKey    *gorocksdb.DB
	resolution      time.Duration
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	derivedFields   []DerivedField
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

func (db *DB) CreateTable(name string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, derivedFields ...DerivedField) error {
	name = strings.ToLower(name)
	t := &table{
		name:            name,
		batchSize:       db.opts.BatchSize,
		clock:           vtime.NewClock(time.Time{}),
		resolution:      resolution,
		hotPeriod:       hotPeriod,
		retentionPeriod: retentionPeriod,
		derivedFields:   derivedFields,
		toArchive:       make(chan *archiveRequest, db.opts.BatchSize*100),
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

	err := os.MkdirAll(db.opts.Dir, 0755)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("Unable to create folder for rocksdb database: %v", err)
	}
	t.archiveByKey, err = t.createDatabase(db.opts.Dir, "bykey")
	if err != nil {
		return fmt.Errorf("Unable to create rocksdb database: %v", err)
	}
	db.tables[name] = t

	for i := 0; i < numCPU; i++ {
		go t.partitions[i].processInserts()
	}

	go t.archive()
	go t.retain()

	return nil
}

func (t *table) createDatabase(dir string, suffix string) (*gorocksdb.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbtopts.SetBlockCache(gorocksdb.NewLRUCache(256 * 1024 * 1024))
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
