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
	ExpiredKeys     int64
}

type field struct {
	expr.Expr
	Name string
}

type table struct {
	name            string
	db              *DB
	log             golog.Logger
	fields          sortedFields
	fieldIndexes    map[string]int
	views           []*view
	viewsMutex      sync.RWMutex
	batchSize       int64
	clock           *vtime.Clock
	archiveByKey    *gorocksdb.DB
	resolution      time.Duration
	hotPeriod       time.Duration
	retentionPeriod time.Duration
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

type view struct {
	To   string
	Dims map[string]bool
}

func NewDB(opts *DBOpts) *DB {
	return &DB{opts: opts, tables: make(map[string]*table)}
}

func (db *DB) CreateTable(name string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, fields map[string]expr.Expr) error {
	name = strings.ToLower(name)
	fieldsArray := sortFields(fields)
	fieldIndexes := make(map[string]int, len(fieldsArray))
	for i, field := range fieldsArray {
		fieldIndexes[field.Name] = i
	}

	t := &table{
		name:            name,
		db:              db,
		log:             golog.LoggerFor("tdb." + name),
		fields:          fieldsArray,
		fieldIndexes:    fieldIndexes,
		views:           make([]*view, 0),
		batchSize:       db.opts.BatchSize,
		clock:           vtime.NewClock(time.Time{}),
		resolution:      resolution,
		hotPeriod:       hotPeriod,
		retentionPeriod: retentionPeriod,
		toArchive:       make(chan *archiveRequest, db.opts.BatchSize*100),
	}
	numCPU := runtime.NumCPU()
	t.partitions = make([]*partition, 0, numCPU)
	for i := 0; i < numCPU; i++ {
		p := &partition{
			t:            t,
			archiveDelay: time.Duration(i) * t.archivePeriod() / time.Duration(numCPU),
			inserts:      make(chan *insert, 100000/numCPU),
			tail:         make(map[string]*bucket),
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

func (db *DB) CreateView(from string, to string, resolution time.Duration, hotPeriod time.Duration, retentionPeriod time.Duration, dims ...string) error {
	f := db.getTable(from)
	if f == nil {
		return fmt.Errorf("Unable to create view, from table not found: %v", from)
	}
	fields := make(map[string]expr.Expr, len(f.fields))
	for _, field := range f.fields {
		fields[field.Name] = field.Expr
	}
	err := db.CreateTable(to, resolution, hotPeriod, retentionPeriod, fields)
	if err != nil {
		return fmt.Errorf("Unable to create view: %v", err)
	}
	view := &view{
		To:   strings.ToLower(to),
		Dims: make(map[string]bool, len(dims)),
	}
	for _, dim := range dims {
		view.Dims[dim] = true
	}
	f.viewsMutex.Lock()
	f.views = append(f.views, view)
	f.viewsMutex.Unlock()
	return nil
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

func (t *table) createDatabase(dir string, suffix string) (*gorocksdb.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	// TODO: make this tunable or auto-adjust based on table resolution or
	// something
	bbtopts.SetBlockCache(gorocksdb.NewLRUCache(256 * 1024 * 1024))
	filter := gorocksdb.NewBloomFilter(10)
	bbtopts.SetFilterPolicy(filter)
	opts.SetBlockBasedTableFactory(bbtopts)
	opts.SetCreateIfMissing(true)
	opts.SetMergeOperator(t)
	return gorocksdb.OpenDb(opts, filepath.Join(dir, t.name+"_"+suffix))
}

func roundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}
