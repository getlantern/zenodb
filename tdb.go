package tdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/tdb/sql"
	"github.com/getlantern/vtime"
	"github.com/tecbot/gorocksdb"
)

var (
	log = golog.LoggerFor("tdb")
)

type TableStats struct {
	FilteredPoints int64
	QueuedPoints   int64
	InsertedPoints int64
	DroppedPoints  int64
	DirtyPoints    int64
	ArchivedPoints int64
	ExpiredPoints  int64
}

type table struct {
	sql.Query
	db              *DB
	name            string
	sqlString       string
	log             golog.Logger
	batchSize       int64
	clock           *vtime.Clock
	rdb             *gorocksdb.DB
	accum           *gorocksdb.ColumnFamilyHandle
	dirty           *gorocksdb.ColumnFamilyHandle
	archive         *gorocksdb.ColumnFamilyHandle
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	inserts         chan *insert
	archiveRequests chan *gorocksdb.Snapshot
	where           *govaluate.EvaluableExpression
	whereMutex      sync.RWMutex
	stats           TableStats
	statsMutex      sync.RWMutex
}

type DBOpts struct {
	SchemaFile string
	Dir        string
	BatchSize  int64
	QueueDepth int64
}

type DB struct {
	opts        *DBOpts
	streams     map[string][]*table
	tables      map[string]*table
	tablesMutex sync.RWMutex
}

type view struct {
	To   string
	Dims map[string]bool
}

func NewDB(opts *DBOpts) (*DB, error) {
	var err error
	db := &DB{opts: opts, tables: make(map[string]*table), streams: make(map[string][]*table)}
	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}
	if opts.QueueDepth == 0 {
		opts.QueueDepth = opts.BatchSize * 1000
	}
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
	}
	log.Debugf("Dir: %v    SchemaFile: %v    BatchSize: %d    QueueDepth: %d", opts.Dir, opts.SchemaFile, opts.BatchSize, opts.QueueDepth)
	return db, err
}

func (db *DB) CreateTable(name string, hotPeriod time.Duration, retentionPeriod time.Duration, sqlString string) error {
	if hotPeriod <= 0 {
		return errors.New("Please specify a positive hot period")
	}
	if retentionPeriod <= 0 {
		return errors.New("Please specify a positive retention period")
	}
	q, err := sql.Parse(sqlString)
	if err != nil {
		return err
	}

	return db.doCreateTable(name, hotPeriod, retentionPeriod, sqlString, q)
}

func (db *DB) doCreateTable(name string, hotPeriod time.Duration, retentionPeriod time.Duration, sqlString string, q *sql.Query) error {
	name = strings.ToLower(name)

	t := &table{
		Query:           *q,
		db:              db,
		name:            name,
		sqlString:       sqlString,
		log:             golog.LoggerFor("tdb." + name),
		batchSize:       db.opts.BatchSize,
		clock:           vtime.NewClock(time.Time{}),
		hotPeriod:       hotPeriod,
		retentionPeriod: retentionPeriod,
		inserts:         make(chan *insert, db.opts.QueueDepth),
		archiveRequests: make(chan *gorocksdb.Snapshot, 1),
	}

	err := t.applyWhere(q.Where)
	if err != nil {
		return err
	}

	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()

	if db.tables[name] != nil {
		return fmt.Errorf("Table %v already exists", name)
	}

	err = os.MkdirAll(db.opts.Dir, 0755)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("Unable to create folder for rocksdb database: %v", err)
	}
	err = t.createDatabase(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("Unable to create rocksdb database: %v", err)
	}
	db.tables[name] = t

	go t.process()
	go t.processArchiving()
	go t.retain()

	db.streams[q.From] = append(db.streams[q.From], t)

	return nil
}

func (t *table) applyWhere(where string) error {
	var e *govaluate.EvaluableExpression
	var err error
	if where != "" {
		e, err = govaluate.NewEvaluableExpression(where)
		if err != nil {
			return fmt.Errorf("Unable to parse where: %v", err)
		}
	}
	t.whereMutex.Lock()
	t.Where = where
	t.where = e
	t.whereMutex.Unlock()
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

func (db *DB) AllTableStats() map[string]TableStats {
	m := make(map[string]TableStats)
	tables := make(map[string]*table, 0)
	db.tablesMutex.RLock()
	for name, t := range db.tables {
		tables[name] = t
	}
	db.tablesMutex.RUnlock()
	for name, t := range tables {
		t.statsMutex.RLock()
		m[name] = t.stats
		t.statsMutex.RUnlock()
	}
	return m
}

func (db *DB) PrintTableStats(table string) string {
	stats := db.TableStats(table)
	now := db.Now(table)
	return fmt.Sprintf("%v (%v)\tFiltered: %v    Queued: %v    Inserted: %v    Dropped: %v    Dirty: %v    Archived: %v",
		table,
		now.In(time.UTC),
		humanize.Comma(stats.FilteredPoints),
		humanize.Comma(stats.QueuedPoints),
		humanize.Comma(stats.InsertedPoints),
		humanize.Comma(stats.DroppedPoints),
		humanize.Comma(stats.DirtyPoints),
		humanize.Comma(stats.ArchivedPoints))
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

func (t *table) createDatabase(dir string) error {
	opts := t.buildDBOpts(nil)
	cfNames := []string{"accumulators", "dirty", "default"}
	cfOpts := []*gorocksdb.Options{t.buildDBOpts(&accumMerger{t}), t.buildDBOpts(nil), t.buildDBOpts(&archiveMerger{t})}
	rdb, cfs, err := gorocksdb.OpenDbColumnFamilies(opts, filepath.Join(dir, t.name), cfNames, cfOpts)
	if err != nil {
		return err
	}
	t.rdb = rdb
	t.accum = cfs[0]
	t.dirty = cfs[1]
	t.archive = cfs[2]
	return nil
}

func (t *table) buildDBOpts(mergeOperator gorocksdb.MergeOperator) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	// TODO: make this tunable or auto-adjust based on table resolution or
	// something
	bbtopts.SetBlockCache(gorocksdb.NewLRUCache(128 * 1024 * 1024))
	filter := gorocksdb.NewBloomFilter(10)
	bbtopts.SetFilterPolicy(filter)
	opts.SetBlockBasedTableFactory(bbtopts)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	if mergeOperator != nil {
		opts.SetMergeOperator(mergeOperator)
	}
	return opts
}

func roundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}

func (t *table) truncateBefore() time.Time {
	return t.clock.Now().Add(-1 * t.retentionPeriod)
}
