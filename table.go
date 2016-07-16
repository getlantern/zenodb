package tdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"
	"github.com/getlantern/vtime"
	"github.com/tecbot/gorocksdb"
)

type TableStats struct {
	FilteredPoints int64
	QueuedPoints   int64
	InsertedPoints int64
	DroppedPoints  int64
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
	data            *gorocksdb.ColumnFamilyHandle
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	inserts         chan *insert
	where           *govaluate.EvaluableExpression
	whereMutex      sync.RWMutex
	stats           TableStats
	statsMutex      sync.RWMutex
	accums          *sync.Pool
}

var (
	// use a single block cache for all databases
	blockCache = gorocksdb.NewLRUCache(1024 * 1024 * 1024)

	// use a single env for all databases
	defaultEnv = gorocksdb.NewDefaultEnv()
)

func init() {
	defaultEnv.SetBackgroundThreads(runtime.NumCPU())
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
		inserts:         make(chan *insert, db.opts.BatchSize*2),
	}
	t.accums = &sync.Pool{New: t.newAccumulators}

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

func (t *table) createDatabase(dir string) error {
	opts := t.buildDBOpts(nil)
	cfNames := []string{"default"}
	cfOpts := []*gorocksdb.Options{t.buildDBOpts(&merger{t})}
	rdb, cfs, err := gorocksdb.OpenDbColumnFamilies(opts, filepath.Join(dir, t.name), cfNames, cfOpts)
	if err != nil {
		return err
	}
	t.rdb = rdb
	t.data = cfs[0]
	return nil
}

func (t *table) buildDBOpts(mergeOperator gorocksdb.MergeOperator) *gorocksdb.Options {
	// See https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
	opts := gorocksdb.NewDefaultOptions()
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	// TODO: make this tunable or auto-adjust based on table resolution or
	// something
	bbtopts.SetBlockCache(blockCache)
	// Use a large block size
	bbtopts.SetBlockSize(65536)
	opts.SetBlockBasedTableFactory(bbtopts)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.EnableStatistics()
	opts.SetStatsDumpPeriodSec(60) // dump stats every 60 seconds
	opts.SetEnv(defaultEnv)
	opts.SetMaxOpenFiles(-1) // don't limit number of open files
	opts.SetWriteBufferSize(128 * 1024 * 1024)
	opts.SetMaxWriteBufferNumber(5)
	opts.SetMinWriteBufferNumberToMerge(2)
	if mergeOperator != nil {
		opts.SetMergeOperator(mergeOperator)
	}
	opts.SetPrefixExtractor(&prefixExtractor{})
	return opts
}

func (t *table) truncateBefore() time.Time {
	return t.clock.Now().Add(-1 * t.retentionPeriod)
}

func (t *table) getAccumulators() []expr.Accumulator {
	return t.accums.Get().([]expr.Accumulator)
}

func (t *table) putAccumulators(accums []expr.Accumulator) {
	t.accums.Put(accums)
}

func (t *table) newAccumulators() interface{} {
	accums := make([]expr.Accumulator, 0, len(t.Fields))
	for _, field := range t.Fields {
		accums = append(accums, field.Accumulator())
	}
	return accums
}
