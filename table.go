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
	ExpiredValues  int64
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
	retentionPeriod time.Duration
	inserts         chan *insert
	where           *govaluate.EvaluableExpression
	whereMutex      sync.RWMutex
	stats           TableStats
	statsMutex      sync.RWMutex
	accums          *sync.Pool
}

var (
	// use a single env for all databases
	defaultEnv = gorocksdb.NewDefaultEnv()
)

func init() {
	// Background threads are used for compaction, use cores - 1
	defaultEnv.SetBackgroundThreads(runtime.NumCPU() - 1)
	// High priority background threads are used for flushing, just need 1
	defaultEnv.SetHighPriorityBackgroundThreads(1)
}

func (db *DB) CreateTable(name string, retentionPeriod time.Duration, sqlString string) error {
	if retentionPeriod <= 0 {
		return errors.New("Please specify a positive retention period")
	}
	q, err := sql.Parse(sqlString)
	if err != nil {
		return err
	}

	return db.doCreateTable(name, retentionPeriod, sqlString, q)
}

func (db *DB) doCreateTable(name string, retentionPeriod time.Duration, sqlString string, q *sql.Query) error {
	name = strings.ToLower(name)

	t := &table{
		Query:           *q,
		db:              db,
		name:            name,
		sqlString:       sqlString,
		log:             golog.LoggerFor("tdb." + name),
		batchSize:       db.opts.BatchSize,
		clock:           vtime.NewClock(time.Time{}),
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
	// go t.retain()

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
	opts := t.buildDBOpts()
	rdb, err := gorocksdb.OpenDb(opts, filepath.Join(dir, t.name))
	if err != nil {
		return err
	}
	t.rdb = rdb
	return nil
}

func (t *table) buildDBOpts() *gorocksdb.Options {
	/*********************** General Config *************************************/
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	// All keys are prefixed by the field name, giving us something resembling a
	// column-store optimized for querying one or a few fields.
	opts.SetPrefixExtractor(&fieldPrefixExtractor{})
	// Updates to existing values are handled as merges
	opts.SetMergeOperator(&merger{t})
	// On compaction, we filter out expired values
	opts.SetCompactionFilter(&filterExpired{t})

	/*********************** Tuning *********************************************/
	// See https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
	// TODO: make this tunable or auto-adjust based on table resolution or
	// something
	bbtopts := gorocksdb.NewDefaultBlockBasedTableOptions()
	// We're continuously just appending values and queries just do range scans,
	// so there's no use for a block cache or bloom filters, and we might as well
	// create large blocks because we'll scan most of their contents anyway.
	bbtopts.SetBlockSize(65536)
	bbtopts.SetNoBlockCache(true)
	// Note - we don't set a bloom filter
	// bbtopts.SetFilterPolicy(...)
	opts.SetBlockBasedTableFactory(bbtopts)
	// Use a shared environment for all column families (shares thread pool)
	opts.SetEnv(defaultEnv)
	// Set background compactions to same number as threads in defaultEnv
	opts.SetMaxBackgroundCompactions(runtime.NumCPU() - 1)
	// Don't limit number of open files, avoids expensive table cache lookups
	opts.SetMaxOpenFiles(-1)
	// Use more write buffers in order to avoid stalling writes during flush/
	// compaction.
	opts.SetMaxWriteBufferNumber(20)
	// Increase number of write buffers that get merged on flush in order to
	// reduce read amplification.
	opts.SetMinWriteBufferNumberToMerge(4)
	// Compact aggressively to reduce the number of outstanding merge ops (speeds
	// up queries).
	opts.SetLevel0FileNumCompactionTrigger(1)

	// Suggested by rocksdb documentation, allocate a memtable budget of 128 MiB
	// opts.OptimizeLevelStyleCompaction(128 * 1024 * 1024)

	if t.db.opts.RocksDBStatsInterval > 0 {
		opts.EnableStatistics()
		opts.SetStatsDumpPeriodSec(uint(t.db.opts.RocksDBStatsInterval / time.Second))
	}

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
