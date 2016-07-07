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
	"github.com/getlantern/tdb/sql"
	"github.com/getlantern/vtime"
	"github.com/tecbot/gorocksdb"
)

var (
	log = golog.LoggerFor("tdb")
)

type TableStats struct {
	FilteredPoints  int64
	InsertedPoints  int64
	DroppedPoints   int64
	HotKeys         int64
	ArchivedBuckets int64
	ExpiredKeys     int64
}

type table struct {
	sql.Query
	db              *DB
	name            string
	sqlString       string
	log             golog.Logger
	batchSize       int64
	clock           *vtime.Clock
	archiveByKey    *gorocksdb.DB
	hotPeriod       time.Duration
	retentionPeriod time.Duration
	partitions      []*partition
	toArchive       chan *archiveRequest
	where           *govaluate.EvaluableExpression
	whereMutex      sync.RWMutex
	stats           TableStats
	statsMutex      sync.RWMutex
}

type DBOpts struct {
	SchemaFile string
	Dir        string
	BatchSize  int64
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
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
	}
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
		toArchive:       make(chan *archiveRequest, db.opts.BatchSize*100),
	}

	err := t.applyWhere(q.Where)
	if err != nil {
		return err
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

	err = os.MkdirAll(db.opts.Dir, 0755)
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
