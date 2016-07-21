package tdb

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"
	"github.com/getlantern/vtime"
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
	columnStores    []*columnStore
	name            string
	sqlString       string
	log             golog.Logger
	batchSize       int64
	clock           *vtime.Clock
	retentionPeriod time.Duration
	inserts         chan *insert
	where           *govaluate.EvaluableExpression
	whereMutex      sync.RWMutex
	stats           TableStats
	statsMutex      sync.RWMutex
	accums          *sync.Pool
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

	t.columnStores = make([]*columnStore, 0, len(t.Fields))
	for _, field := range t.Fields {
		cs, csErr := openColumnStore(&columnStoreOptions{
			dir:            filepath.Join(db.opts.Dir, field.Name),
			ex:             field.Expr,
			resolution:     t.Resolution,
			truncateBefore: t.truncateBefore,
			numMemStores:   2,
			flushAt:        int(db.opts.BatchSize),
		})
		if csErr != nil {
			return csErr
		}
		t.columnStores = append(t.columnStores, cs)
	}

	go t.processInserts()

	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()

	if db.tables[name] != nil {
		return fmt.Errorf("Table %v already exists", name)
	}
	db.tables[name] = t
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
