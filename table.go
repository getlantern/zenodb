package tdb

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
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

type TableOpts struct {
	Name             string
	MaxMemStoreBytes int
	MaxFlushLatency  time.Duration
	RetentionPeriod  time.Duration
	SQL              string
}

type table struct {
	*TableOpts
	sql.Query
	db           *DB
	columnStores []*columnStore
	log          golog.Logger
	clock        *vtime.Clock
	where        *govaluate.EvaluableExpression
	whereMutex   sync.RWMutex
	stats        TableStats
	statsMutex   sync.RWMutex
	inserts      chan (*insert)
}

func (db *DB) CreateTable(opts *TableOpts) error {
	if opts.RetentionPeriod <= 0 {
		return errors.New("Please specify a positive RetentionPeriod")
	}
	if opts.MaxMemStoreBytes <= 0 {
		opts.MaxMemStoreBytes = 100000000
		log.Debugf("Defaulted MaxMemStoreBytes to %v", opts.MaxMemStoreBytes)
	}
	if opts.MaxFlushLatency <= 0 {
		opts.MaxFlushLatency = time.Duration(math.MaxInt64)
		log.Debug("MaxFlushLatency disabled")
	}
	q, err := sql.Parse(opts.SQL)
	if err != nil {
		return err
	}

	return db.doCreateTable(opts, q)
}

func (db *DB) doCreateTable(opts *TableOpts, q *sql.Query) error {
	opts.Name = strings.ToLower(opts.Name)

	t := &table{
		TableOpts: opts,
		Query:     *q,
		db:        db,
		log:       golog.LoggerFor("tdb." + opts.Name),
		clock:     vtime.NewClock(time.Time{}),
		inserts:   make(chan *insert, 1000),
	}

	err := t.applyWhere(q.Where)
	if err != nil {
		return err
	}

	t.columnStores = make([]*columnStore, 0, len(t.Fields))
	for _, field := range t.Fields {
		cs, csErr := openColumnStore(&columnStoreOptions{
			dir:              filepath.Join(db.opts.Dir, t.Name, field.Name),
			ex:               field.Expr,
			resolution:       t.Resolution,
			truncateBefore:   t.truncateBefore,
			maxMemStoreBytes: t.MaxMemStoreBytes,
			maxFlushLatency:  t.MaxFlushLatency,
		})
		if csErr != nil {
			return csErr
		}
		t.columnStores = append(t.columnStores, cs)
	}

	go t.processInserts()

	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()

	if db.tables[t.Name] != nil {
		return fmt.Errorf("Table %v already exists", t.Name)
	}
	db.tables[t.Name] = t
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
	return t.clock.Now().Add(-1 * t.RetentionPeriod)
}
