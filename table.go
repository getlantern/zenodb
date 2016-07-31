package tibsdb

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
	"github.com/getlantern/tibsdb/sql"
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
	MinFlushLatency  time.Duration
	MaxFlushLatency  time.Duration
	RetentionPeriod  time.Duration
	SQL              string
}

type table struct {
	*TableOpts
	sql.Query
	db         *DB
	rowStore   *rowStore
	log        golog.Logger
	where      *govaluate.EvaluableExpression
	whereMutex sync.RWMutex
	stats      TableStats
	statsMutex sync.RWMutex
	inserts    chan (*insert)
}

func (db *DB) CreateTable(opts *TableOpts) error {
	if opts.RetentionPeriod <= 0 {
		return errors.New("Please specify a positive RetentionPeriod")
	}
	if opts.MaxMemStoreBytes <= 0 {
		opts.MaxMemStoreBytes = 100000000
		log.Debugf("Defaulted MaxMemStoreBytes to %v", opts.MaxMemStoreBytes)
	}
	if opts.MinFlushLatency <= 0 {
		log.Debug("MinFlushLatency disabled")
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
		log:       golog.LoggerFor("tibsdb." + opts.Name),
		inserts:   make(chan *insert, 1000),
	}

	err := t.applyWhere(q.Where)
	if err != nil {
		return err
	}

	var rsErr error
	t.rowStore, rsErr = t.openRowStore(&rowStoreOptions{
		dir:              filepath.Join(db.opts.Dir, t.Name),
		maxMemStoreBytes: t.MaxMemStoreBytes,
		minFlushLatency:  t.MinFlushLatency,
		maxFlushLatency:  t.MaxFlushLatency,
	})
	if rsErr != nil {
		return rsErr
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
	return clock.Now().Add(-1 * t.RetentionPeriod)
}
