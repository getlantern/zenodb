package zenodb

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
)

// TableStats presents statistics for a given table (currently only since the
// last time the database process was started).
type TableStats struct {
	FilteredPoints int64
	QueuedPoints   int64
	InsertedPoints int64
	DroppedPoints  int64
	ExpiredValues  int64
}

// TableOpts configures a table.
type TableOpts struct {
	// Name is the name of the table.
	Name string
	// View indicates if this table is a view on top of an existing table.
	View bool
	// MaxMemStoreBytes sets a cap on how large the memstore is allowed to become
	// before being flushed to disk.
	MaxMemStoreBytes int
	// MinFlushLatency sets a lower bound on how frequently the memstore is
	// flushed to disk.
	MinFlushLatency time.Duration
	// MaxFlushLatency sets an upper bound on how long to wait before flushing the
	// memstore to disk.
	MaxFlushLatency time.Duration
	// RetentionPeriod limits how long data is kept in the table (based on the
	// timestamp of the data itself).
	RetentionPeriod time.Duration
	// SQL is the SELECT query that determines the fields, filtering and input
	// source for this table.
	SQL string
}

type table struct {
	*TableOpts
	sql.Query
	db         *DB
	rowStore   *rowStore
	log        golog.Logger
	whereMutex sync.RWMutex
	stats      TableStats
	statsMutex sync.RWMutex
	inserts    chan (*insert)
}

// CreateTable creates a table based on the given opts.
func (db *DB) CreateTable(opts *TableOpts) error {
	q, err := sql.Parse(opts.SQL, nil)
	if err != nil {
		return err
	}
	return db.doCreateTable(opts, q)
}

// CreateView creates a view based on the given opts.
func (db *DB) CreateView(opts *TableOpts) error {
	table, err := sql.TableFor(opts.SQL)
	if err != nil {
		return err
	}

	// Get existing fields from existing table
	t := db.getTable(table)
	if t == nil {
		return fmt.Errorf("Table '%v' not found", table)
	}
	q, err := sql.Parse(opts.SQL, db.getFieldsOptional)
	if err != nil {
		return err
	}

	// Point view at same stream as table
	// TODO: populate view with existing data from table
	q.From = t.From

	if q.GroupBy == nil {
		q.GroupBy = t.GroupBy
		q.GroupByAll = t.GroupByAll
	}

	if q.Resolution == 0 {
		q.Resolution = t.Resolution
	}

	// Combine where clauses
	if t.Where != nil {
		if q.Where == nil {
			q.Where = t.Where
		} else {
			combined, err := goexpr.Binary("AND", q.Where, t.Where)
			if err != nil {
				return err
			}
			q.Where = combined
		}
	}

	return db.doCreateTable(opts, q)
}

func (db *DB) doCreateTable(opts *TableOpts, q *sql.Query) error {
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
	opts.Name = strings.ToLower(opts.Name)

	// prepend a magic _points field
	newFields := make([]sql.Field, 0, len(q.Fields)+1)
	newFields = append(newFields, sql.NewField("_points", expr.SUM("_point")))
	for _, field := range q.Fields {
		// Don't add _points twice
		if field.Name != "_points" {
			newFields = append(newFields, field)
		}
	}
	q.Fields = newFields

	t := &table{
		TableOpts: opts,
		Query:     *q,
		db:        db,
		log:       golog.LoggerFor("zenodb." + opts.Name),
		inserts:   make(chan *insert, 10000),
	}

	t.applyWhere(q.Where)

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

func (t *table) applyWhere(where goexpr.Expr) {
	t.whereMutex.Lock()
	t.Where = where
	t.whereMutex.Unlock()
}

func (t *table) truncateBefore() time.Time {
	return t.db.clock.Now().Add(-1 * t.RetentionPeriod)
}
