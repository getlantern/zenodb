package zenodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/getlantern/bytemap"

	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
)

var (
	ErrOutOfMemory = errors.New("out of memory")
)

func (db *DB) Query(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool) (core.FlatRowSource, error) {
	opts := &planner.Opts{
		GetTable: func(table string, outFields func(tableFields core.Fields) (core.Fields, error)) (planner.Table, error) {
			return db.getQueryable(table, outFields, includeMemStore)
		},
		Now:             db.now,
		IsSubQuery:      isSubQuery,
		SubQueryResults: subQueryResults,
	}
	if db.opts.Passthrough {
		opts.QueryCluster = func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) (interface{}, error) {
			return db.queryCluster(ctx, sqlString, isSubQuery, subQueryResults, includeMemStore, unflat, onFields, onRow, onFlatRow)
		}
	}
	plan, err := planner.Plan(sqlString, opts)
	if err != nil {
		return nil, err
	}
	log.Debugf("\n------------ Query Plan ------------\n\n%v\n\n%v\n----------- End Query Plan ----------", sqlString, core.FormatSource(plan))
	return plan, nil
}

func (db *DB) getQueryable(table string, outFields func(tableFields core.Fields) (core.Fields, error), includeMemStore bool) (*queryable, error) {
	t := db.getTable(table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", table)
	}
	if t.Virtual {
		return nil, fmt.Errorf("Table %v is virtual and cannot be queried", table)
	}
	until := encoding.RoundTimeUp(db.clock.Now(), t.Resolution)
	asOf := encoding.RoundTimeUp(until.Add(-1*t.RetentionPeriod), t.Resolution)
	fields := t.getFields()
	out, err := outFields(fields)
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = t.getFields()
	}
	return &queryable{db, t, out, asOf, until, includeMemStore}, nil
}

func MetaDataFor(source core.FlatRowSource, fields core.Fields) *common.QueryMetaData {
	return &common.QueryMetaData{
		FieldNames: fields.Names(),
		AsOf:       source.GetAsOf(),
		Until:      source.GetUntil(),
		Resolution: source.GetResolution(),
		Plan:       core.FormatSource(source),
	}
}

type queryable struct {
	db              *DB
	t               *table
	fields          core.Fields
	asOf            time.Time
	until           time.Time
	includeMemStore bool
}

func (q *queryable) GetGroupBy() []core.GroupBy {
	return q.t.GroupBy
}

func (q *queryable) GetResolution() time.Duration {
	return q.t.Resolution
}

func (q *queryable) GetAsOf() time.Time {
	return q.asOf
}

func (q *queryable) GetUntil() time.Time {
	return q.until
}

func (q *queryable) GetPartitionBy() []string {
	return q.t.PartitionBy
}

func (q *queryable) String() string {
	return q.t.Name
}

func (q *queryable) Iterate(ctx context.Context, onFields core.OnFields, onRow core.OnRow) (interface{}, error) {
	// We report all fields from the table
	err := onFields(q.fields)
	if err != nil {
		return nil, err
	}

	i := 0
	// When iterating, as an optimization, we read only the needed fields (not
	// all table fields).
	highWaterMark, err := q.t.iterate(ctx, q.fields, q.includeMemStore, func(key bytemap.ByteMap, vals []encoding.Sequence) (bool, error) {
		if i%1000 == 0 {
			// every 1000 rows, check and cap memory size
			if !q.db.capMemorySize(false) || true {
				log.Error("Returning ErrOutOfMemory")
				return false, ErrOutOfMemory
			}
		}
		i++
		return onRow(key, vals)
	})
	if err != nil {
		log.Errorf("Error on iterating: %v", err)
	}
	numSuccessfulPartitions := 0
	if err == nil {
		numSuccessfulPartitions = 1
	}
	return &common.QueryStats{
		NumPartitions:           1,
		NumSuccessfulPartitions: numSuccessfulPartitions,
		LowestHighWaterMark:     common.TimeToMillis(highWaterMark),
		HighestHighWaterMark:    common.TimeToMillis(highWaterMark),
	}, err
}
