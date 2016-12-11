package zenodb

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
	"time"
)

func (db *DB) Query(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool) (core.FlatRowSource, error) {
	opts := &planner.Opts{
		GetTable: func(table string, includedFields func(tableFields core.Fields) core.Fields) core.RowSource {
			return db.getQueryable(table, includedFields, includeMemStore)
		},
		Now:             db.now,
		FieldSource:     db.getFields,
		IsSubQuery:      isSubQuery,
		SubQueryResults: subQueryResults,
	}
	if db.opts.Passthrough {
		opts.QueryCluster = func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, onRow core.OnFlatRow) error {
			return db.queryCluster(ctx, sqlString, isSubQuery, subQueryResults, includeMemStore, onRow)
		}
		opts.PartitionBy = db.opts.PartitionBy
	}
	plan, err := planner.Plan(sqlString, opts)
	if err != nil {
		return nil, err
	}
	log.Debugf("\n------------ Query Plan ------------\n\n%v\n\n%v\n----------- End Query Plan ----------", sqlString, core.FormatSource(plan))
	return plan, nil
}

func (db *DB) getQueryable(table string, includedFields func(tableFields core.Fields) core.Fields, includeMemStore bool) *queryable {
	t := db.getTable(table)
	if t == nil {
		return nil
	}
	until := encoding.RoundTime(db.clock.Now(), t.Resolution)
	asOf := encoding.RoundTime(until.Add(-1*t.RetentionPeriod), t.Resolution)
	return &queryable{t, includedFields(t.Fields), asOf, until, includeMemStore}
}

type QueryMetaData struct {
	FieldNames []string
	AsOf       time.Time
	Until      time.Time
	Resolution time.Duration
	Plan       string
}

func MetaDataFor(source core.FlatRowSource) *QueryMetaData {
	return &QueryMetaData{
		FieldNames: source.GetFields().Names(),
		AsOf:       source.GetAsOf(),
		Until:      source.GetUntil(),
		Resolution: source.GetResolution(),
		Plan:       core.FormatSource(source),
	}
}

type queryable struct {
	t               *table
	fields          core.Fields
	asOf            time.Time
	until           time.Time
	includeMemStore bool
}

func (q *queryable) GetFields() core.Fields {
	// We report all fields from the table
	return q.t.Fields
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

func (q *queryable) String() string {
	return fmt.Sprintf("%v (%v)", q.t.Name, q.GetFields().Names())
}

func (q *queryable) Iterate(ctx context.Context, onRow core.OnRow) error {
	// When iterating, as an optimization, we read only the needed fields (not
	// all table fields).
	return q.t.iterate(q.fields.Names(), q.includeMemStore, func(key bytemap.ByteMap, vals []encoding.Sequence) {
		onRow(key, vals)
	})
}
