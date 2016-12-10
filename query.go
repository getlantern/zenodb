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

type QueryResult struct {
	AsOf         time.Time
	Until        time.Time
	Resolution   time.Duration
	FieldNames   []string // FieldNames are needed for serializing QueryResult across rpc
	IsCrosstab   bool
	CrosstabDims []interface{}
	GroupBy      []string
	Stats        *QueryStats
	NumPeriods   int
	Plan         string
}

type QueryStats struct {
	Scanned      int64
	FilterPass   int64
	FilterReject int64
	ReadValue    int64
	DataValid    int64
	InTimeRange  int64
	Runtime      time.Duration
}

func (db *DB) SQLQuery(sqlString string, subQueryResults [][]interface{}, isSubQuery bool, includeMemStore bool, onRow core.OnFlatRow) (*QueryResult, error) {
	plan, err := planner.Plan(sqlString, &planner.Opts{
		GetTable: func(table string, includedFields func(tableFields core.Fields) core.Fields) core.RowSource {
			return db.getQueryable(table, includedFields, includeMemStore)
		},
		Now:             db.now,
		FieldSource:     db.getFields,
		SubQueryResults: subQueryResults,
		IsSubQuery:      isSubQuery,
	})
	if err != nil {
		return nil, err
	}
	log.Debugf("\n------------ Query Plan ------------\n\n%v\n----------- End Query Plan ----------", core.FormatSource(plan))

	ctx := core.Context()
	iterErr := plan.Iterate(ctx, onRow)
	if iterErr != nil {
		return nil, iterErr
	}

	planString := core.FormatSource(plan)
	var groupBy []string
	groupByMD := core.GetMD(ctx, core.MDKeyDims)
	if groupByMD != nil {
		groupBy = groupByMD.([]string)
	}

	return &QueryResult{
		AsOf:       plan.GetAsOf(),
		Until:      plan.GetUntil(),
		Resolution: plan.GetResolution(),
		FieldNames: plan.GetFields().Names(),
		GroupBy:    groupBy,
		NumPeriods: int(plan.GetUntil().Sub(plan.GetAsOf()) / plan.GetResolution()),
		Plan:       planString,
	}, nil
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

type queryable struct {
	t               *table
	fields          core.Fields
	asOf            time.Time
	until           time.Time
	includeMemStore bool
}

func (q *queryable) GetFields() core.Fields {
	return q.fields
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
	return q.t.iterate(q.GetFields().Names(), q.includeMemStore, func(key bytemap.ByteMap, vals []encoding.Sequence) {
		onRow(key, vals)
	})
}
