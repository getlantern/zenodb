package zenodb

import (
	"context"
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
	Rows         []*core.FlatRow
	Stats        *QueryStats
	NumPeriods   int
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

func (db *DB) SQLQuery(sqlString string, includeMemStore bool) (*QueryResult, error) {
	plan, err := planner.Plan(sqlString, &planner.Opts{
		GetTable: func(table string) core.RowSource {
			return db.getQueryable(table, includeMemStore)
		},
		Now:         db.now,
		FieldSource: db.getFields,
	})
	if err != nil {
		return nil, err
	}
	var rows []*core.FlatRow
	ctx := core.Context()
	plan.Iterate(ctx, func(row *core.FlatRow) (bool, error) {
		rows = append(rows, row)
		return true, nil
	})
	fields := plan.GetFields()
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}

	return &QueryResult{
		AsOf:       plan.GetAsOf(),
		Until:      plan.GetUntil(),
		Resolution: plan.GetResolution(),
		FieldNames: fieldNames,
		GroupBy:    core.GetMD(ctx, core.MDKeyDims).([]string),
		NumPeriods: int(plan.GetUntil().Sub(plan.GetAsOf()) / plan.GetResolution()),
		Rows:       rows,
	}, nil
}

func (db *DB) getQueryable(table string, includeMemStore bool) *queryable {
	t := db.getTable(table)
	if t == nil {
		return nil
	}
	until := encoding.RoundTime(db.clock.Now(), t.Resolution)
	asOf := encoding.RoundTime(until.Add(-1*t.RetentionPeriod), t.Resolution)
	return &queryable{t, asOf, until, includeMemStore}
}

type queryable struct {
	t               *table
	asOf            time.Time
	until           time.Time
	includeMemStore bool
}

func (q *queryable) GetFields() core.Fields {
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
	return q.String()
}

func (q *queryable) Iterate(ctx context.Context, onRow core.OnRow) error {
	fields := q.t.Fields
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}
	return q.t.iterate(fieldNames, q.includeMemStore, func(key bytemap.ByteMap, vals []encoding.Sequence) {
		onRow(key, vals)
	})
}
