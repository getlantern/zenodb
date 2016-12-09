package zenodb

import (
	"time"

	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
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
		GetTable:    db.getTable,
		Now:         db.now,
		FieldSource: db,
	})
	if err != nil {
		return nil, err
	}
	var rows []*core.FlatRow
	ctx := core.Context()
	numPeriods := 0
	plan.Iterate(ctx, func(row *core.FlatRow) (bool, error) {
		rows = append(rows, row)
		for _, vals := range row.Values {
			newPeriods := len(vals)
			if newPeriods > numPeriods {
				numPeriods = newPeriods
			}
		}
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
		GroupBy:    core.GetMD(ctx, core.MDKeyDims),
		NumPeriods: numPeriods,
		Rows:       rows,
	}, nil
}
