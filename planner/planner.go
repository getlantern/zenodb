// Package planner provides functionality for planning the execution of queries.
package planner

import (
	"context"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"sync/atomic"
	"time"
)

var (
	log = golog.LoggerFor("planner")
)

type QueryClusterFN func(ctx context.Context, sqlString string, subQueryResults [][]interface{}, onRow core.OnFlatRow) error

type Opts struct {
	IsSubquery    bool
	GetTable      func(table string) core.RowSource
	Now           func(table string) time.Time
	FieldSource   sql.FieldSource
	QueryCluster  QueryClusterFN
	PartitionKeys []string
}

func Plan(sqlString string, opts *Opts) (core.FlatRowSource, error) {
	query, err := sql.Parse(sqlString, opts.FieldSource)
	if err != nil {
		return nil, err
	}

	if opts.IsSubquery {
		// Change field to _points field
		query.Fields[0] = sql.PointsField
	}

	if opts.QueryCluster != nil {
		allowPushdown := pushdownAllowed(opts, query)
		if allowPushdown {
			return planClusterPushdown(opts, query)
		}
		if query.FromSubQuery == nil {
			return planClusterNonPushdown(opts, query)
		}
	}

	return planLocal(query, opts)
}

func planLocal(query *sql.Query, opts *Opts) (core.FlatRowSource, error) {
	var source core.RowSource
	if query.FromSubQuery != nil {
		subSource, err := Plan(query.FromSubQuery.SQL, opts)
		if err != nil {
			return nil, err
		}
		source = core.Unflatten(subSource, query.Fields...)
	} else {
		source = opts.GetTable(query.From)
	}

	now := opts.Now(query.From)
	if query.AsOfOffset != 0 {
		query.AsOf = now.Add(query.AsOfOffset)
	}
	if query.UntilOffset != 0 {
		query.Until = now.Add(query.UntilOffset)
	}

	asOfChanged := !query.AsOf.IsZero() && query.AsOf.UnixNano() != source.GetAsOf().UnixNano()
	untilChanged := !query.Until.IsZero() && query.Until.UnixNano() != source.GetUntil().UnixNano()
	resolutionChanged := query.Resolution != 0 && query.Resolution != source.GetResolution()

	if query.Where != nil {
		runSubQueries, subQueryPlanErr := planSubQueries(opts, query)
		if subQueryPlanErr != nil {
			return nil, subQueryPlanErr
		}

		hasRunSubqueries := int32(0)
		source = core.RowFilter(source, query.WhereSQL, func(ctx context.Context, key bytemap.ByteMap, vals core.Vals) (bytemap.ByteMap, core.Vals, error) {
			if atomic.CompareAndSwapInt32(&hasRunSubqueries, 0, 1) {
				_, err := runSubQueries(ctx)
				if err != nil && err != core.ErrDeadlineExceeded {
					return nil, nil, err
				}
			}
			result := query.Where.Eval(key)
			if result != nil && result.(bool) {
				return key, vals, nil
			}
			return nil, nil, nil
		})
	}

	if asOfChanged || untilChanged || resolutionChanged || needsGroupBy(query) {
		source = addGroupBy(source, query)
	}

	var flat core.FlatRowSource = core.Flatten(source)

	if query.Having != nil {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func needsGroupBy(query *sql.Query) bool {
	return !query.GroupByAll || query.HasSpecificFields || query.Having != nil
}

func addGroupBy(source core.RowSource, query *sql.Query) core.RowSource {
	// Need to do a group by
	fields := query.Fields
	if query.Having != nil {
		// Add having to fields
		fields = make([]core.Field, 0, len(query.Fields))
		fields = append(fields, query.Fields...)
		fields = append(fields, core.NewField("_having", query.Having))
	}

	return core.Group(source, core.GroupOpts{
		By:         query.GroupBy,
		Fields:     fields,
		Resolution: query.Resolution,
		AsOf:       query.AsOf,
		Until:      query.Until,
	})
}

func addHaving(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	havingIdx := len(query.Fields)
	return core.FlatRowFilter(flat, query.HavingSQL, func(ctx context.Context, row *core.FlatRow) (*core.FlatRow, error) {
		include := row.Values[havingIdx]
		if include == 1 {
			// Removing having field
			row.Values = row.Values[:havingIdx]
			return row, nil
		}
		return nil, nil
	})
}

func addOrderLimitOffset(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	if len(query.OrderBy) > 0 {
		flat = core.Sort(flat, query.OrderBy...)
	}

	if query.Offset > 0 {
		flat = core.Offset(flat, query.Offset)
	}

	if query.Limit > 0 {
		flat = core.Limit(flat, query.Limit)
	}

	return flat
}
