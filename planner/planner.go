// Package planner provides functionality for planning the execution of queries.
package planner

import (
	"context"
	"errors"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"sync/atomic"
	"time"
)

var (
	ErrNoSQL               = errors.New("Need at least one SQL string")
	ErrNoMultipleOnCluster = errors.New("Multiple SQL strings not allowed when querying cluster")
	ErrIncompatibleTables  = errors.New("Planning multiple SQL strings only allowed with queries from the same table (and not subqueries)")
	ErrNestedFromSubquery  = errors.New("nested FROM subqueries not supported")

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

func Plan(opts *Opts, sqlStrings ...string) (core.FlatRowSource, error) {
	if len(sqlStrings) == 0 {
		return nil, ErrNoSQL
	}
	if opts.QueryCluster != nil && len(sqlStrings) > 1 {
		return nil, ErrNoMultipleOnCluster
	}

	queries := make([]*sql.Query, 0, len(sqlStrings))
	for _, sqlString := range sqlStrings {
		query, err := sql.Parse(sqlString, opts.FieldSource)
		if err != nil {
			return nil, err
		}

		if opts.IsSubquery {
			// Change field to _points field
			query.Fields[0] = sql.PointsField
		}

		queries = append(queries, query)
	}

	if len(queries) > 1 {
		return planMultiple(opts, queries)
	}

	return planSingle(opts, queries[0])
}

func planSingle(opts *Opts, query *sql.Query) (core.FlatRowSource, error) {
	var source core.RowSource
	if opts.QueryCluster != nil {
		allowPushdown := pushdownAllowed(opts, query)
		if allowPushdown {
			return planClusterPushdown(opts, query)
		}
		if query.FromSubQuery == nil {
			return planClusterNonPushdown(opts, query)
		}
	}

	if source == nil {
		if query.FromSubQuery != nil {
			subSource, err := Plan(opts, query.FromSubQuery.SQL)
			if err != nil {
				return nil, err
			}
			unflatten := core.Unflatten(query.Fields...)
			unflatten.Connect(subSource)
			source = unflatten
		} else {
			source = opts.GetTable(query.From)
		}
	}
	return doPlan(opts, query, source)
}

func planMultiple(opts *Opts, queries []*sql.Query) (core.FlatRowSource, error) {
	// Mutiple queries, check that they're all from the same table
	lastFrom := ""
	for i, query := range queries {
		if query.FromSubQuery != nil {
			return nil, ErrIncompatibleTables
		}
		if i > 0 && query.From != lastFrom {
			return nil, ErrIncompatibleTables
		}
		lastFrom = query.From
	}

	table := opts.GetTable(lastFrom)
	s := core.NewSplitter()
	s.Connect(table)
	m := core.MergeFlat()
	for _, query := range queries {
		plan, err := doPlan(opts, query, s.Split())
		if err != nil {
			return nil, err
		}
		m.Connect(plan)
	}

	return m, nil
}

func doPlan(opts *Opts, query *sql.Query, source core.RowSource) (core.FlatRowSource, error) {
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
		filter := &core.RowFilter{
			Include: func(ctx context.Context, key bytemap.ByteMap, vals core.Vals) (bytemap.ByteMap, core.Vals, error) {
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
			},
			Label: query.WhereSQL,
		}
		filter.Connect(source)
		source = filter
	}

	if asOfChanged || untilChanged || resolutionChanged || needsGroupBy(query) {
		source = addGroupBy(source, query)
	}

	flat := flatten(source)

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

	group := &core.Group{
		By:         query.GroupBy,
		Fields:     fields,
		Resolution: query.Resolution,
		AsOf:       query.AsOf,
		Until:      query.Until,
	}
	group.Connect(source)
	return group
}

func flatten(source core.RowSource) core.FlatRowSource {
	flatten := core.Flatten()
	flatten.Connect(source)
	return flatten
}

func addHaving(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	havingIdx := len(query.Fields)
	filter := &core.FlatRowFilter{
		Include: func(ctx context.Context, row *core.FlatRow) (*core.FlatRow, error) {
			include := row.Values[havingIdx]
			if include == 1 {
				// Removing having field
				row.Values = row.Values[:havingIdx]
				return row, nil
			}
			return nil, nil
		},
		Label: query.HavingSQL,
	}
	filter.Connect(flat)
	return filter
}

func addOrderLimitOffset(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	if len(query.OrderBy) > 0 {
		sort := core.Sort(query.OrderBy...)
		sort.Connect(flat)
		flat = sort
	}

	if query.Offset > 0 {
		offset := core.Offset(query.Offset)
		offset.Connect(flat)
		flat = offset
	}

	if query.Limit > 0 {
		limit := core.Limit(query.Limit)
		limit.Connect(flat)
		flat = limit
	}

	return flat
}
