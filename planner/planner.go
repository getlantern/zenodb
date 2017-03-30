// Package planner provides functionality for planning the execution of queries.
package planner

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
	"sync/atomic"
	"time"
)

var (
	log = golog.LoggerFor("planner")
)

type Table interface {
	core.RowSource
	GetPartitionBy() []string
}

type Opts struct {
	GetTable        func(table string, includedFields func(tableFields core.Fields) (core.Fields, error)) (Table, error)
	Now             func(table string) time.Time
	IsSubQuery      bool
	SubQueryResults [][]interface{}
	QueryCluster    QueryClusterFN
}

func Plan(sqlString string, opts *Opts) (core.FlatRowSource, error) {
	query, err := sql.Parse(sqlString)
	if err != nil {
		return nil, err
	}

	fixupSubQuery(query, opts)

	if opts.QueryCluster != nil {
		allowPushdown, err := pushdownAllowed(opts, query)
		if err != nil {
			return nil, err
		}
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
	fixupSubQuery(query, opts)

	var source core.RowSource
	if query.FromSubQuery != nil {
		subSource, err := Plan(query.FromSubQuery.SQL, opts)
		if err != nil {
			return nil, err
		}
		source = core.Unflatten(subSource, query.Fields)
	} else {
		var sourceErr error
		source, sourceErr = opts.GetTable(query.From, func(tableFields core.Fields) (core.Fields, error) {
			if query.HasSelectAll {
				// For SELECT *, include all table fields
				return tableFields, nil
			}

			tableExprs := tableFields.Exprs()

			// Otherwise, figure out minimum set of fields needed by query
			includedFields := make([]bool, len(tableFields))
			fields, err := query.Fields.Get(tableFields)
			if err != nil {
				return nil, err
			}
			for _, field := range fields {
				sms := field.Expr.SubMergers(tableExprs)
				for i, sm := range sms {
					if sm != nil {
						includedFields[i] = true
					}
				}
			}

			if query.Having != nil {
				having, err := query.Having.Get(append(tableFields, fields...))
				if err != nil {
					return nil, err
				}
				sms := having.SubMergers(tableExprs)
				for i, sm := range sms {
					if sm != nil {
						includedFields[i] = true
					}
				}
			}

			result := make(core.Fields, 0, len(tableFields))
			for i, included := range includedFields {
				if included {
					result = append(result, tableFields[i])
				}
			}

			return result, nil
		})

		if sourceErr != nil {
			return nil, sourceErr
		}
	}

	now := opts.Now(query.From)
	if query.AsOfOffset != 0 {
		query.AsOf = now.Add(query.AsOfOffset)
	}
	if query.UntilOffset != 0 {
		query.Until = now.Add(query.UntilOffset)
	}

	// Round asOf and until
	query.AsOf = encoding.RoundTimeUp(query.AsOf, source.GetResolution())
	query.Until = encoding.RoundTimeUp(query.Until, source.GetResolution())

	asOf := source.GetAsOf()
	asOfChanged := !query.AsOf.IsZero() && query.AsOf.UnixNano() != source.GetAsOf().UnixNano()
	if asOfChanged {
		asOf = query.AsOf
	}

	until := source.GetUntil()
	untilChanged := !query.Until.IsZero() && query.Until.UnixNano() != source.GetUntil().UnixNano()
	if untilChanged {
		until = query.Until
	}

	window := until.Sub(asOf)
	if query.Resolution > window {
		query.Resolution = window
	}

	resolutionChanged := query.Resolution != 0 && query.Resolution != source.GetResolution()
	if resolutionChanged {
		if query.Resolution < source.GetResolution() {
			return nil, fmt.Errorf("Query resolution '%v' is higher than table resolution of '%v'", query.Resolution, source.GetResolution())
		}
		if query.Resolution%source.GetResolution() != 0 {
			return nil, fmt.Errorf("Query resolution '%v' is not an even multiple of table resolution of '%v'", query.Resolution, source.GetResolution())
		}
	}

	if query.Where != nil {
		runSubQueries, subQueryPlanErr := planSubQueries(opts, query)
		if subQueryPlanErr != nil {
			return nil, subQueryPlanErr
		}

		hasRunSubqueries := int32(0)
		source = core.RowFilter(source, query.WhereSQL, func(ctx context.Context, key bytemap.ByteMap, fields core.Fields, vals core.Vals) (bytemap.ByteMap, core.Vals, error) {
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

	flat := core.Flatten(source)

	if query.Having != nil {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func fixupSubQuery(query *sql.Query, opts *Opts) {
	if opts.IsSubQuery {
		// Change field to _points field
		query.Fields = core.StaticFieldSource{sql.PointsField}
	}
}

func needsGroupBy(query *sql.Query) bool {
	return !query.GroupByAll || query.HasSpecificFields || query.Having != nil || query.Crosstab != nil
}

func addGroupBy(source core.RowSource, query *sql.Query) core.RowSource {
	// Need to do a group by
	fields := query.Fields
	if query.Having != nil {
		// Add having to fields
		fields = core.CombinedFieldSource{query.Fields, core.ExprFieldSource{Name: "_having", Expr: query.Having}}
	}

	return core.Group(source, core.GroupOpts{
		By:         query.GroupBy,
		Crosstab:   query.Crosstab,
		Fields:     fields,
		Resolution: query.Resolution,
		AsOf:       query.AsOf,
		Until:      query.Until,
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
