package planner

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
)

func planLocal(query *sql.Query, opts *Opts) (core.FlatRowSource, error) {
	fixupSubQuery(query, opts)

	var source core.RowSource
	var err error
	if query.FromSubQuery != nil {
		source, err = sourceForSubQuery(query, opts)
		if err != nil {
			return nil, err
		}
	} else {
		source, err = sourceForTable(query, opts)
		if err != nil {
			return nil, err
		}
	}

	now := opts.Now(query.From)
	asOf, asOfChanged, until, untilChanged := asOfUntilFor(query, opts, source, now)

	resolution, strideSlice, resolutionChanged, resolutionTruncated, err := resolutionFor(query, opts, source, asOf, until)
	if err != nil {
		return nil, err
	}

	if query.Where != nil {
		source, err = applySubQueryFilters(query, opts, source)
		if err != nil {
			return nil, err
		}
	}

	needsGroupBy := asOfChanged || untilChanged || resolutionChanged ||
		!query.GroupByAll || query.HasSpecificFields || query.Having != nil ||
		query.Crosstab != nil || strideSlice > 0
	if needsGroupBy {
		source = addGroupBy(source, query, resolutionTruncated || resolutionChanged, resolution, strideSlice)
	}

	flat := core.Flatten(source)

	if query.Having != nil {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func sourceForSubQuery(query *sql.Query, opts *Opts) (core.RowSource, error) {
	subSource, err := Plan(query.FromSubQuery.SQL, opts)
	if err != nil {
		return nil, err
	}
	return core.Unflatten(subSource, query.Fields), nil
}

func sourceForTable(query *sql.Query, opts *Opts) (core.RowSource, error) {
	return opts.GetTable(query.From, func(tableFields core.Fields) (core.Fields, error) {
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
}

func asOfUntilFor(query *sql.Query, opts *Opts, source core.RowSource, now time.Time) (time.Time, bool, time.Time, bool) {
	// TODO: take into account the SHIFT to make sure we're not reading SHIFT from
	// empty data.
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

	return asOf, asOfChanged, until, untilChanged
}

func resolutionFor(query *sql.Query, opts *Opts, source core.RowSource, asOf time.Time, until time.Time) (time.Duration, time.Duration, bool, bool, error) {
	resolution := query.Resolution
	var strideSlice time.Duration
	if resolution == 0 {
		resolution = source.GetResolution()
	}

	if query.Stride > 0 {
		if query.Stride%source.GetResolution() != 0 {
			return 0, 0, false, false, fmt.Errorf("Query stride '%v' is not an even multiple of table resolution '%v'", query.Stride, source.GetResolution())
		}
		strideSlice = resolution
		resolution = query.Stride
	}

	resolutionTruncated := false
	window := until.Sub(asOf)
	if resolution > window {
		resolution = window
		resolutionTruncated = true
	}

	resolutionChanged := resolution != source.GetResolution()
	if resolutionChanged {
		if resolution < source.GetResolution() {
			return 0, 0, false, false, fmt.Errorf("Query resolution '%v' is higher than table resolution '%v'", resolution, source.GetResolution())
		}
		if resolution%source.GetResolution() != 0 {
			return 0, 0, false, false, fmt.Errorf("Query resolution '%v' is not an even multiple of table resolution '%v'", resolution, source.GetResolution())
		}
	}

	return resolution, strideSlice, resolutionChanged, resolutionTruncated, nil
}

func applySubQueryFilters(query *sql.Query, opts *Opts, source core.RowSource) (core.RowSource, error) {
	runSubQueries, subQueryPlanErr := planSubQueries(opts, query)
	if subQueryPlanErr != nil {
		return nil, subQueryPlanErr
	}

	hasRunSubqueries := int32(0)
	return core.RowFilter(source, query.WhereSQL, func(ctx context.Context, key bytemap.ByteMap, fields core.Fields, vals core.Vals) (bytemap.ByteMap, core.Vals, error) {
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
	}), nil
}
