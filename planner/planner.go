// Package planner provides functionality for planning the execution of queries.
package planner

import (
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
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

func fixupSubQuery(query *sql.Query, opts *Opts) {
	if opts.IsSubQuery {
		// Change field to _points field
		query.Fields = core.StaticFieldSource{core.PointsField}
	}
}

func addGroupBy(source core.RowSource, query *sql.Query, applyResolution bool, resolution time.Duration, strideSlice time.Duration) core.RowSource {
	opts := core.GroupOpts{
		By:                    query.GroupBy,
		Crosstab:              query.Crosstab,
		CrosstabIncludesTotal: query.CrosstabIncludesTotal,
		Fields:                query.Fields,
		AsOf:                  query.AsOf,
		Until:                 query.Until,
		StrideSlice:           strideSlice,
	}
	if applyResolution {
		opts.Resolution = resolution
	}
	return core.Group(source, opts)
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
