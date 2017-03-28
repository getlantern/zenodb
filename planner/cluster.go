package planner

import (
	"context"
	"fmt"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"strings"
	"time"
)

const (
	backtick = "`"
)

type QueryClusterFN func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) error

type clusterSource struct {
	opts          *Opts
	query         *sql.Query
	planAsIfLocal core.Source
}

func (cs *clusterSource) doIterate(ctx context.Context, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	var subQueryResults [][]interface{}
	if cs.query.Where != nil {
		runSubQueries, subQueryPlanErr := planSubQueries(cs.opts, cs.query)
		if subQueryPlanErr != nil {
			return subQueryPlanErr
		}

		var subQueryErr error
		subQueryResults, subQueryErr = runSubQueries(ctx)
		if subQueryErr != nil {
			return subQueryErr
		}
	}

	return cs.opts.QueryCluster(ctx, cs.query.SQL, cs.opts.IsSubQuery, subQueryResults, unflat, onFields, onRow, onFlatRow)
}

func (cs *clusterSource) GetGroupBy() []core.GroupBy {
	return cs.planAsIfLocal.GetGroupBy()
}

func (cs *clusterSource) GetResolution() time.Duration {
	return cs.planAsIfLocal.GetResolution()
}

func (cs *clusterSource) GetAsOf() time.Time {
	return cs.planAsIfLocal.GetAsOf()
}

func (cs *clusterSource) GetUntil() time.Time {
	return cs.planAsIfLocal.GetUntil()
}

type clusterRowSource struct {
	clusterSource
}

func (cs *clusterRowSource) Iterate(ctx context.Context, onFields core.OnFields, onRow core.OnRow) error {
	return cs.doIterate(ctx, true, onFields, onRow, nil)
}

func (cs *clusterRowSource) String() string {
	return fmt.Sprintf("cluster %v", cs.query.SQL)
}

type clusterFlatRowSource struct {
	clusterSource
}

func (cs *clusterFlatRowSource) Iterate(ctx context.Context, onFields core.OnFields, onFlatRow core.OnFlatRow) error {
	var fields core.Fields

	return cs.doIterate(ctx, false, func(inFields core.Fields) {
		fields = inFields
		onFields(fields)
	}, nil, func(row *core.FlatRow) (bool, error) {
		row.SetFields(fields)
		return onFlatRow(row)
	})
}

func (cs *clusterFlatRowSource) String() string {
	return fmt.Sprintf("cluster flat %v", cs.query.SQL)
}

// pushdownAllowed checks whether we're allowed to push down a query to the
// individual partitions. "Push down" means that the entire query (including
// subquery) is run on each partition and the results are combined through a
// simple union on the leader node. If a query cannot be pushed down, the leader
// will query the partitions for the raw data and then perform group by and
// having logic on the leader. For queries that contains subqueries, if pushdown
// is not allowed, the entire subquery result set is returned to the leader for
// further processing, which is much slower than pushdown processing for queries
// that aggregate heavily.
func pushdownAllowed(opts *Opts, query *sql.Query) bool {
	if query.FromSubQuery != nil {
		if len(query.FromSubQuery.OrderBy) > 0 || query.FromSubQuery.Limit > 0 || query.FromSubQuery.Offset > 0 {
			// If subquery contains order by, limit or offset, we can't push down
			return false
		}
	}

	params := make(map[string]bool)
	for current := query; current != nil; current = current.FromSubQuery {
		if len(current.GroupBy) > 0 {
			for _, groupBy := range current.GroupBy {
				groupBy.Expr.WalkOneToOneParams(func(param string) {
					params[param] = true
				})
			}
		}
		if !current.GroupByAll {
			break
		}
		if current.FromSubQuery == nil {
			t := opts.GetTable(current.From, func(fields core.Fields) core.Fields {
				return fields
			})
			for _, groupBy := range t.GetGroupBy() {
				groupBy.Expr.WalkOneToOneParams(func(param string) {
					params[param] = true
				})
			}
		}
	}

	var partitionBy []string
	for current := query; current != nil; current = current.FromSubQuery {
		if current.FromSubQuery == nil {
			t := opts.GetTable(current.From, func(fields core.Fields) core.Fields {
				return fields
			})
			partitionBy = t.GetPartitionBy()
			break
		}
	}

	if len(partitionBy) == 0 {
		// Table not partitioned, can't push down
		return false
	}

	for _, partitionKey := range partitionBy {
		if !params[partitionKey] {
			// Partition key not represented, can't push down
			return false
		}
	}

	return query.FromSubQuery == nil || pushdownAllowed(opts, query.FromSubQuery)
}

func planClusterPushdown(opts *Opts, query *sql.Query) (core.FlatRowSource, error) {
	pail, err := planAsIfLocal(opts, query.SQL)
	if err != nil {
		return nil, err
	}

	flat := &clusterFlatRowSource{
		clusterSource{
			opts:          opts,
			query:         query,
			planAsIfLocal: pail,
		},
	}

	return addOrderLimitOffset(flat, query), nil
}

func planClusterNonPushdown(opts *Opts, query *sql.Query) (core.FlatRowSource, error) {
	// Remove having, order by and limit from query
	sqlString := query.SQL
	lowerSQL := strings.ToLower(sqlString)
	indexOfHaving := strings.Index(lowerSQL, "having ")
	indexOfOrderBy := strings.Index(lowerSQL, "order by ")
	indexOfLimit := strings.Index(lowerSQL, "limit ")
	if indexOfHaving > 0 {
		sqlString = sqlString[:indexOfHaving]
	} else if indexOfOrderBy > 0 {
		sqlString = sqlString[:indexOfOrderBy]
	} else if indexOfLimit > 0 {
		sqlString = sqlString[:indexOfLimit]
	}

	pail, err := planAsIfLocal(opts, sqlString)
	if err != nil {
		return nil, err
	}

	clusterQuery, parseErr := sql.Parse(sqlString, opts.FieldSource)
	if parseErr != nil {
		return nil, parseErr
	}
	fixupSubQuery(clusterQuery, opts)

	source := &clusterRowSource{
		clusterSource{
			opts:          opts,
			query:         clusterQuery,
			planAsIfLocal: core.UnflattenOptimized(pail),
		},
	}

	// Flatten group by to just params
	flattenedGroupBys := make([]core.GroupBy, 0, len(query.GroupBy))
	for _, groupBy := range query.GroupBy {
		flattenedGroupBys = append(flattenedGroupBys, core.NewGroupBy(groupBy.Name, goexpr.Param(groupBy.Name)))
	}
	if query.Resolution > pail.GetResolution() {
		query.Resolution = pail.GetResolution()
	}
	query.GroupBy = flattenedGroupBys
	var flat core.FlatRowSource = core.Flatten(addGroupBy(source, query))
	if query.Having != nil {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func planAsIfLocal(opts *Opts, sqlString string) (core.FlatRowSource, error) {
	unclusteredOpts := &Opts{}
	*unclusteredOpts = *opts
	unclusteredOpts.QueryCluster = nil

	query, parseErr := sql.Parse(sqlString, opts.FieldSource)
	if parseErr != nil {
		return nil, parseErr
	}

	return planLocal(query, unclusteredOpts)
}
