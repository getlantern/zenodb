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

type QueryClusterFN func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onRow core.OnRow, onFlatRow core.OnFlatRow) error

type clusterSource struct {
	opts          *Opts
	query         *sql.Query
	planAsIfLocal core.Source
}

func (cs *clusterSource) doIterate(ctx context.Context, unflat bool, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
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

	return cs.opts.QueryCluster(ctx, cs.query.SQL, cs.opts.IsSubQuery, subQueryResults, unflat, onRow, onFlatRow)
}

func (cs *clusterSource) GetFields() core.Fields {
	return cs.planAsIfLocal.GetFields()
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

func (cs *clusterRowSource) Iterate(ctx context.Context, onRow core.OnRow) error {
	return cs.doIterate(ctx, true, onRow, nil)
}

func (cs *clusterRowSource) String() string {
	return fmt.Sprintf("cluster %v", cs.query.SQL)
}

type clusterFlatRowSource struct {
	clusterSource
}

func (cs *clusterFlatRowSource) Iterate(ctx context.Context, onFlatRow core.OnFlatRow) error {
	return cs.doIterate(ctx, false, nil, onFlatRow)
}

func (cs *clusterFlatRowSource) String() string {
	return fmt.Sprintf("cluster flat %v", cs.query.SQL)
}

// pushdownAllowed checks whether we're allowed to push down a query to the
// individual partitions. "Push down" means that the entire query (including
// subquery) is run on each partition and the results are combined through a
// simple union on the leader node. If a query cannot be pushed down, the leader
// will query the partitions for the raw data and then performing group by and
// having logic on the leader. For queries that contains subqueries, if pushdown
// is not allowed, the entire subquery result set is returned to the leader for
// further processing, which is much slower than pushdown processing for queries
// that aggregate heavily.
func pushdownAllowed(opts *Opts, query *sql.Query) bool {
	if len(opts.PartitionBy) == 0 {
		// With no partition keys, we can't push down
		return false
	}

	if query.FromSubQuery != nil {
		if len(query.FromSubQuery.OrderBy) > 0 || query.FromSubQuery.Limit > 0 || query.FromSubQuery.Offset > 0 {
			// If subquery contains order by, limit or offset, we can't push down
			return false
		}
	}

	for currentQuery := query; currentQuery != nil; currentQuery = currentQuery.FromSubQuery {
		maybeAllowed, groupBy := pushdownMaybeAllowed(opts, currentQuery)
		if !maybeAllowed {
			return false
		}
		if len(groupBy) == 0 {
			// If we're not grouping by anything in base table or query, we can't push
			// down
			return false
		}

		params := make(map[string]bool)
		for _, groupBy := range groupBy {
			groupBy.Expr.WalkOneToOneParams(func(param string) {
				params[param] = true
			})
		}

		partitionKeyRepresented := make([]bool, len(opts.PartitionBy))
		for param := range params {
			for i, partitionKey := range opts.PartitionBy {
				if partitionKey == param {
					partitionKeyRepresented[i] = true
					break
				}
			}
		}

		foundNonRepresented := false
		foundRepresented := false
		for _, represented := range partitionKeyRepresented {
			if represented {
				if foundNonRepresented {
					// Represented partition keys are discontiguous, can't push down.
					return false
				}
				foundRepresented = true
			} else {
				foundNonRepresented = true
			}
		}

		if !foundRepresented {
			return false
		}
	}

	return true
}

func pushdownMaybeAllowed(opts *Opts, query *sql.Query) (bool, []core.GroupBy) {
	if query.FromSubQuery != nil {
		if len(query.FromSubQuery.OrderBy) > 0 || query.FromSubQuery.Limit > 0 || query.FromSubQuery.Offset > 0 {
			// If subquery contains order by, limit or offset, we can't push down
			return false, nil
		}
	}
	if len(query.GroupBy) > 0 {
		return true, query.GroupBy
	}
	if query.FromSubQuery != nil {
		return pushdownMaybeAllowed(opts, query.FromSubQuery)
	}
	if query.GroupByAll {
		t := opts.GetTable(query.From, func(fields core.Fields) core.Fields {
			return fields
		})
		return true, t.GetGroupBy()
	}
	return false, nil
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
	unclusteredOpts.PartitionBy = nil

	query, parseErr := sql.Parse(sqlString, opts.FieldSource)
	if parseErr != nil {
		return nil, parseErr
	}

	return planLocal(query, unclusteredOpts)
}
