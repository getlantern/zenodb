package planner

import (
	"context"
	"fmt"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"strings"
	"time"
)

const (
	backtick = "`"
)

type clusterSource struct {
	opts          *Opts
	query         *sql.Query
	planAsIfLocal core.Source
}

func (cs *clusterSource) Iterate(ctx context.Context, onRow core.OnFlatRow) error {
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

	return cs.opts.QueryCluster(ctx, cs.query.SQL, subQueryResults, onRow)
}

func (cs *clusterSource) GetFields() core.Fields {
	return cs.planAsIfLocal.GetFields()
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

func (cs *clusterSource) String() string {
	return fmt.Sprintf("cluster %v", cs.query.SQL)
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
	if len(opts.PartitionKeys) == 0 {
		// With no partition keys, we can't push down
		return false
	}

	if query.FromSubQuery != nil {
		if len(query.FromSubQuery.OrderBy) > 0 || query.FromSubQuery.Limit > 0 || query.FromSubQuery.Offset > 0 {
			// If subquery contains order by, limit or offset, we can't push down
			return false
		}
	}

	// Find deepest query
	rootQuery := query
	for {
		if rootQuery.FromSubQuery == nil {
			break
		}
		rootQuery = rootQuery.FromSubQuery
	}

	if rootQuery.GroupByAll {
		return false
	}

	if len(rootQuery.GroupBy) == 0 {
		return false
	}

	fc := make(fieldCollector)
	for _, groupBy := range rootQuery.GroupBy {
		groupBy.Expr.Eval(fc)
	}

	partitionKeyRepresented := make([]bool, len(opts.PartitionKeys))
	for field := range fc {
		for i, partitionKey := range opts.PartitionKeys {
			if partitionKey == field {
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
				// Represnted partition keys are discontiguous, can't push down.
				return false
			}
			foundRepresented = true
		} else {
			foundNonRepresented = true
		}
	}

	return foundRepresented
}

// fieldCollector is an implementation of goexpr.Params that remembers what
// keys were requested, thereby allowing it to record what fields are used in
// one or more expressions.
type fieldCollector map[string]bool

func (fc fieldCollector) Get(param string) interface{} {
	fc[param] = true
	return nil
}

func planClusterPushdown(opts *Opts, query *sql.Query) (core.FlatRowSource, error) {
	pail, err := planAsIfLocal(opts, query.SQL)
	if err != nil {
		return nil, err
	}

	flat := &clusterSource{
		opts:          opts,
		query:         query,
		planAsIfLocal: pail,
	}

	return addOrderLimitOffset(flat, query), nil
}

func planClusterNonPushdown(opts *Opts, query *sql.Query) (core.FlatRowSource, error) {
	// Remove having, order by and limit from query
	sqlString := strings.ToLower(query.SQL)
	indexOfHaving := strings.Index(sqlString, "having ")
	indexOfOrderBy := strings.Index(sqlString, "order by ")
	indexOfLimit := strings.Index(sqlString, "limit ")
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

	unflat := core.Unflatten(&clusterSource{
		opts:          opts,
		query:         clusterQuery,
		planAsIfLocal: pail,
	}, query.Fields...)

	var flat core.FlatRowSource = core.Flatten(addGroupBy(unflat, query))
	if query.Having != nil {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func planAsIfLocal(opts *Opts, sqlString string) (core.FlatRowSource, error) {
	unclusteredOpts := &Opts{}
	*unclusteredOpts = *opts
	unclusteredOpts.QueryCluster = nil
	unclusteredOpts.PartitionKeys = nil

	query, parseErr := sql.Parse(sqlString, opts.FieldSource)
	if parseErr != nil {
		return nil, parseErr
	}

	return planLocal(query, unclusteredOpts)
}
