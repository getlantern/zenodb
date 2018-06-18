package planner

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
)

const (
	backtick = "`"
)

type QueryClusterFN func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) (interface{}, error)

type clusterSource struct {
	opts          *Opts
	query         *sql.Query
	planAsIfLocal core.Source
}

func (cs *clusterSource) doIterate(ctx context.Context, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) (interface{}, error) {
	var subQueryResults [][]interface{}
	if cs.query.Where != nil {
		runSubQueries, subQueryPlanErr := planSubQueries(cs.opts, cs.query)
		if subQueryPlanErr != nil {
			return nil, subQueryPlanErr
		}

		var subQueryErr error
		subQueryResults, subQueryErr = runSubQueries(ctx)
		if subQueryErr != nil {
			return nil, subQueryErr
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

func (cs *clusterRowSource) Iterate(ctx context.Context, onFields core.OnFields, onRow core.OnRow) (interface{}, error) {
	return cs.doIterate(ctx, true, onFields, onRow, nil)
}

func (cs *clusterRowSource) String() string {
	return fmt.Sprintf("cluster %v", cs.query.SQL)
}

type clusterFlatRowSource struct {
	clusterSource
}

func (cs *clusterFlatRowSource) Iterate(ctx context.Context, onFields core.OnFields, onFlatRow core.OnFlatRow) (interface{}, error) {
	return cs.doIterate(ctx, false, onFields, nil, func(row *core.FlatRow) (bool, error) {
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
func pushdownAllowed(opts *Opts, query *sql.Query) (bool, error) {
	if query.Crosstab != nil {
		log.Debug("Pushdown not allowed because query contains crosstab")
		return false, nil
	}

	if query.FromSubQuery != nil {
		if len(query.FromSubQuery.OrderBy) > 0 || query.FromSubQuery.Crosstab != nil || query.FromSubQuery.Limit > 0 || query.FromSubQuery.Offset > 0 {
			// If subquery contains order by, crosstab, limit or offset, we can't push down
			log.Debugf("Pushdown not allowed because subquery contains disallowed clause: %v", query.FromSubQuery.SQL)
			return false, nil
		}
	}

	parentGroupByAll := true
	parentGroupParams := make(map[string]bool)
	for current := query; current != nil; current = current.FromSubQuery {
		if current.FromSubQuery == nil {
			// we've reached the bottom
			t, err := opts.GetTable(current.From, func(tableFields core.Fields) (core.Fields, error) {
				return tableFields, nil
			})
			if err != nil {
				log.Debugf("Unexpected error checking if pushdown allowed: %v", err)
				return false, err
			}
			if current.GroupByAll && parentGroupByAll {
				log.Debug("Pushdown allowed because we're grouping by all")
			} else {
				partitionBy := t.GetPartitionBy()
				if len(partitionBy) == 0 {
					// Table not partitioned, can't push down
					log.Debug("Pushdown not allowed because table is not partitioned")
					return false, nil
				}

				var groupParams map[string]bool
				if current.GroupByAll {
					groupParams = parentGroupParams
				} else {
					groupParams = make(map[string]bool)
					for _, groupBy := range current.GroupBy {
						groupBy.Expr.WalkOneToOneParams(func(param string) {
							if parentGroupByAll || parentGroupParams[groupBy.Name] {
								groupParams[param] = true
							}
						})
					}
				}
				for _, partitionKey := range partitionBy {
					if !groupParams[partitionKey] {
						log.Debugf("Pushdown not allowed because partition key %v is not represented in group by params %v", partitionKey, groupParams)
						// Partition key not represented, can't push down
						return false, nil
					}
				}
			}
			log.Debugf("Pushdown allowed")
			return true, nil
		}

		if !current.GroupByAll {
			groupParams := make(map[string]bool)
			for _, groupBy := range current.GroupBy {
				groupBy.Expr.WalkOneToOneParams(func(param string) {
					if parentGroupByAll || parentGroupParams[groupBy.Name] {
						groupParams[param] = true
					}
				})
			}
			parentGroupByAll = false
			parentGroupParams = groupParams
		}
	}

	return false, fmt.Errorf("Should never reach this branch of pushdownAllowed")
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
	// Remove group by, having, order by and limit from query
	sqlString := query.SQL
	crosstabString := concatForCrosstab(sqlString)
	lowerSQL := strings.ToLower(sqlString)
	indexOfGroupBy := strings.Index(lowerSQL, "group by ")
	indexOfHaving := strings.Index(lowerSQL, "having ")
	indexOfOrderBy := strings.Index(lowerSQL, "order by ")
	indexOfLimit := strings.Index(lowerSQL, "limit ")
	if indexOfGroupBy > 0 {
		sqlString = sqlString[:indexOfGroupBy]
	} else if indexOfHaving > 0 {
		sqlString = sqlString[:indexOfHaving]
	} else if indexOfOrderBy > 0 {
		sqlString = sqlString[:indexOfOrderBy]
	} else if indexOfLimit > 0 {
		sqlString = sqlString[:indexOfLimit]
	}

	if query.HasHaving {
		// Insert having field
		fromRegex, err := regexp.Compile(fmt.Sprintf("from\\s+%v", strings.ToLower(query.FromSQL)))
		if err != nil {
			return nil, fmt.Errorf("Unable to compile from regex: %v", err)
		}
		fromIndexes := fromRegex.FindStringIndex(lowerSQL)
		if len(fromIndexes) == 0 {
			return nil, fmt.Errorf("FROM clause not found!")
		}
		indexOfFrom := fromIndexes[0]
		sqlString = fmt.Sprintf("%v, %v %v", sqlString[:indexOfFrom], query.HavingSQL, sqlString[indexOfFrom:])
	}

	hasGroupBy := len(query.GroupBy) > 0
	hasCrosstab := crosstabString != ""
	var groupByParts []string
	if query.GroupByAll {
		if hasGroupBy || hasCrosstab {
			// need an explicit wildcard
			groupByParts = append(groupByParts, "*")
		}
	}
	if hasGroupBy {
		groupByParams := make(map[string]bool)
		for _, groupBy := range query.GroupBy {
			groupBy.Expr.WalkParams(func(name string) {
				groupByParams[name] = true
			})
		}
		sortedNames := make([]string, 0, len(groupByParams))
		for name := range groupByParams {
			sortedNames = append(sortedNames, name)
		}
		sort.Strings(sortedNames)
		for _, name := range sortedNames {
			groupByParts = append(groupByParts, name)
		}
	}
	if hasCrosstab {
		groupByParts = append(groupByParts, crosstabString)
		query.Crosstab = core.ClusterCrosstab
	}
	if query.Resolution != 0 {
		groupByParts = append(groupByParts, fmt.Sprintf("period(%v)", query.Resolution))
	}
	if query.Stride > 0 {
		groupByParts = append(groupByParts, fmt.Sprintf("stride(%v)", query.Stride))
	}
	if len(groupByParts) > 0 {
		sqlString = fmt.Sprintf("%v group by %v", sqlString, strings.Join(groupByParts, ", "))
	}

	pail, err := planAsIfLocal(opts, sqlString)
	if err != nil {
		return nil, fmt.Errorf("Unable to plan non-pushdown query: %v", err)
	}

	clusterQuery, parseErr := sql.Parse(sqlString)
	if parseErr != nil {
		return nil, parseErr
	}
	fixupSubQuery(clusterQuery, opts)

	var source core.RowSource = &clusterRowSource{
		clusterSource{
			opts:          opts,
			query:         clusterQuery,
			planAsIfLocal: core.UnflattenOptimized(pail),
		},
	}

	if query.Resolution > pail.GetResolution() {
		query.Resolution = pail.GetResolution()
	}
	// Pass through fields since the remote query already has the correct ones
	query.Fields = core.PassthroughFieldSource
	// Pass through asOf, until and resolution since the remote query already has
	// those
	query.AsOf = time.Time{}
	query.Until = time.Time{}
	query.Resolution = 0

	flat := core.Flatten(addGroupBy(source, query, true, query.Resolution, 0))
	if query.HasHaving {
		flat = addHaving(flat, query)
	}

	return addOrderLimitOffset(flat, query), nil
}

func planAsIfLocal(opts *Opts, sqlString string) (core.FlatRowSource, error) {
	unclusteredOpts := &Opts{}
	*unclusteredOpts = *opts
	unclusteredOpts.QueryCluster = nil

	query, parseErr := sql.Parse(sqlString)
	if parseErr != nil {
		return nil, parseErr
	}

	return planLocal(query, unclusteredOpts)
}

func concatForCrosstab(sql string) string {
	crosstab := "CROSSTABT"
	idx := strings.Index(strings.ToUpper(sql), crosstab)
	if idx < 0 {
		crosstab = "CROSSTAB"
		idx = strings.Index(strings.ToUpper(sql), crosstab)
	}
	if idx < 0 {
		return ""
	}
	var out []byte
	out = append(out, []byte("concat('_', ")...)
	idx += len(crosstab + "(")
	level := 1
parseLoop:
	for ; idx < len(sql); idx++ {
		c := sql[idx]
		switch c {
		case '(':
			level++
			out = append(out, c)
			fmt.Printf("Descend at %v\n", string(out))
		case ')':
			level--
			out = append(out, c)
			fmt.Printf("Ascend at %v\n", string(out))
			if level == 0 {
				break parseLoop
			}
		default:
			out = append(out, c)
		}
	}
	out = append(out, []byte(" as _crosstab")...)
	return string(out)
}
