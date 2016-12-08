package planner

import (
	"context"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"sync"
)

func planSubQueries(opts *Opts, query *sql.Query) (func(ctx context.Context) ([][]interface{}, error), error) {
	var subQueries []*sql.SubQuery
	var subQueryPlans []core.FlatRowSource
	query.Where.WalkLists(func(list goexpr.List) {
		sq, ok := list.(*sql.SubQuery)
		if ok {
			subQueries = append(subQueries, sq)
		}
	})
	sqOpts := &Opts{}
	*sqOpts = *opts
	sqOpts.IsSubquery = true
	for _, sq := range subQueries {
		sqPlan, sqPlanErr := Plan(sqOpts, sq.SQL)
		if sqPlanErr != nil {
			return nil, sqPlanErr
		}
		subQueryPlans = append(subQueryPlans, sqPlan)
	}

	if len(subQueries) == 0 {
		return func(ctx context.Context) ([][]interface{}, error) {
			return nil, nil
		}, nil
	}

	return func(ctx context.Context) ([][]interface{}, error) {
		// Run subqueries in parallel
		// TODO: respect ctx deadline
		subQueryResultsChs := make(chan []interface{}, len(subQueries))
		errors := make(chan error, len(subQueries))
		for _i, _sq := range subQueries {
			i := _i
			sq := _sq
			go func() {
				var mx sync.Mutex
				uniques := make(map[interface{}]bool, 0)
				sqPlan := subQueryPlans[i]
				err := sqPlan.Iterate(ctx, func(row *core.FlatRow) (bool, error) {
					dim := row.Key.Get(sq.Dim)
					mx.Lock()
					uniques[dim] = true
					mx.Unlock()
					return true, nil
				})

				dims := make([]interface{}, 0, len(uniques))
				if err == nil || err == core.ErrDeadlineExceeded {
					for dim := range uniques {
						dims = append(dims, dim)
					}
					sq.SetResult(dims)
				}

				subQueryResultsChs <- dims
				errors <- err
			}()
		}

		subQueryResults := make([][]interface{}, 0, len(subQueries))
		var finalErr error
		for i := 0; i < len(subQueries); i++ {
			subQueryResults = append(subQueryResults, <-subQueryResultsChs)
			err := <-errors
			if err != nil && (finalErr == nil || finalErr == core.ErrDeadlineExceeded) {
				finalErr = err
			}
		}
		return subQueryResults, finalErr
	}, nil
}
