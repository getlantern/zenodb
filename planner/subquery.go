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
	query.Where.WalkLists(func(list goexpr.List) {
		sq, ok := list.(*sql.SubQuery)
		if ok {
			subQueries = append(subQueries, sq)
		}
	})
	if len(opts.SubQueryResults) == len(subQueries) {
		for i, sq := range subQueries {
			sq.SetResult(opts.SubQueryResults[i])
		}
		return noopSubQueries, nil
	}

	var subQueryPlans []core.FlatRowSource
	sqOpts := &Opts{}
	*sqOpts = *opts
	sqOpts.IsSubQuery = true
	for _, sq := range subQueries {
		sqPlan, sqPlanErr := Plan(sq.SQL, sqOpts)
		if sqPlanErr != nil {
			return nil, sqPlanErr
		}
		subQueryPlans = append(subQueryPlans, sqPlan)
	}

	if len(subQueries) == 0 {
		return noopSubQueries, nil
	}

	return func(ctx context.Context) ([][]interface{}, error) {
		// Run subqueries in parallel
		// TODO: respect ctx deadline
		sqResultChs := make(chan chan *sqResult, len(subQueries))
		for _i, _sq := range subQueries {
			i := _i
			sq := _sq
			sqResultCh := make(chan *sqResult)
			sqResultChs <- sqResultCh
			go func() {
				var mx sync.Mutex
				uniques := make(map[interface{}]bool, 0)
				sqPlan := subQueryPlans[i]
				onRow := func(row *core.FlatRow) (bool, error) {
					dim := row.Key.Get(sq.Dim)
					mx.Lock()
					uniques[dim] = true
					mx.Unlock()
					return true, nil
				}
				err := sqPlan.Iterate(ctx, onRow)

				dims := make([]interface{}, 0, len(uniques))
				if err == nil || err == core.ErrDeadlineExceeded {
					for dim := range uniques {
						dims = append(dims, dim)
					}
					sq.SetResult(dims)
				}

				sqResultCh <- &sqResult{dims, err}
			}()
		}

		subQueryResults := make([][]interface{}, 0, len(subQueries))
		var finalErr error
		for i := 0; i < len(subQueries); i++ {
			sqResultCh := <-sqResultChs
			result := <-sqResultCh
			err := result.err
			if err != nil && (finalErr == nil || finalErr == core.ErrDeadlineExceeded) {
				finalErr = err
			}
			subQueryResults = append(subQueryResults, result.dims)
		}
		return subQueryResults, finalErr
	}, nil
}

type sqResult struct {
	dims []interface{}
	err  error
}

func noopSubQueries(ctx context.Context) ([][]interface{}, error) {
	return nil, nil
}
