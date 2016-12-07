package planner

import (
	"context"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"sync"
)

func planSubQueries(query *sql.Query, opts *Opts) (func(ctx context.Context) error, error) {
	var subQueries []*sql.SubQuery
	var subQueryPlans []core.FlatRowSource
	query.Where.WalkLists(func(list goexpr.List) {
		sq, ok := list.(*sql.SubQuery)
		if ok {
			subQueries = append(subQueries, sq)
		}
	})
	for _, sq := range subQueries {
		sqPlan, sqPlanErr := Plan(sq.SQL, opts)
		if sqPlanErr != nil {
			return nil, sqPlanErr
		}
		subQueryPlans = append(subQueryPlans, sqPlan)
	}

	if len(subQueries) == 0 {
		return func(ctx context.Context) error {
			return nil
		}, nil
	}

	return func(ctx context.Context) error {
		// Run subqueries in parallel
		// TODO: respect ctx deadline
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

				if err == nil || err == core.ErrDeadlineExceeded {
					dims := make([]interface{}, 0, len(uniques))
					for _, dim := range uniques {
						dims = append(dims, dim)
					}
					sq.SetResult(dims)
				}

				errors <- err
			}()
		}

		var finalErr error
		for i := 0; i < len(subQueries); i++ {
			err := <-errors
			if err != nil && (finalErr == nil || finalErr == core.ErrDeadlineExceeded) {
				finalErr = err
			}
		}
		return finalErr
	}, nil
}
