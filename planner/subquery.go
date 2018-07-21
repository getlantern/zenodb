package planner

import (
	"context"
	"fmt"
	"sync"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
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
				_, err := sqPlan.Iterate(ctx, core.FieldsIgnored, onRow)

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

// pointsAndHavingFieldSource is a FieldSource that wraps an existing
// FieldSource and returns only the _having field (if present) and the _points
// field.
type pointsAndHavingFieldSource struct {
	wrapped core.FieldSource
}

func (phfs pointsAndHavingFieldSource) Get(known core.Fields) (core.Fields, error) {
	var result core.Fields
	origFields, err := phfs.wrapped.Get(known)
	if err != nil {
		return result, err
	}
	result = append(result, core.PointsField)
	for _, field := range origFields {
		if field.Name == core.HavingFieldName {
			result = append(result, field)
		}
	}
	return result, nil
}

func (phfs pointsAndHavingFieldSource) String() string {
	return fmt.Sprintf("pointsAndHaving(%s)", phfs.wrapped)
}
