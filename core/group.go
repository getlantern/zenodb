package core

import (
	"bytes"
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"time"
)

// GroupBy is a named goexpr.Expr.
type GroupBy struct {
	Expr goexpr.Expr
	Name string
}

// NewGroupBy is a convenience method for creating new GroupBys.
func NewGroupBy(name string, ex goexpr.Expr) GroupBy {
	return GroupBy{
		Expr: ex,
		Name: name,
	}
}

func (g GroupBy) String() string {
	return fmt.Sprintf("%v (%v)", g.Name, g.Expr)
}

type Group struct {
	rowConnectable
	By         []GroupBy
	Fields     Fields
	Resolution time.Duration
	AsOf       time.Time
	Until      time.Time
}

func (g *Group) GetFields() Fields {
	if len(g.Fields) == 0 {
		return g.rowConnectable.GetFields()
	}
	return g.Fields
}

func (g *Group) GetResolution() time.Duration {
	if g.Resolution == 0 {
		return g.rowConnectable.GetResolution()
	}
	return g.Resolution
}

func (g *Group) GetAsOf() time.Time {
	asOf := g.AsOf
	if asOf.IsZero() {
		asOf = g.rowConnectable.GetAsOf()
	}
	until := g.GetUntil()
	resolution := g.GetResolution()
	if until.Sub(asOf) < resolution {
		// Make sure that we have at least one period in here
		asOf = until.Add(-1 * resolution)
	}
	return asOf
}

func (g *Group) GetUntil() time.Time {
	if g.Until.IsZero() {
		return g.rowConnectable.GetUntil()
	}
	return g.Until
}

func (g *Group) Iterate(ctx context.Context, onRow OnRow) error {
	bt := bytetree.New(
		fieldsToExprs(g.GetFields()),
		fieldsToExprs(g.rowConnectable.GetFields()), // todo: consider all sources
		g.GetResolution(),
		g.rowConnectable.GetResolution(),
		g.GetAsOf(),
		g.GetUntil(),
	)

	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(g.By) == 0 {
		// Wildcard, select all
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key
		}
	} else {
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			names := make([]string, 0, len(g.By))
			values := make([]interface{}, 0, len(g.By))
			for _, groupBy := range g.By {
				val := groupBy.Expr.Eval(key)
				if val != nil {
					names = append(names, groupBy.Name)
					values = append(values, val)
				}
			}
			return bytemap.FromSortedKeysAndValues(names, values)
		}
	}

	err := g.iterateParallel(true, ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		metadata := key
		key = sliceKey(key)
		bt.Update(key, vals, metadata)
		return proceed()
	})

	var walkErr error
	if err != ErrDeadlineExceeded {
		deadline, hasDeadline := ctx.Deadline()
		walkErr = bt.Walk(0, func(key []byte, data []encoding.Sequence) (bool, bool, error) {
			more, iterErr := onRow(key, data)
			if iterErr == nil && hasDeadline && time.Now().After(deadline) {
				iterErr = ErrDeadlineExceeded
			}
			return more, true, iterErr
		})
	}

	if walkErr != nil {
		return walkErr
	}

	return err
}

func fieldsToExprs(fields Fields) []expr.Expr {
	exprs := make([]expr.Expr, 0, len(fields))
	for _, field := range fields {
		exprs = append(exprs, field.Expr)
	}
	return exprs
}

func (g *Group) String() string {
	result := &bytes.Buffer{}
	result.WriteString("group")
	if len(g.By) > 0 {
		result.WriteString(fmt.Sprintf("\n    by: %v", g.By))
	}
	if len(g.Fields) > 0 {
		result.WriteString(fmt.Sprintf("\n    fields: %v", g.Fields))
	}
	if g.Resolution > 0 {
		result.WriteString(fmt.Sprintf("\n    resolution: %v", g.Resolution))
	}
	if !g.AsOf.IsZero() {
		result.WriteString(fmt.Sprintf("\n    as of: %v", g.AsOf.In(time.UTC)))
	}
	if !g.Until.IsZero() {
		result.WriteString(fmt.Sprintf("\n    until: %v", g.Until.In(time.UTC)))
	}
	return result.String()
}
