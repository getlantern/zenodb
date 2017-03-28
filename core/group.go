package core

import (
	"bytes"
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/encoding"
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

type GroupOpts struct {
	By         []GroupBy
	Fields     Fields
	Resolution time.Duration
	AsOf       time.Time
	Until      time.Time
}

func Group(source RowSource, opts GroupOpts) RowSource {
	return &group{
		rowTransform{source},
		opts,
	}
}

type group struct {
	rowTransform
	GroupOpts
}

func (g *group) GetGroupBy() []GroupBy {
	return g.GroupOpts.By
}

func (g *group) GetResolution() time.Duration {
	if g.Resolution == 0 {
		return g.source.GetResolution()
	}
	return g.Resolution
}

func (g *group) GetAsOf() time.Time {
	asOf := g.AsOf
	if asOf.IsZero() {
		asOf = g.source.GetAsOf()
	}
	until := g.GetUntil()
	resolution := g.GetResolution()
	if until.Sub(asOf) < resolution {
		// Make sure that we have at least one period in here
		asOf = until.Add(-1 * resolution)
	}
	return asOf
}

func (g *group) GetUntil() time.Time {
	if g.Until.IsZero() {
		return g.source.GetUntil()
	}
	return g.Until
}

func (g *group) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(g.By) == 0 {
		// Wildcard, select all and track all unique dims
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

	var bt *bytetree.Tree
	outFields := g.Fields
	var inFields Fields

	err := g.source.Iterate(ctx, func(fields Fields) {
		inFields = fields
		// Lazily initialize bytetree
		if len(outFields) == 0 {
			// default to input fields
			outFields = inFields
		}
		bt = bytetree.New(
			outFields.Exprs(),
			inFields.Exprs(),
			g.GetResolution(),
			g.source.GetResolution(),
			g.GetAsOf(),
			g.GetUntil(),
		)
		onFields(outFields)
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		metadata := key
		key = sliceKey(key)
		bt.Update(key, vals, nil, metadata)
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

func (g *group) String() string {
	result := &bytes.Buffer{}
	result.WriteString("group")
	if len(g.By) > 0 {
		result.WriteString(fmt.Sprintf("\n       by: %v", g.By))
	}
	if len(g.Fields) > 0 {
		result.WriteString(fmt.Sprintf("\n       fields: %v", g.Fields))
	}
	if g.Resolution > 0 {
		result.WriteString(fmt.Sprintf("\n       resolution: %v", g.Resolution))
	}
	if !g.AsOf.IsZero() {
		result.WriteString(fmt.Sprintf("\n       as of: %v", g.AsOf.In(time.UTC)))
	}
	if !g.Until.IsZero() {
		result.WriteString(fmt.Sprintf("\n       until: %v", g.Until.In(time.UTC)))
	}
	return result.String()
}
