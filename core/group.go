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
	"sort"
	"strings"
	"time"
)

var (
	// ClusterCrosstab is the crosstab expression for crosstabs that come from an
	// contained Group By (i.e. from a cluster follower)
	ClusterCrosstab = goexpr.Param("_crosstab")
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

type sortedGroupBys []GroupBy

func (gbs sortedGroupBys) Len() int      { return len(gbs) }
func (gbs sortedGroupBys) Swap(i, j int) { gbs[i], gbs[j] = gbs[j], gbs[i] }
func (gbs sortedGroupBys) Less(i, j int) bool {
	return gbs[i].Name < gbs[j].Name
}

func (g GroupBy) String() string {
	return fmt.Sprintf("%v (%v)", g.Name, g.Expr)
}

type keyedVals struct {
	key  bytemap.ByteMap
	vals Vals
}

type GroupOpts struct {
	By          []GroupBy
	Crosstab    goexpr.Expr
	Fields      FieldSource
	Resolution  time.Duration
	AsOf        time.Time
	Until       time.Time
	StrideSlice time.Duration
}

func Group(source RowSource, opts GroupOpts) RowSource {
	sort.Sort(sortedGroupBys(opts.By))
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
	guard := Guard(ctx)

	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(g.By) == 0 {
		if g.Crosstab != nil && g.Crosstab.String() == ClusterCrosstab.String() {
			// Remove cluster crosstab expression
			sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
				_, nonCrosstab := key.Split("_crosstab")
				return nonCrosstab
			}
		} else if g.Crosstab != nil {
			// Don't group by anything
			sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
				return nil
			}
		} else {
			// Wildcard, select all and track all unique dims
			sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
				return key
			}
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
	var ctabs map[string]interface{}
	var kvs []*keyedVals
	var inFields Fields
	var outFields Fields
	if g.Fields == nil {
		g.Fields = PassthroughFieldSource
	}

	updateTree := func(key bytemap.ByteMap, vals Vals) {
		// Lazily initialize bytetree
		if bt == nil {
			bt = bytetree.New(
				outFields.Exprs(),
				inFields.Exprs(),
				g.GetResolution(),
				g.source.GetResolution(),
				g.GetAsOf(),
				g.GetUntil(),
				g.StrideSlice,
			)
		}
		metadata := key
		key = sliceKey(key)
		bt.Update(key, vals, nil, metadata)
	}

	err := g.source.Iterate(ctx, func(fields Fields) error {
		inFields = fields
		var err error
		outFields, err = g.Fields.Get(inFields)
		if err != nil {
			return err
		}
		return nil
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		if g.Crosstab != nil {
			if ctabs == nil {
				ctabs = make(map[string]interface{})
			}
			ctab := g.Crosstab.Eval(key).(string)
			ctabs[ctab] = nil
			kvs = append(kvs, &keyedVals{key, vals})
		} else {
			updateTree(key, vals)
		}
		return guard.Proceed()
	})

	var walkErr error
	if err != ErrDeadlineExceeded {
		if g.Crosstab != nil {
			origOutFields := outFields
			sortedCtabs := make([]string, 0, len(ctabs))
			for ctab := range ctabs {
				sortedCtabs = append(sortedCtabs, ctab)
			}
			sort.Strings(sortedCtabs)
			outFields = make([]Field, 0, (len(sortedCtabs)+1)*len(origOutFields))
			var havingField Field
			for _, ctab := range sortedCtabs {
				if guard.TimedOut() {
					return ErrDeadlineExceeded
				}
				for _, outField := range origOutFields {
					if outField.Name == "_having" {
						// _having is not subjected to CROSSTAB treatment, save it and
						// append later
						havingField = outField
						continue
					}
					cond, condErr := goexpr.Binary("=", g.Crosstab, goexpr.Constant(ctab))
					if condErr != nil {
						return condErr
					}
					ifex := expr.IF(cond, outField.Expr)
					outFields = append(outFields, NewField(fmt.Sprintf("%v_%v", strings.ToLower(ctab), outField.Name), ifex))
				}
			}
			for _, outField := range origOutFields {
				if outField.Name != "_having" {
					outFields = append(outFields, NewField(fmt.Sprintf("total_%v", outField.Name), outField.Expr))
				}
			}
			if havingField.Name != "" {
				outFields = append(outFields, havingField)
			}

			for _, kv := range kvs {
				if guard.TimedOut() {
					return ErrDeadlineExceeded
				}
				updateTree(kv.key, kv.vals)
			}
		}

		onFieldsErr := onFields(outFields)
		if onFieldsErr != nil {
			return onFieldsErr
		}

		if bt != nil {
			walkErr = bt.Walk(0, func(key []byte, data []encoding.Sequence) (bool, bool, error) {
				more, iterErr := onRow(key, data)
				if iterErr == nil && guard.TimedOut() {
					more = false
					iterErr = ErrDeadlineExceeded
				}
				return more, true, iterErr
			})
		}
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
	if g.Crosstab != nil {
		result.WriteString(fmt.Sprintf("\n       crosstab: %v", g.Crosstab))
	}
	if g.Fields != nil {
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
	if g.StrideSlice > 0 {
		result.WriteString(fmt.Sprintf("\n       stride slice: %v", g.StrideSlice))
	}
	return result.String()
}
