package pipeline

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"time"
)

type Group struct {
	Join
	Dims       []string
	Fields     Fields
	Resolution time.Duration
	AsOf       time.Time
	Until      time.Time
}

func (g *Group) Iterate(onRow OnRow) error {
	bt := bytetree.New(
		fieldsToExprs(g.Fields),
		fieldsToExprs(g.sources[0].Fields()), // todo: consider all sources
		g.Resolution,
		g.sources[0].Resolution(),
		g.AsOf,
		g.Until,
	)

	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(g.Dims) == 0 {
		// Wildcard, select all dims
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key
		}
	} else {
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(g.Dims...)
		}
	}

	err := g.iterateParallel(func(key bytemap.ByteMap, vals Vals) {
		metadata := key
		key = sliceKey(key)
		bt.Update(key, vals, metadata)
	})

	bt.Walk(0, func(key []byte, data []encoding.Sequence) bool {
		onRow(key, data)
		return true
	})

	return err
}

func fieldsToExprs(fields Fields) []expr.Expr {
	exprs := make([]expr.Expr, 0, len(fields))
	for _, field := range fields {
		exprs = append(exprs, field.Expr)
	}
	return exprs
}
