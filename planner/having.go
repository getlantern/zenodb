package planner

import (
	"context"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
	"time"
)

func addHaving(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	havingIdx := len(query.Fields)
	base := core.FlatRowFilter(flat, query.HavingSQL, func(ctx context.Context, row *core.FlatRow) (*core.FlatRow, error) {
		include := row.Values[havingIdx]
		if include == 1 {
			// Removing having field
			row.Values = row.Values[:havingIdx]
			return row, nil
		}
		return nil, nil
	})
	return &havingFilter{base}
}

type havingFilter struct {
	base core.FlatRowSource
}

func (f *havingFilter) Iterate(ctx context.Context, onRow core.OnFlatRow) error {
	return f.base.Iterate(ctx, onRow)
}

func (f *havingFilter) GetFields() core.Fields {
	// Remove the trailing "_having" field
	fields := f.base.GetFields()
	return fields[:len(fields)-1]
}

func (f *havingFilter) GetResolution() time.Duration {
	return f.base.GetResolution()
}

func (f *havingFilter) GetAsOf() time.Time {
	return f.base.GetAsOf()
}

func (f *havingFilter) GetUntil() time.Time {
	return f.base.GetUntil()
}

func (f *havingFilter) GetSource() core.Source {
	switch t := f.base.(type) {
	case core.Transform:
		return t.GetSource()
	}
	return nil
}

func (f *havingFilter) String() string {
	return f.base.String()
}
