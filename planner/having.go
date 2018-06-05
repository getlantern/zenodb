package planner

import (
	"context"
	"time"

	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
)

func addHaving(flat core.FlatRowSource, query *sql.Query) core.FlatRowSource {
	base := core.FlatRowFilter(flat, core.HavingFieldName, func(ctx context.Context, row *core.FlatRow, fields core.Fields) (*core.FlatRow, error) {
		havingIdx := len(fields) - 1
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

func (f *havingFilter) Iterate(ctx context.Context, onFields core.OnFields, onRow core.OnFlatRow) (interface{}, error) {
	return f.base.Iterate(ctx, func(fields core.Fields) error {
		// Remove the "_having" field
		cleanedFields := make(core.Fields, 0, len(fields))
		for _, field := range fields {
			if field.Name != core.HavingFieldName {
				cleanedFields = append(cleanedFields, field)
			}
		}
		return onFields(cleanedFields)
	}, onRow)
}

func (f *havingFilter) GetGroupBy() []core.GroupBy {
	return f.base.GetGroupBy()
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
