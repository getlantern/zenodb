package core

import (
	"context"
	"fmt"
	"sort"
)

// OrderBy specifies an element by whith to order (element being ither a field
// name or the name of a dimension in the row key).
type OrderBy struct {
	Field      string
	Descending bool
}

func (o OrderBy) String() string {
	ascending := "asc"
	if o.Descending {
		ascending = "desc"
	}
	return fmt.Sprintf("%v(%v)", o.Field, ascending)
}

func NewOrderBy(field string, descending bool) OrderBy {
	return OrderBy{
		Field:      field,
		Descending: descending,
	}
}

// Get implements the interface method from goexpr.Params
func (row *FlatRow) Get(param string) interface{} {
	// First look at values
	for i, field := range row.fields {
		if field.Name == param {
			return row.Values[i]
		}
	}

	// Then look at key
	return row.Key.Get(param)
}

func Sort(source FlatRowSource, by ...OrderBy) FlatRowSource {
	return &sorter{
		flatRowTransform{source},
		by,
	}
}

type sorter struct {
	flatRowTransform
	by []OrderBy
}

func (s *sorter) Iterate(ctx context.Context, onFields OnFields, onRow OnFlatRow) (interface{}, error) {
	guard := Guard(ctx)

	rows := orderedRows{
		orderBy: s.by,
	}

	metadata, err := s.source.Iterate(ctx, onFields, func(row *FlatRow) (bool, error) {
		rows.rows = append(rows.rows, row)
		return guard.Proceed()
	})

	if err != ErrDeadlineExceeded {
		sort.Sort(rows)
		for _, row := range rows.rows {
			if guard.TimedOut() {
				return metadata, ErrDeadlineExceeded
			}

			more, onRowErr := onRow(row)
			if onRowErr != nil {
				return metadata, onRowErr
			}
			if !more {
				break
			}
		}
	}
	return metadata, err
}

func (s *sorter) String() string {
	return fmt.Sprintf("order by %v", s.by)
}

type orderedRows struct {
	orderBy []OrderBy
	rows    []*FlatRow
}

func (r orderedRows) Len() int      { return len(r.rows) }
func (r orderedRows) Swap(i, j int) { r.rows[i], r.rows[j] = r.rows[j], r.rows[i] }
func (r orderedRows) Less(i, j int) bool {
	a := r.rows[i]
	b := r.rows[j]
	for _, order := range r.orderBy {
		// _time is a special case
		if order.Field == "_time" {
			ta := a.TS
			tb := b.TS
			if order.Descending {
				ta, tb = tb, ta
			}
			if ta < tb {
				return true
			}
			continue
		}

		// sort by field or dim
		va := a.Get(order.Field)
		vb := b.Get(order.Field)
		if order.Descending {
			va, vb = vb, va
		}
		result := compare(va, vb)
		if result < 0 {
			return true
		}
		if result > 0 {
			return false
		}
	}
	return false
}
