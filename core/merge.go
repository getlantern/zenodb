package core

import (
	"context"
)

func MergeFlat() FlatToFlat {
	return &mergeFlat{}
}

type mergeFlat struct {
	flatRowConnectable
}

func (m *mergeFlat) Iterate(ctx context.Context, onRow OnFlatRow) error {
	return m.iterateParallel(false, ctx, func(row *FlatRow) (bool, error) {
		return onRow(row)
	})
}

func (m *mergeFlat) String() string {
	return "merge"
}
