package core

import (
	"context"
	"github.com/getlantern/bytemap"
	"sync/atomic"
	"time"
)

type Splitter interface {
	RowConnectable
	Split() RowSource
}

func NewSplitter() Splitter {
	return &splitter{
		onRows: make(chan OnRow),
		errors: make(chan error),
	}
}

type splitter struct {
	rowConnectable
	numSplits int
	onRows    chan OnRow
	errors    chan error
	started   int32
}

func (s *splitter) Split() RowSource {
	s.numSplits++
	return &split{s}
}

func (s *splitter) start(ctx context.Context, onRow OnRow) {
	if atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		go s.run(ctx)
	}
	s.onRows <- onRow
}

func (s *splitter) run(ctx context.Context) {
	// Collect all OnRow callbacks
	onRows := make([]OnRow, 0, s.numSplits)
	for i := 0; i < s.numSplits; i++ {
		onRows = append(onRows, <-s.onRows)
	}

	// Iterate
	iterErr := s.iterateParallel(false, ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		anyMore := false
		for i, onRow := range onRows {
			more, err := onRow(key, vals)
			if err != nil {
				// Fail on any error
				return false, err
			}
			if !more {
				// stop sending to this source
				onRows = append(onRows[:i], onRows[i+1:]...)
			} else {
				anyMore = true
			}
		}
		return anyMore, nil
	})

	// Return error to all splits
	for i := 0; i < s.numSplits; i++ {
		s.errors <- iterErr
	}
}

type split struct {
	splitter *splitter
}

func (s *split) Iterate(ctx context.Context, onRow OnRow) error {
	s.splitter.start(ctx, onRow)
	return <-s.splitter.errors
}

func (s *split) GetSources() []Source {
	return s.splitter.GetSources()
}

func (s *split) GetFields() Fields {
	return s.splitter.GetFields()
}

func (s *split) GetResolution() time.Duration {
	return s.splitter.GetResolution()
}

func (s *split) GetAsOf() time.Time {
	return s.splitter.GetAsOf()
}

func (s *split) GetUntil() time.Time {
	return s.splitter.GetUntil()
}

func (s *split) String() string {
	return "split"
}
