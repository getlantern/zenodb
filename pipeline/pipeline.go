package pipeline

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"sync"
	"time"
)

type Field struct {
	Expr expr.Expr
	Name string
}

type Fields []Field

type Vals []encoding.Sequence

type FlatRow struct {
	TS  int64
	Key bytemap.ByteMap
	// Values for each field
	Values []float64
	// For crosstab queries, this contains the total value for each field
	Totals []float64
	fields []Field
}

type OnRow func(key bytemap.ByteMap, vals Vals)

type OnFlatRow func(flatRow *FlatRow)

type Source interface {
	GetFields() Fields

	GetResolution() time.Duration

	GetAsOf() time.Time

	GetUntil() time.Time
}

type RowSource interface {
	Iterate(onRow OnRow) error
}

type FlatRowSource interface {
	Iterate(onRow OnFlatRow) error
}

type Join struct {
	sources []Source
}

// TODO: Join assumes that the metadata for all sources is the same, we should
// add validation about this.
func (j *Join) GetFields() Fields {
	return j.sources[0].GetFields()
}

func (j *Join) GetResolution() time.Duration {
	return j.sources[0].GetResolution()
}

func (j *Join) GetAsOf() time.Time {
	return j.sources[0].GetAsOf()
}

func (j *Join) GetUntil() time.Time {
	return j.sources[0].GetUntil()
}

func (j *Join) Connect(source Source) {
	j.sources = append(j.sources, source)
}

func (j *Join) iterateSerial(onRow OnRow) error {
	onRow = lockingOnRow(onRow)

	for _, source := range j.sources {
		err := source.(RowSource).Iterate(onRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *Join) iterateSerialFlat(onRow OnFlatRow) error {
	onRow = lockingOnFlatRow(onRow)

	for _, source := range j.sources {
		err := source.(FlatRowSource).Iterate(onRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *Join) iterateParallel(lock bool, onRow OnRow) error {
	if len(j.sources) == 1 {
		return j.iterateSerial(onRow)
	}

	if lock {
		onRow = lockingOnRow(onRow)
	}

	errors := make(chan error, len(j.sources))

	for _, s := range j.sources {
		source := s
		go func() {
			errors <- source.(RowSource).Iterate(func(key bytemap.ByteMap, vals Vals) {
				onRow(key, vals)
			})
		}()
	}

	// TODO: add timeout handling
	var finalErr error
	for range j.sources {
		err := <-errors
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}

func (j *Join) iterateParallelFlat(lock bool, onRow OnFlatRow) error {
	if len(j.sources) == 1 {
		return j.iterateSerialFlat(onRow)
	}

	if lock {
		onRow = lockingOnFlatRow(onRow)
	}

	errors := make(chan error, len(j.sources))

	for _, s := range j.sources {
		source := s
		go func() {
			errors <- source.(FlatRowSource).Iterate(func(flatRow *FlatRow) {
				onRow(flatRow)
			})
		}()
	}

	// TODO: add timeout handling
	var finalErr error
	for range j.sources {
		err := <-errors
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}

func lockingOnRow(onRow OnRow) OnRow {
	var mx sync.Mutex
	return func(key bytemap.ByteMap, vals Vals) {
		mx.Lock()
		onRow(key, vals)
		mx.Unlock()
	}
}

func lockingOnFlatRow(onRow OnFlatRow) OnFlatRow {
	var mx sync.Mutex
	return func(row *FlatRow) {
		mx.Lock()
		onRow(row)
		mx.Unlock()
	}
}
