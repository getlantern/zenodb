package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"sync"
	"time"
)

var (
	// ErrDeadlineExceeded indicates that the deadline for iterating has been
	// exceeded. Results may be incomplete.
	ErrDeadlineExceeded = errors.New("deadline exceeded")

	reallyLongTime = 100 * 365 * 24 * time.Hour
)

// Field is a named expr.Expr
type Field struct {
	Expr expr.Expr
	Name string
}

// NewField is a convenience method for creating new Fields.
func NewField(name string, ex expr.Expr) Field {
	return Field{
		Expr: ex,
		Name: name,
	}
}

func (f Field) String() string {
	return fmt.Sprintf("%v (%v)", f.Name, f.Expr)
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

type Source interface {
	GetFields() Fields

	GetResolution() time.Duration

	GetAsOf() time.Time

	GetUntil() time.Time

	String() string
}

type OnRow func(key bytemap.ByteMap, vals Vals) (bool, error)

type RowSource interface {
	Source
	Iterate(ctx context.Context, onRow OnRow) error
}

type OnFlatRow func(flatRow *FlatRow) (bool, error)

type FlatRowSource interface {
	Source
	Iterate(ctx context.Context, onRow OnFlatRow) error
}

type Transform interface {
	GetSources() []Source
}

type RowToRow interface {
	RowSource
	Transform
	Connect(source RowSource)
}

type RowToFlat interface {
	FlatRowSource
	Transform
	Connect(source RowSource)
}

type FlatToFlat interface {
	FlatRowSource
	Transform
	Connect(source FlatRowSource)
}

type connectable struct {
	sources []Source
}

// TODO: Connectable assumes that the metadata for all sources is the same, we
// should add validation about this.
func (c *connectable) GetFields() Fields {
	return c.sources[0].GetFields()
}

func (c *connectable) GetResolution() time.Duration {
	return c.sources[0].GetResolution()
}

func (c *connectable) GetAsOf() time.Time {
	return c.sources[0].GetAsOf()
}

func (c *connectable) GetUntil() time.Time {
	return c.sources[0].GetUntil()
}

func (c *connectable) GetSources() []Source {
	return c.sources
}

type rowConnectable struct {
	connectable
}

func (c *rowConnectable) Connect(source RowSource) {
	c.sources = append(c.sources, source)
}

func (c *rowConnectable) iterateSerial(ctx context.Context, onRow OnRow) error {
	onRow = lockingOnRow(onRow)

	for _, source := range c.sources {
		err := source.(RowSource).Iterate(ctx, onRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *rowConnectable) iterateParallel(lock bool, ctx context.Context, onRow OnRow) error {
	if len(c.sources) == 1 {
		return c.iterateSerial(ctx, onRow)
	}

	if lock {
		onRow = lockingOnRow(onRow)
	}

	errors := make(chan error, len(c.sources))

	for _, s := range c.sources {
		source := s
		go func() {
			errors <- source.(RowSource).Iterate(ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
				return onRow(key, vals)
			})
		}()
	}

	// TODO: add timeout handling
	var finalErr error
	for range c.sources {
		err := <-errors
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}

type flatRowConnectable struct {
	connectable
}

func (c *flatRowConnectable) Connect(source FlatRowSource) {
	c.sources = append(c.sources, source)
}

func (c *flatRowConnectable) getSource(i int) Source {
	return c.sources[i]
}

func (c *flatRowConnectable) iterateSerial(ctx context.Context, onRow OnFlatRow) error {
	onRow = lockingOnFlatRow(onRow)

	for _, source := range c.sources {
		err := source.(FlatRowSource).Iterate(ctx, onRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *flatRowConnectable) iterateParallel(lock bool, ctx context.Context, onRow OnFlatRow) error {
	if len(c.sources) == 1 {
		return c.iterateSerial(ctx, onRow)
	}

	if lock {
		onRow = lockingOnFlatRow(onRow)
	}

	errors := make(chan error, len(c.sources))

	for _, s := range c.sources {
		source := s
		go func() {
			errors <- source.(FlatRowSource).Iterate(ctx, func(flatRow *FlatRow) (bool, error) {
				return onRow(flatRow)
			})
		}()
	}

	// TODO: add timeout handling
	var finalErr error
	for range c.sources {
		err := <-errors
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}

func lockingOnRow(onRow OnRow) OnRow {
	var mx sync.Mutex
	return func(key bytemap.ByteMap, vals Vals) (bool, error) {
		mx.Lock()
		more, err := onRow(key, vals)
		mx.Unlock()
		return more, err
	}
}

func lockingOnFlatRow(onRow OnFlatRow) OnFlatRow {
	var mx sync.Mutex
	return func(row *FlatRow) (bool, error) {
		mx.Lock()
		more, err := onRow(row)
		mx.Unlock()
		return more, err
	}
}

func proceed() (bool, error) {
	return true, nil
}

func stop() (bool, error) {
	return false, nil
}
