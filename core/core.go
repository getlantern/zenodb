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

	mdmx sync.RWMutex
)

const (
	metadataKey = "_zenomd"
)

// Context creates a zenodb-specific context that can store metadata with SetMD
// and GetMD.
func Context() context.Context {
	return context.WithValue(context.Background(), metadataKey, make(map[string]interface{}))
}

// SetMD sets metadata in the given Context (thread-safe)
func SetMD(ctx context.Context, key string, value interface{}) {
	mdmx.Lock()
	GetMDMap(ctx)[key] = value
	mdmx.Unlock()
}

// GetMD gets metadata from the givne Context (thread-safe)
func GetMD(ctx context.Context, key string) interface{} {
	mdmx.RLock()
	defer mdmx.RUnlock()
	return GetMDMap(ctx)[key]
}

func GetMDMap(ctx context.Context) map[string]interface{} {
	return ctx.Value(metadataKey).(map[string]interface{})
}

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

func (fields Fields) Names() []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

func (fields Fields) Exprs() []expr.Expr {
	exprs := make([]expr.Expr, 0, len(fields))
	for _, field := range fields {
		exprs = append(exprs, field.Expr)
	}
	return exprs
}

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
	GetSource() Source
}

type rowTransform struct {
	source RowSource
}

func (t *rowTransform) GetFields() Fields {
	return t.source.GetFields()
}

func (t *rowTransform) GetResolution() time.Duration {
	return t.source.GetResolution()
}

func (t *rowTransform) GetAsOf() time.Time {
	return t.source.GetAsOf()
}

func (t *rowTransform) GetUntil() time.Time {
	return t.source.GetUntil()
}

func (t *rowTransform) GetSource() Source {
	return t.source
}

type flatRowTransform struct {
	source FlatRowSource
}

func (t *flatRowTransform) GetFields() Fields {
	return t.source.GetFields()
}

func (t *flatRowTransform) GetResolution() time.Duration {
	return t.source.GetResolution()
}

func (t *flatRowTransform) GetAsOf() time.Time {
	return t.source.GetAsOf()
}

func (t *flatRowTransform) GetUntil() time.Time {
	return t.source.GetUntil()
}

func (t *flatRowTransform) GetSource() Source {
	return t.source
}

func proceed() (bool, error) {
	return true, nil
}

func stop() (bool, error) {
	return false, nil
}
