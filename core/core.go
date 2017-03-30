package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"strings"
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

// FieldSource is a source of Fields based on some known Fields.
type FieldSource interface {
	Get(known Fields) (Fields, error)

	String() string
}

// PassthroughFieldSource simply passes through the known Fields.
var PassthroughFieldSource FieldSource = passthroughFieldSource{}

type passthroughFieldSource struct{}

func (pfs passthroughFieldSource) Get(known Fields) (Fields, error) {
	return known, nil
}

func (pfs passthroughFieldSource) String() string {
	return "passthrough"
}

// StaticFieldSource is a FieldSource that always returns the same Fields.
type StaticFieldSource Fields

func (sfs StaticFieldSource) Get(known Fields) (Fields, error) {
	return Fields(sfs), nil
}

func (sfs StaticFieldSource) String() string {
	return fmt.Sprint(Fields(sfs))
}

// CombinedFieldSource is a FieldSource that combines multiple FieldSources and
// ensures that a field is not repeated.
type CombinedFieldSource []FieldSource

func (cfs CombinedFieldSource) Get(known Fields) (Fields, error) {
	var combined Fields
	names := make(map[string]bool)
	for _, source := range cfs {
		fields, err := source.Get(known)
		if err != nil {
			return nil, err
		}
		for _, field := range fields {
			if !names[field.Name] {
				combined = append(combined, field)
				names[field.Name] = true
			}
		}
	}
	return combined, nil
}

func (cfs CombinedFieldSource) String() string {
	combined := make([]string, 0, len(cfs))
	for _, source := range cfs {
		combined = append(combined, source.String())
	}
	return strings.Join(combined, " && ")
}

// ExprFieldSource turns an ExprSource into a FieldSource with a single named
// field.
type ExprFieldSource struct {
	Name string
	Expr ExprSource
}

func (efs ExprFieldSource) Get(known Fields) (Fields, error) {
	ex, err := efs.Expr.Get(known)
	if err != nil {
		return nil, err
	}
	return Fields{NewField(efs.Name, ex)}, nil
}

func (efs ExprFieldSource) String() string {
	return fmt.Sprintf("%v as %v", efs.Expr, efs.Name)
}

// ExprSource is a source of an expression based on some known Fields.
type ExprSource interface {
	Get(known Fields) (expr.Expr, error)

	String() string
}

type Vals []encoding.Sequence

type FlatRow struct {
	TS  int64
	Key bytemap.ByteMap
	// Values for each field
	Values []float64
	fields Fields
}

func (row *FlatRow) SetFields(fields Fields) {
	row.fields = fields
}

type Source interface {
	GetGroupBy() []GroupBy

	GetResolution() time.Duration

	GetAsOf() time.Time

	GetUntil() time.Time

	String() string
}

type OnFields func(fields Fields) error

// FieldsIgnored is a placeholder for an OnFields that does nothing.
func FieldsIgnored(fields Fields) error {
	return nil
}

type OnRow func(key bytemap.ByteMap, vals Vals) (bool, error)

type RowSource interface {
	Source
	Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error
}

type OnFlatRow func(flatRow *FlatRow) (bool, error)

type FlatRowSource interface {
	Source
	Iterate(ctx context.Context, onFields OnFields, onRow OnFlatRow) error
}

type Transform interface {
	GetSource() Source
}

type rowTransform struct {
	source RowSource
}

func (t *rowTransform) GetGroupBy() []GroupBy {
	return t.source.GetGroupBy()
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

func (t *flatRowTransform) GetGroupBy() []GroupBy {
	return t.source.GetGroupBy()
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
