package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/getlantern/goexpr"
)

// PERCENTILE tracks estimated percentile values for the given expression
// assuming the given min and max possible values, to the given precision.
// Inputs are automatically bounded to the min/max using BOUNDED, such that
// values falling outside the range are discarded. Percentile is input in
// percent (e.g. 0-100). See https://godoc.org/github.com/codahale/hdrhistogram.
//
// It is possible to wrap an existing PERCENTILE with a new PERCENTILE to reuse
// the original PERCENTILE's storage but look at a different percentile. In this
// case, the min, max and precision parameters are ignored.
//
// WARNING - when PERCENTILEs that wrap existing PERCENTILEs are not stored and
// as such are only suitable for use in querying but not in tables or views
// unless those explicitly include the original PERCENTILE as well.
//
// WARNING - relative to other types of expressions, PERCENTILE types can be
// very large (e.g. on the order of Kilobytes vs 8 or 16 bytes for most other
// expressions) so it is best to keep these relatively low cardinality.
func PERCENTILE(value interface{}, percentile interface{}, min float64, max float64, precision int) Expr {
	valueExpr := exprFor(value)
	switch t := valueExpr.(type) {
	case *ptile:
		return newPtileOptimized(t, exprFor(percentile))
	case *ptileOptimized:
		return newPtileOptimized(t.wrapped, exprFor(percentile))
	default:
		sampleHisto := hdrhistogram.New(scaleToInt(min, precision), scaleToInt(max, precision), precision)
		numCounts := len(sampleHisto.Export().Counts)
		return &ptile{
			Value:      BOUNDED(valueExpr, min, max),
			Percentile: exprFor(percentile),
			Min:        scaleToInt(min, precision),
			Max:        scaleToInt(max, precision),
			Precision:  precision,
			Width:      (1+numCounts)*width64bits + valueExpr.EncodedWidth(),
		}
	}
}

// IsPercentile indicates whether the given expression is a percentile
// expression.
func IsPercentile(e Expr) bool {
	switch e.(type) {
	case *ptile:
		return true
	case *ptileOptimized:
		return true
	default:
		return false
	}
}

func scaleToInt(value float64, precision int) int64 {
	return int64(value * math.Pow10(precision))
}

func scaleFromInt(value int64, precision int) float64 {
	return scaleFromFloat(float64(value), precision)
}

func scaleFromFloat(value float64, precision int) float64 {
	return value / float64(math.Pow10(precision))
}

type ptile struct {
	Value      Expr
	Percentile Expr
	Min        int64
	Max        int64
	Precision  int
	Width      int
}

func (e *ptile) Validate() error {
	if e.Precision < 0 || e.Precision > 5 {
		return fmt.Errorf("Precision must be between 0 and 5 (inclusive), was %d", e.Precision)
	}
	err := validateWrappedInAggregate(e.Value)
	if err != nil {
		return err
	}
	if e.Percentile.EncodedWidth() > 0 {
		return fmt.Errorf("Percentile expression %v must be a constant or directly derived from a field", e.Percentile)
	}
	return nil
}

func (e *ptile) EncodedWidth() int {
	return e.Width
}

func (e *ptile) Shift() time.Duration {
	a := e.Value.Shift()
	b := e.Percentile.Shift()
	if a < b {
		return a
	}
	return b
}

func (e *ptile) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return e.doUpdate(b, params, metadata, e.Percentile)
}

func (e *ptile) doUpdate(b []byte, params Params, metadata goexpr.Params, percentileExpr Expr) ([]byte, float64, bool) {
	histo, _, remain := e.load(b)
	remain, value, updated := e.Value.Update(remain, params, metadata)
	remain, percentile, _ := percentileExpr.Update(remain, params, metadata)
	if updated {
		histo.RecordValue(scaleToInt(value, e.Precision))
		e.save(b, histo)
	}
	return remain, e.calc(histo, percentile), updated
}

func (e *ptile) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	histoX, xWasSet, remainX := e.load(x)
	histoY, yWasSet, remainY := e.load(y)
	if !xWasSet {
		if yWasSet {
			// Use valueY
			b = e.save(b, histoY)
		} else {
			// Nothing to save, just advance
			b = b[e.Width:]
		}
	} else {
		if yWasSet {
			histoX.Merge(histoY)
		}
		b = e.save(b, histoX)
	}
	return b, remainX, remainY
}

func (e *ptile) SubMergers(subs []Expr) []SubMerge {
	result := make([]SubMerge, 0, len(subs))
	for _, sub := range subs {
		var sm SubMerge
		if e.String() == sub.String() {
			sm = e.subMerge
		}
		result = append(result, sm)
	}
	return result
}

func (e *ptile) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
	e.Merge(data, data, other)
}

func (e *ptile) Get(b []byte) (float64, bool, []byte) {
	return e.doGet(b, e.Percentile)
}

func (e *ptile) doGet(b []byte, percentileExpr Expr) (float64, bool, []byte) {
	histo, wasSet, remain := e.load(b)
	percentile, _, remain := percentileExpr.Get(remain)
	if !wasSet {
		return 0, wasSet, remain
	}
	return e.calc(histo, percentile), wasSet, remain
}

func (e *ptile) calc(histo *hdrhistogram.Histogram, percentile float64) float64 {
	return scaleFromInt(histo.ValueAtQuantile(percentile), e.Precision)
}

func (e *ptile) load(b []byte) (*hdrhistogram.Histogram, bool, []byte) {
	remain := b[e.Width:]
	numCounts := int(binaryEncoding.Uint64(b))
	wasSet := numCounts > 0
	var histo *hdrhistogram.Histogram
	if !wasSet {
		histo = hdrhistogram.New(e.Min, e.Max, e.Precision)
	} else {
		counts := make([]int64, numCounts)
		for i := 0; i < numCounts; i++ {
			counts[i] = int64(binaryEncoding.Uint64(b[(i+1)*width64bits:]))
		}
		histo = hdrhistogram.Import(&hdrhistogram.Snapshot{
			LowestTrackableValue:  e.Min,
			HighestTrackableValue: e.Max,
			SignificantFigures:    int64(e.Precision),
			Counts:                counts,
		})
	}
	return histo, wasSet, remain
}

func (e *ptile) save(b []byte, histo *hdrhistogram.Histogram) []byte {
	s := histo.Export()
	numCounts := len(s.Counts)
	binaryEncoding.PutUint64(b, uint64(numCounts))
	for i := 0; i < numCounts; i++ {
		binaryEncoding.PutUint64(b[(i+1)*width64bits:], uint64(s.Counts[i]))
	}
	return b[e.Width:]
}

func (e *ptile) IsConstant() bool {
	return e.Value.IsConstant()
}

func (e *ptile) String() string {
	return fmt.Sprintf("PERCENTILE(%v, %v, %v, %v, %v)", e.Value, e.Percentile, e.Min, e.Max, e.Precision)
}
