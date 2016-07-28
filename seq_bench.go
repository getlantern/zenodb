package tdb

import (
	"github.com/getlantern/tdb/expr"
	"math"
)

func BenchmarkSeq() {
	e := expr.SUM("i")
	sfp := &singleFieldParams{}
	seq := make(sequence, width64bits+e.EncodedWidth())
	for i := 0; i < math.MaxInt64; i++ {
		sfp.field = "i"
		sfp.value = float64(i)
		seq.updateValueAt(0, e, sfp)
	}
}
