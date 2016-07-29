package tdb

import (
	"math"

	"github.com/getlantern/tdb/enc"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sequence"
)

func BenchmarkSeq() {
	e := expr.SUM("i")
	sfp := &singleFieldParams{}
	seq := make(sequence.Seq, enc.Width64Bits+e.EncodedWidth())
	for i := 0; i < math.MaxInt64; i++ {
		sfp.field = "i"
		sfp.value = float64(i)
		seq.UpdateValueAt(0, e, sfp)
	}
}
