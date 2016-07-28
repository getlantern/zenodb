package tdb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"

	"github.com/dustin/go-humanize"
	"github.com/golang/snappy"
	"github.com/jmcvetta/randutil"

	"testing"
)

const numPeriods = 365 * 24

func TestSnappyNumberCompression(t *testing.T) {
	for i := 0.01; i <= 1; i += 0.01 {
		buf := bytes.NewBuffer(make([]byte, 0, numPeriods*8))
		val := rand.Float64()
		for j := 0; j < numPeriods; j++ {
			delta := float64(rand.Intn(1000))
			if rand.Float64() > 0.5 {
				val += delta
			} else {
				val -= delta
			}
			if val > i {
				val = 0
			}
			binary.Write(buf, binary.BigEndian, val)
		}
		b := buf.Bytes()
		compressed := snappy.Encode(make([]byte, len(b)), b)
		log.Debugf("Fill Rate: %f\tUncompressed: %v\tCompressed: %v\tRatio: %f", i, humanize.Comma(int64(len(b))), humanize.Comma(int64(len(compressed))), float64(len(compressed))/float64(len(b)))
	}
}

func TestSnappyStringCompression(t *testing.T) {
	uniques := make([]string, 0, 10000)
	for j := 0; j < 10000; j++ {
		str, err := randutil.AlphaStringRange(15, 25)
		if err != nil {
			log.Fatal(err)
		}
		uniques = append(uniques, str)
	}
	all := make([]string, 0, 1000000)
	for j := 0; j < 1000000; j++ {
		all = append(all, uniques[rand.Intn(len(uniques))])
	}
	var b bytes.Buffer
	for _, str := range all {
		b.WriteString(str)
	}
	unsorted := b.Len()
	sort.Strings(all)
	var sb bytes.Buffer
	for _, str := range all {
		sb.WriteString(str)
	}
	sorted := sb.Len()
	compressed := snappy.Encode(make([]byte, b.Len()), b.Bytes())
	compressedSorted := snappy.Encode(make([]byte, sb.Len()), sb.Bytes())
	log.Debugf("Unsorted: %v   Sorted: %v   Unsorted Compressed: %v    Sorted Compressed: %v", humanize.Comma(int64(unsorted)), humanize.Comma(int64(sorted)), humanize.Comma(int64(len(compressed))), humanize.Comma(int64(len(compressedSorted))))
}
