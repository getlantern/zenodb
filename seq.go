package tdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

const (
	size64bits = 8
)

var (
	emptySequence = []byte{}
)

// sequence represents a time-ordered sequence of values in descending time
// order. The first 8 bytes are the timestamp at which the sequence starts, and
// after that each 8 bytes are a floating point value for the next interval in
// the sequence.
type sequence []byte

func (b *bucket) toSequence(resolution time.Duration) sequence {
	// Allocate a largish amount of space to avoid having to grow the buffer too
	// often.
	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	// Write the starting time of the sequence
	err := binary.Write(buf, binary.BigEndian, b.start.UnixNano())
	if err != nil {
		log.Errorf("Unable to encode start time to sequence: %v", err)
		return emptySequence
	}

	// Write all values
	for {
		err := binary.Write(buf, binary.BigEndian, b.val)
		if err != nil {
			log.Errorf("Unable to encode value to sequence: %v", err)
			return emptySequence
		}
		if b.prev == nil {
			break
		}

		// Fill gaps
		delta := int(b.start.Sub(b.prev.start)/resolution) - 1
		for i := 0; i < delta*size64bits; i++ {
			err := buf.WriteByte(0)
			if err != nil {
				log.Errorf("Unable to fill gaps in sequence: %v", err)
				return emptySequence
			}
		}

		// Continue with previous bucket
		b = b.prev
	}

	return sequence(buf.Bytes())
}

func (a sequence) append(b sequence, resolution time.Duration) sequence {
	as := a.start()
	bs := b.start()
	gap := int(as.Sub(bs)/resolution) - (len(a) / size64bits) + 1
	gapSize := gap * size64bits
	result := make(sequence, len(a)+len(b)+gapSize-size64bits)
	copy(result, a)
	copy(result[len(a)+gapSize:], b[size64bits:])
	return result
}

func (seq sequence) valueAt(t time.Time, resolution time.Duration) float64 {
	start := seq.start()
	if t.After(start) {
		return 0
	}
	bucket := int(start.Sub(t) / resolution)
	offset := (bucket + 1) * size64bits
	if offset >= len(seq) {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(seq[offset:]))
}

func (seq sequence) start() time.Time {
	ts := int64(binary.BigEndian.Uint64(seq))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}
