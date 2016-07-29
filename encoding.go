package tdb

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/enc"
	"github.com/getlantern/tdb/sequence"
)

func readInt16(b []byte) (int, []byte) {
	i := enc.Binary.Uint16(b)
	return int(i), b[enc.Width16Bits:]
}

func writeInt16(b []byte, i int) []byte {
	enc.Binary.PutUint16(b, uint16(i))
	return b[enc.Width16Bits:]
}

func readInt64(b []byte) (int, []byte) {
	i := enc.Binary.Uint64(b)
	return int(i), b[enc.Width64Bits:]
}

func writeInt64(b []byte, i int) []byte {
	enc.Binary.PutUint64(b, uint64(i))
	return b[enc.Width64Bits:]
}

func readByteMap(b []byte, l int) (bytemap.ByteMap, []byte) {
	return bytemap.ByteMap(b[:l]), b[l:]
}

func readSequence(b []byte, l int) (sequence.Seq, []byte) {
	return sequence.Seq(b[:l]), b[l:]
}

func write(b []byte, d []byte) []byte {
	copy(b, d)
	return b[len(d):]
}
