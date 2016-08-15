// Package encoding handles encoding of zenodb data in binary form.
package encoding

import (
	"encoding/binary"

	"github.com/getlantern/bytemap"
)

const (
	Width16bits = 2
	Width64bits = 8
)

var (
	// Binary is the standard number encoding for zenodb
	Binary = binary.BigEndian
)

func readInt16(b []byte) (int, []byte) {
	i := Binary.Uint16(b)
	return int(i), b[Width16bits:]
}

func writeInt16(b []byte, i int) []byte {
	Binary.PutUint16(b, uint16(i))
	return b[Width16bits:]
}

func readInt64(b []byte) (int, []byte) {
	i := Binary.Uint64(b)
	return int(i), b[Width64bits:]
}

func writeInt64(b []byte, i int) []byte {
	Binary.PutUint64(b, uint64(i))
	return b[Width64bits:]
}

func readByteMap(b []byte, l int) (bytemap.ByteMap, []byte) {
	return bytemap.ByteMap(b[:l]), b[l:]
}

func readSequence(b []byte, l int) (Sequence, []byte) {
	return Sequence(b[:l]), b[l:]
}

func write(b []byte, d []byte) []byte {
	copy(b, d)
	return b[len(d):]
}
