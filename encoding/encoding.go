// Package encoding handles encoding of zenodb data in binary form.
package encoding

import (
	"encoding/binary"

	"github.com/getlantern/bytemap"
)

const (
	Width16bits = 2
	Width32bits = 4
	Width64bits = 8
)

var (
	// Binary is the standard number encoding for zenodb
	Binary = binary.BigEndian
)

func ReadInt16(b []byte) (int, []byte) {
	i := Binary.Uint16(b)
	return int(i), b[Width16bits:]
}

func WriteInt16(b []byte, i int) []byte {
	Binary.PutUint16(b, uint16(i))
	return b[Width16bits:]
}

func ReadInt32(b []byte) (int, []byte) {
	i := Binary.Uint32(b)
	return int(i), b[Width32bits:]
}

func WriteInt32(b []byte, i int) []byte {
	Binary.PutUint32(b, uint32(i))
	return b[Width32bits:]
}

func ReadInt64(b []byte) (int, []byte) {
	i := Binary.Uint64(b)
	return int(i), b[Width64bits:]
}

func WriteInt64(b []byte, i int) []byte {
	Binary.PutUint64(b, uint64(i))
	return b[Width64bits:]
}

func ReadByteMap(b []byte, l int) (bytemap.ByteMap, []byte) {
	return bytemap.ByteMap(b[:l]), b[l:]
}

func Read(b []byte, l int) ([]byte, []byte) {
	return b[:l], b[l:]
}

func ReadSequence(b []byte, l int) (Sequence, []byte) {
	s, r := Read(b, l)
	return Sequence(s), r
}

func Write(b []byte, d []byte) []byte {
	copy(b, d)
	return b[len(d):]
}
