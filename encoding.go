package tdb

import (
	"encoding/binary"

	"github.com/getlantern/bytemap"
)

const (
	width16bits = 2
	width64bits = 8
)

var (
	binaryEncoding = binary.BigEndian
)

func readInt16(b []byte) (int, []byte) {
	i := binaryEncoding.Uint16(b)
	return int(i), b[width16bits:]
}

func readInt64(b []byte) (int, []byte) {
	i := binaryEncoding.Uint64(b)
	return int(i), b[width64bits:]
}

func readByteMap(b []byte, l int) (bytemap.ByteMap, []byte) {
	return bytemap.ByteMap(b[:l]), b[l:]
}

func readSequence(b []byte, l int) (sequence, []byte) {
	return sequence(b[:l]), b[l:]
}
