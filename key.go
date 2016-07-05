package tdb

import (
	"encoding/binary"

	"github.com/oxtoacart/bytemap"
)

func keyWithField(keyBytes []byte, field string) []byte {
	encodedFieldLen := 2 + len(field)
	b := make([]byte, len(keyBytes)+encodedFieldLen)
	doEncodeField(b, field)
	copy(b[encodedFieldLen:], keyBytes)
	return b
}

func fieldAndKey(b []byte) (string, bytemap.ByteMap) {
	fieldLen := int(binary.BigEndian.Uint16(b))
	encodedFieldLen := 2 + fieldLen
	field := string(b[2:encodedFieldLen])
	bm := bytemap.ByteMap(b[encodedFieldLen:])
	return field, bm
}

func encodeField(field string) []byte {
	b := make([]byte, 2+len(field))
	doEncodeField(b, field)
	return b
}

func doEncodeField(b []byte, field string) {
	binary.BigEndian.PutUint16(b, uint16(len(field)))
	copy(b[2:], field)
}
