package tdb

import (
	"encoding/binary"

	"github.com/getlantern/bytemap"
)

func keyWithField(keyBytes []byte, field string) []byte {
	encodedFieldLen := 2 + len(field)
	b := make([]byte, len(keyBytes)+encodedFieldLen)
	doEncodeField(b, field)
	copy(b[encodedFieldLen:], keyBytes)
	return b
}

func keyFor(b []byte) bytemap.ByteMap {
	fieldLen := int(binary.BigEndian.Uint16(b))
	encodedFieldLen := 2 + fieldLen
	return bytemap.ByteMap(b[encodedFieldLen:])
}

func fieldFor(b []byte) string {
	fieldLen := int(binary.BigEndian.Uint16(b))
	encodedFieldLen := 2 + fieldLen
	return string(b[2:encodedFieldLen])
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
