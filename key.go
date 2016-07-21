package tdb

import (
	"encoding/binary"

	"github.com/getlantern/bytemap"
)

func keyWithField(keyBytes []byte, field string) []byte {
	encodedFieldLen := 2 + len(field)
	b := make([]byte, len(keyBytes)+encodedFieldLen)
	copy(b, keyBytes)
	doEncodeField(b[len(keyBytes):], field)
	return b
}

func keyAndFieldFor(b []byte) (bytemap.ByteMap, string) {
	bl := len(b)
	fieldLen := int(binary.BigEndian.Uint16(b[bl-2:]))
	encodedFieldLen := 2 + fieldLen
	return bytemap.ByteMap(b[:bl-encodedFieldLen]), string(b[bl-encodedFieldLen : bl-2])
}

func fieldFor(b []byte) string {
	bl := len(b)
	fieldLen := int(binary.BigEndian.Uint16(b[bl-2:]))
	encodedFieldLen := 2 + fieldLen
	return string(b[bl-encodedFieldLen : bl-2])
}

func encodeField(field string) []byte {
	b := make([]byte, 2+len(field))
	doEncodeField(b, field)
	return b
}

func doEncodeField(b []byte, field string) {
	copy(b, field)
	fl := len(field)
	binary.BigEndian.PutUint16(b[fl:], uint16(fl))
}
