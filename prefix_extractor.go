package tdb

import (
	"encoding/binary"
)

// fieldPrefixExtractor is a SliceTransform that extracts the field part of the key.
type fieldPrefixExtractor struct{}

func (pe *fieldPrefixExtractor) Transform(src []byte) []byte {
	fieldLen := int(binary.BigEndian.Uint16(src))
	encodedFieldLen := 2 + fieldLen
	return src[:encodedFieldLen]
}

func (pe *fieldPrefixExtractor) InDomain(src []byte) bool {
	return len(src) > 3
}

func (pe *fieldPrefixExtractor) InRange(src []byte) bool {
	return len(src) > 0
}

func (pe *fieldPrefixExtractor) Name() string {
	return "field"
}
