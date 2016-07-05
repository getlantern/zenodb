package tdb

import (
	"bytes"
	"fmt"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func keyWithField(keyBytes []byte, field string) ([]byte, error) {
	// Preallocate buffer to avoid having to grow
	buf := bytes.NewBuffer(make([]byte, len(keyBytes)+255))
	buf.Reset()
	enc := msgpack.NewEncoder(buf)
	err := enc.Encode(field)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode field: %v", err)
	}
	_, err = buf.Write(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("Unable to write keyBytes: %v", err)
	}
	return buf.Bytes(), nil
}
