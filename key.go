package tdb

import (
	"bytes"
	"fmt"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func keyToBytes(key map[string]interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode dims: %v", err)
	}
	return buf.Bytes(), nil
}

func keyFromBytes(keyBytes []byte) (map[string]interface{}, error) {
	key := make(map[string]interface{}, 0)
	err := msgpack.Unmarshal(keyBytes, &key)
	if err != nil {
		return nil, fmt.Errorf("Unable to decode dims: %v", err)
	}
	return key, nil
}

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
