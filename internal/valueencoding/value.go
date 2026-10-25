// Package valueencoding owns private deterministic identity and routing encodings.
package valueencoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

// ScalarBytes is the deterministic, source-neutral serialization used when a
// scalar must participate in an opaque identity. It is not the in-memory
// datetime representation or a public wire format.
func ScalarBytes(value storage.Value) ([]byte, error) {
	if value.State != storage.ValuePresent || value.Type == storage.TypeList || value.Type == storage.TypeObject {
		return nil, errors.New("scalar serialization requires a present scalar value")
	}
	if err := value.ValidateComplete(); err != nil {
		return nil, err
	}
	if value.Type == storage.TypeDateTime {
		return value.DateTime.MarshalBinary()
	}
	return append([]byte(nil), value.Data...), nil
}

// ParseScalarBytes reverses ScalarBytes. It is used for opaque engine
// identities and is deliberately separate from textual query parsing.
func ParseScalarBytes(valueType storage.ValueType, data []byte) (storage.Value, error) {
	if valueType != storage.TypeDateTime {
		return storage.ParseScalar(valueType, data)
	}
	var instant time.Time
	if err := instant.UnmarshalBinary(data); err != nil {
		return storage.Value{}, fmt.Errorf("decode datetime identity: %w", err)
	}
	return storage.NewDateTime(instant), nil
}

// CanonicalBytes returns a deterministic encoding suitable for hashing,
// routing, identity, and equality. It is not a public wire encoding.
func CanonicalBytes(value storage.Value) []byte {
	var encoded bytes.Buffer
	encodeValue(&encoded, value)
	return encoded.Bytes()
}

func encodeValue(dst *bytes.Buffer, value storage.Value) {
	writePart(dst, []byte(value.Type))
	dst.WriteByte(byte(value.State))
	switch value.Type {
	case storage.TypeList:
		for _, child := range value.List {
			writePart(dst, CanonicalBytes(child))
		}
	case storage.TypeObject:
		keys := make([]string, 0, len(value.Object))
		for key := range value.Object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			writePart(dst, []byte(key))
			writePart(dst, CanonicalBytes(value.Object[key]))
		}
	case storage.TypeDateTime:
		encoded, _ := value.DateTime.MarshalBinary()
		writePart(dst, encoded)
	default:
		writePart(dst, value.Data)
	}
}

func writePart(dst *bytes.Buffer, data []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(data)))
	dst.Write(size[:])
	dst.Write(data)
}
