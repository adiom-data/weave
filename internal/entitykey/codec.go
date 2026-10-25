package entitykey

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sort"
)

const version byte = 1

type Part struct {
	Name    string
	TypeOID uint32
	Value   []byte
}

// Encode produces an unambiguous, versioned representation suitable for use
// as a Go map key. Values may contain arbitrary bytes.
func Encode(parts []Part) string {
	buffer := []byte{version}
	buffer = binary.AppendUvarint(buffer, uint64(len(parts)))
	for _, part := range parts {
		buffer = appendBytes(buffer, []byte(part.Name))
		buffer = binary.BigEndian.AppendUint32(buffer, part.TypeOID)
		buffer = appendBytes(buffer, part.Value)
	}
	return base64.RawURLEncoding.EncodeToString(buffer)
}

// CanonicalEncode makes a composite identity independent of the order in
// which its source reports the named components. Names are the semantic part
// identifiers; the remaining comparisons make malformed duplicate-name input
// deterministic as well.
func CanonicalEncode(parts []Part) string {
	canonical := append([]Part(nil), parts...)
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].Name != canonical[j].Name {
			return canonical[i].Name < canonical[j].Name
		}
		if canonical[i].TypeOID != canonical[j].TypeOID {
			return canonical[i].TypeOID < canonical[j].TypeOID
		}
		return bytes.Compare(canonical[i].Value, canonical[j].Value) < 0
	})
	return Encode(canonical)
}

func Decode(encoded string) ([]Part, error) {
	buffer, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(buffer) == 0 || buffer[0] != version {
		return nil, errors.New("invalid entity key")
	}
	buffer = buffer[1:]
	count, n := binary.Uvarint(buffer)
	if n <= 0 {
		return nil, errors.New("invalid entity key")
	}
	buffer = buffer[n:]
	// Every part consumes at least two length prefixes plus a four-byte OID.
	// This also bounds the allocation requested by an untrusted cursor string.
	if count > uint64(len(buffer)/6) {
		return nil, errors.New("invalid entity key")
	}
	parts := make([]Part, 0, count)
	for range count {
		name, rest, ok := takeBytes(buffer)
		if !ok || len(rest) < 4 {
			return nil, errors.New("invalid entity key")
		}
		typeOID := binary.BigEndian.Uint32(rest[:4])
		value, rest, ok := takeBytes(rest[4:])
		if !ok {
			return nil, errors.New("invalid entity key")
		}
		parts = append(parts, Part{Name: string(name), TypeOID: typeOID, Value: append([]byte(nil), value...)})
		buffer = rest
	}
	if len(buffer) != 0 {
		return nil, errors.New("invalid entity key")
	}
	return parts, nil
}

func appendBytes(buffer, value []byte) []byte {
	buffer = binary.AppendUvarint(buffer, uint64(len(value)))
	return append(buffer, value...)
}

func takeBytes(buffer []byte) ([]byte, []byte, bool) {
	length, n := binary.Uvarint(buffer)
	if n <= 0 || length > uint64(len(buffer)-n) {
		return nil, nil, false
	}
	start, end := n, n+int(length)
	return buffer[start:end], buffer[end:], true
}
