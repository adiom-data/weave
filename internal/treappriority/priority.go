// Package treappriority derives privately seeded treap priorities from
// composite keys without losing component boundaries.
package treappriority

import (
	"encoding/binary"
	"hash/maphash"
)

// Seeded hashes length-delimited parts with a private per-tree seed. Length
// delimiters ensure distinct composite keys cannot become identical hash input
// merely because their component boundaries differ.
func Seeded(seed maphash.Seed, parts ...[]byte) uint64 {
	var hash maphash.Hash
	hash.SetSeed(seed)
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(part)
	}
	return hash.Sum64()
}
