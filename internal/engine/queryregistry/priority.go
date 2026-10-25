package queryregistry

import (
	"hash/maphash"

	"github.com/adiom-data/weave/internal/treappriority"
)

// seededPriority hashes length-delimited parts with a private per-tree seed.
// Length delimiters ensure distinct composite keys cannot hash as the same byte
// concatenation merely because their component boundaries differ.
func seededPriority(seed maphash.Seed, parts ...[]byte) uint64 {
	return treappriority.Seeded(seed, parts...)
}
