package treappriority

import (
	"hash/maphash"
	"testing"
)

func TestSeededPreservesCompositeKeyBoundaries(t *testing.T) {
	seed := maphash.MakeSeed()
	left := Seeded(seed, []byte("a"), []byte("b\x00c"))
	right := Seeded(seed, []byte("a\x00b"), []byte("c"))
	if left == right {
		t.Fatal("distinct composite keys produced the same priority")
	}
}
