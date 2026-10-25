package messagebudget

import (
	"math"
	"testing"
)

func TestOversizedMessageDoesNotDiscardFollowingValidMessage(t *testing.T) {
	b := Budget{Limits: Limits{MaxMessageBytes: 8, MaxTransactionBytes: 256, MaxMessages: 10}}
	for index, want := range []Decision{Keep, Empty, Keep} {
		sizes := [][2]int{{1, 2}, {1, math.MaxInt}, {2, 2}}
		if got := b.Add(sizes[index][0], sizes[index][1]); got != want {
			t.Fatalf("message %d = %v, want %v", index, got, want)
		}
	}
	if b.Dropped != 1 || b.Bytes > b.Limits.MaxTransactionBytes {
		t.Fatalf("budget = %+v", b)
	}
}

func TestCountAndByteExhaustionHaveOneBoundedTailMarker(t *testing.T) {
	for _, limits := range []Limits{
		{MaxMessageBytes: 16, MaxTransactionBytes: 256, MaxMessages: 2},
		{MaxMessageBytes: 16, MaxTransactionBytes: 64, MaxMessages: 100},
	} {
		b := Budget{Limits: limits}
		retained := 0
		for index := 0; index < 10000; index++ {
			if b.Add(0, 0) != Skip {
				retained++
			}
			if b.Bytes > limits.MaxTransactionBytes || retained > limits.MaxMessages+1 {
				t.Fatalf("unbounded budget = %+v", b)
			}
		}
		if retained != b.Count+1 || !b.tail {
			t.Fatalf("missing or repeated tail = %+v", b)
		}
	}
}

func FuzzBudgetAlwaysBoundsRetainedMetadata(f *testing.F) {
	f.Add([]byte{0, 255, 1, 2, 255, 255})
	f.Fuzz(func(t *testing.T, sizes []byte) {
		limits := Limits{MaxMessageBytes: 64, MaxTransactionBytes: 512, MaxMessages: 8}
		b := Budget{Limits: limits}
		retained := 0
		actualBytes := 0
		for _, size := range sizes {
			d := b.Add(1, int(size))
			switch d {
			case Keep:
				retained++
				actualBytes += MessageOverhead + 1 + int(size)
			case Empty:
				retained++
				actualBytes += MessageOverhead
			}
			if b.Bytes != actualBytes || actualBytes > limits.MaxTransactionBytes || retained > limits.MaxMessages+1 {
				t.Fatalf("invalid budget: %+v retained=%d bytes=%d", b, retained, actualBytes)
			}
		}
	})
}
