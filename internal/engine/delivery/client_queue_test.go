package delivery

import (
	"context"
	"errors"
	"testing"
)

func TestClientQueueNeverBlocksProducerAndPoisonsOnOverflow(t *testing.T) {
	queue := NewClientQueue[int](2)
	if err := queue.Offer(1); err != nil {
		t.Fatal(err)
	}
	if err := queue.Offer(2); err != nil {
		t.Fatal(err)
	}
	if err := queue.Offer(3); !errors.Is(err, ErrClientQueueFull) {
		t.Fatalf("overflow error = %v", err)
	}
	if _, err := queue.Next(context.Background()); !errors.Is(err, ErrClientQueueFull) {
		t.Fatalf("next error = %v", err)
	}
	if err := queue.Offer(4); !errors.Is(err, ErrClientQueueFull) {
		t.Fatalf("offer after overflow = %v", err)
	}
}

func TestClientQueueDeliversInOrder(t *testing.T) {
	queue := NewClientQueue[string](2)
	_ = queue.Offer("a")
	_ = queue.Offer("b")
	for _, expected := range []string{"a", "b"} {
		actual, err := queue.Next(context.Background())
		if err != nil || actual != expected {
			t.Fatalf("next = %q, %v; expected %q", actual, err, expected)
		}
	}
}
