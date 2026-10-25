// Package streamqueue provides a bounded single-consumer queue for ordered
// streams. Overflow is terminal because dropping a suffix would make an
// incremental consumer inconsistent.
package streamqueue

import (
	"context"
	"errors"
	"sync"
)

type Queue[T any] struct {
	mu           sync.Mutex
	capacity     int
	items        []T
	closed       bool
	reason       error
	fullReason   error
	closedReason error
	wake         chan struct{}
}

func New[T any](capacity int, fullReason, closedReason error) *Queue[T] {
	if capacity < 1 {
		capacity = 1
	}
	if fullReason == nil {
		fullReason = errors.New("stream queue is full")
	}
	if closedReason == nil {
		closedReason = errors.New("stream queue is closed")
	}
	return &Queue[T]{capacity: capacity, fullReason: fullReason, closedReason: closedReason, wake: make(chan struct{}, 1)}
}

func (q *Queue[T]) Offer(item T) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return q.reasonOrClosed()
	}
	if len(q.items) >= q.capacity {
		q.items = nil
		q.closed = true
		q.reason = q.fullReason
		q.signal()
		return q.reason
	}
	q.items = append(q.items, item)
	q.signal()
	return nil
}

// Len returns the number of accepted items not yet observed by the consumer.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue[T]) Next(ctx context.Context) (T, error) {
	var zero T
	for {
		q.mu.Lock()
		if q.closed {
			reason := q.reasonOrClosed()
			q.mu.Unlock()
			return zero, reason
		}
		if len(q.items) > 0 {
			item := q.items[0]
			q.items[0] = zero
			q.items = q.items[1:]
			q.mu.Unlock()
			return item, nil
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

func (q *Queue[T]) Close(reason error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.items = nil
	q.closed = true
	q.reason = reason
	q.signal()
}

func (q *Queue[T]) reasonOrClosed() error {
	if q.reason != nil {
		return q.reason
	}
	return q.closedReason
}

func (q *Queue[T]) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
