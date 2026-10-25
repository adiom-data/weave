package delivery

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/streamqueue"
)

var (
	ErrClientQueueFull = errors.New("client event queue is full; resynchronization required")
	ErrClientClosed    = errors.New("client event queue is closed")
)

// ClientQueue decouples reactive evaluation from network writes. Overflow is
// terminal: pending incremental events are discarded because the client can no
// longer safely apply a suffix of the stream and must obtain a new snapshot.
type ClientQueue[T any] struct {
	queue *streamqueue.Queue[T]
}

func NewClientQueue[T any](capacity int) *ClientQueue[T] {
	return &ClientQueue[T]{queue: streamqueue.New[T](capacity, ErrClientQueueFull, ErrClientClosed)}
}

func (q *ClientQueue[T]) Offer(item T) error {
	return q.queue.Offer(item)
}

func (q *ClientQueue[T]) Next(ctx context.Context) (T, error) {
	return q.queue.Next(ctx)
}

func (q *ClientQueue[T]) Close(reason error) {
	q.queue.Close(reason)
}
