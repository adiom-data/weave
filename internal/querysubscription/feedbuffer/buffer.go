// Package feedbuffer owns the ordered publication boundary shared by local
// and remote query feeds. Snapshot installation never invokes consumer code
// while holding the buffer lock.
package feedbuffer

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/streamqueue"
)

// Buffer stages complete envelopes while snapshots install. Publishing a
// staged prefix and starting another installation are serialized with Offer;
// no event can slip between checking installation state and entering the queue.
// Failure is terminal for its subscription ID for the buffer's lifetime. Later
// data is filtered at enqueue time; already accepted data keeps its prior order.
// All work under mu is bounded in-memory work, never IO or a callback.
type Buffer struct {
	mu         sync.Mutex
	limit      int
	installing int
	staged     []querysubscription.Event
	queue      *streamqueue.Queue[querysubscription.Event]
	positions  position.Tracker
	terminal   map[string]struct{} // guarded by mu; never cleared by installation completion
	err        error
}

func New(limit int) *Buffer {
	return &Buffer{limit: limit, queue: streamqueue.New[querysubscription.Event](limit, querysubscription.ErrBackpressure, querysubscription.ErrClosed)}
}

func (b *Buffer) BeginInstallation() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.installing++
}

func (b *Buffer) FinishInstallation() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.installing == 0 {
		return errors.New("unbalanced snapshot installation")
	}
	b.installing--
	if b.err != nil {
		return b.err
	}
	if b.installing > 0 {
		return nil
	}
	for _, event := range b.staged {
		if err := b.queue.Offer(event); err != nil {
			return b.closeLocked(err)
		}
	}
	b.staged = nil
	return nil
}

func (b *Buffer) Offer(event querysubscription.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	if err := event.Validate(); err != nil {
		return b.closeLocked(err)
	}
	if event.Failure != nil {
		if _, terminal := b.terminal[event.Failure.SubscriptionID]; terminal {
			return nil
		}
		b.markTerminalLocked(event.Failure.SubscriptionID)
	} else {
		var keep bool
		event, keep = withoutSubscriptions(event, b.terminal)
		if !keep {
			return nil
		}
	}
	var err error
	if event.Progress != nil {
		err = b.positions.ObserveReport(event.Progress.Position)
	} else if event.Delivery != nil && event.Delivery.Delivery.Kind == querysubscription.DeliveryCommit {
		err = b.positions.Observe(event.Delivery.Delivery.Checkpoint)
	}
	if err != nil {
		return b.closeLocked(err)
	}
	if b.installing > 0 {
		if len(b.staged) >= b.limit {
			return b.closeLocked(querysubscription.ErrBackpressure)
		}
		b.staged = append(b.staged, event)
		return nil
	}
	if err := b.queue.Offer(event); err != nil {
		return b.closeLocked(err)
	}
	return nil
}

// DiscardSubscription retires a failed installation at the same publication
// boundary, removing its staged and future effects while preserving complete
// envelopes, other subscriptions' effects, and progress.
func (b *Buffer) DiscardSubscription(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return
	}
	b.markTerminalLocked(id)
	excluded := map[string]struct{}{id: {}}
	retained := b.staged[:0]
	for _, event := range b.staged {
		if event, keep := withoutSubscriptions(event, excluded); keep {
			retained = append(retained, event)
		}
	}
	clear(b.staged[len(retained):])
	b.staged = retained
}

func (b *Buffer) Next(ctx context.Context) (querysubscription.Event, error) { return b.queue.Next(ctx) }
func (b *Buffer) Len() int                                                  { return b.queue.Len() }

func (b *Buffer) Close(reason error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeLocked(reason)
}

func (b *Buffer) closeLocked(reason error) error {
	if b.err == nil {
		if reason == nil {
			reason = querysubscription.ErrClosed
		}
		b.err = reason
		b.staged = nil
		b.terminal = nil
		b.queue.Close(reason)
	}
	return b.err
}
