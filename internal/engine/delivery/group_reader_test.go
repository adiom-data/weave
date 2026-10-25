package delivery

import (
	"context"
	"sync"
)

// Test assertions may inspect individual contributions, but the producer is
// always the real shared group queue. No private delivery queues are created.
type testSubscription struct {
	*Subscription
	Queue *testGroupReader
}

type testGroupState struct {
	mu       sync.Mutex
	queue    *ClientQueue[GroupMessage]
	pending  map[string][]SubscriptionBatch
	failures map[string]error
}

type testGroupReader struct {
	state *testGroupState
	id    string
}

var testGroups sync.Map // real group queue -> test assertion demultiplexer

func testHandle(sub *Subscription, queue *ClientQueue[GroupMessage]) *testSubscription {
	state, _ := testGroups.LoadOrStore(queue, &testGroupState{queue: queue, pending: make(map[string][]SubscriptionBatch), failures: make(map[string]error)})
	return &testSubscription{Subscription: sub, Queue: &testGroupReader{state: state.(*testGroupState), id: sub.ID}}
}

func (r *testGroupReader) consume(message GroupMessage) {
	for _, batch := range message.Batches {
		r.state.pending[batch.SubscriptionID] = append(r.state.pending[batch.SubscriptionID], batch)
	}
	for _, failure := range message.Failures {
		r.state.failures[failure.ID] = failure.Cause
	}
}

func (r *testGroupReader) Next(ctx context.Context) (SubscriptionBatch, error) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	for {
		if items := r.state.pending[r.id]; len(items) > 0 {
			r.state.pending[r.id] = items[1:]
			return items[0], nil
		}
		if err := r.state.failures[r.id]; err != nil {
			return SubscriptionBatch{}, err
		}
		message, err := r.state.queue.Next(ctx)
		if err != nil {
			return SubscriptionBatch{}, err
		}
		r.consume(message)
	}
}

func (r *testGroupReader) queued() int {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		message, err := r.state.queue.Next(ctx)
		if err != nil {
			break
		}
		r.consume(message)
	}
	return len(r.state.pending[r.id])
}
