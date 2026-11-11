package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/metrics"
	"github.com/adiom-data/weave/internal/storage"
)

type applicationMetrics struct {
	mu      sync.Mutex
	readers map[string]func() int64
}

func (p *applicationMetrics) RegisterGauge(name string, read func() int64) func() {
	p.mu.Lock()
	p.readers[name] = read
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		delete(p.readers, name)
		p.mu.Unlock()
	}
}

func TestCoordinatorUsesApplicationMetricsProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := &applicationMetrics{readers: make(map[string]func() int64)}
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: storage.RelationKey{Source: "test", Name: "tasks"}, routing: routing.RoutingPlan{Fallback: true}},
		feed:    newCoordinatorFeed(),
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
	}
	c, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Metrics: p})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = c.Wait(context.Background()) }()
	if _, err := c.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4); err != nil {
		t.Fatal(err)
	}
	// Synchronize with the actor's next turn, which publishes its counts before
	// accepting another command. Provider collection never enters the actor.
	turn := make(chan struct{})
	select {
	case c.commands <- coordinatorCommand{run: func() { close(turn) }}:
	case <-c.done:
		t.Fatal("coordinator stopped before publishing metrics")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-turn
	p.mu.Lock()
	for name, want := range map[string]int64{
		metrics.ActiveSubscriptions: 1, metrics.InitializingSubscriptions: 0,
		metrics.CachedQueries: 1, metrics.CachedEntities: 1,
	} {
		read := p.readers[name]
		if read == nil || read() != want {
			t.Errorf("wrong application metric %s", name)
		}
	}
	p.mu.Unlock()
	cancel()
	_ = c.Wait(context.Background())
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.readers) != 0 {
		t.Fatal("coordinator retained provider registrations after shutdown")
	}
}
