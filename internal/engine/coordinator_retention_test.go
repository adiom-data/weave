package engine

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

func TestQueryGraceKeepsAbandonedRangesCurrentWithoutDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id, title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	reads := make(chan datastore.WindowRequest, 20)
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "old"), record("2", "old"), record("3", "old")}, feed: feed, windowReads: reads}
	c, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{QueryIdleTTL: time.Minute, QueryMaintenancePeriod: time.Millisecond, EvictionBatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait(context.Background()) }()
	opened, err := c.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	initialReads := len(reads)
	if _, err := c.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryContract, Count: 1}}, "contract"); err != nil {
		t.Fatal(err)
	}
	if batch, err := nextTestBatch(ctx, opened.Subscription); err != nil || batch.Window == nil {
		t.Fatalf("no contraction: %v %v", batch, err)
	}
	commit := func(point int, kind datastore.ChangeKind, id, title string) {
		t.Helper()
		key := storage.EntityKey{Relation: relation, Value: id}
		feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(point), Changes: []datastore.Change{fakeChange{kind: kind, relation: relation, before: key, after: key, record: record(id, title), complete: true}}}
		for feed.acknowledged() < testPosition(point) {
			select {
			case <-ctx.Done():
				t.Fatal("CDC did not advance")
			case <-time.After(time.Millisecond):
			}
		}
	}
	commit(1, datastore.ChangeUpdate, "2", "retained")
	// The retained range owns no delivery window. If its update leaked, it
	// would be queued before this next transition.
	if _, err := c.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "extend"); err != nil {
		t.Fatal(err)
	}
	batch, err := nextTestBatch(ctx, opened.Subscription)
	if err != nil || batch.Window == nil {
		t.Fatalf("retention produced client output or lost extension: %v %v", batch, err)
	}
	fresh := false
	for _, event := range batch.Events {
		if event.Key.Value == "2" && string(event.Record["title"].Data) == "retained" {
			fresh = true
		}
	}
	if !fresh || len(reads) != initialReads {
		t.Fatal("extension did not reuse current retained coverage")
	}
	if _, err := c.Unsubscribe(ctx, "viewer"); err != nil {
		t.Fatal(err)
	}
	commit(2, datastore.ChangeUpdate, "2", "idle")
	commit(3, datastore.ChangeInsert, "15", "new-idle")
	again, err := c.Subscribe(ctx, "again", storage.QuerySpec{}, InitialWindow{Count: 3}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != initialReads || len(again.Initial) != 3 {
		t.Fatalf("last-window grace did not reuse query: reads %d->%d rows %d", initialReads, len(reads), len(again.Initial))
	}
	for _, entity := range again.Initial {
		if entity.Key.Value == "2" && string(entity.Record["title"].Data) != "idle" {
			t.Fatal("idle query stopped processing updates")
		}
	}
}
