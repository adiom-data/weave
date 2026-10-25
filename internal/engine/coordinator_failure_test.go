package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInstallInitializingWindowHandlesMissingQueryPlan(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "query", relation: relation}
	coordinator := &Coordinator{
		commands:      make(chan coordinatorCommand),
		done:          make(chan struct{}),
		queries:       queryregistry.NewQueryRegistry(entitycache.NewEntityCache()),
		subscriptions: subscriptions.NewRegistry(),
		recovery:      recovery.NewManager(1, 1, 1),
	}
	coordinator.subscriptions.BeginInitialization("viewer", plan)
	go func() {
		command := <-coordinator.commands
		command.run()
	}()
	_, err := coordinator.installInitializingWindow(context.Background(), initialWindowOperation{subscriptionID: "viewer", plan: plan}, resolvedWindow{bounds: datastore.Bounds{}, source: windowResolvedFromCache})
	if !errors.Is(err, storage.ErrResumeRequired) {
		t.Fatalf("error = %v, want ErrResumeRequired", err)
	}
}

func TestFailQueryRemovesEveryLifecycleReference(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "query", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	hub := delivery.NewSubscriptionHub(queries)
	hub.OpenGroup("", 16)
	registry := subscriptions.NewRegistry()
	coordinator := &Coordinator{entities: entities, queries: queries, hub: hub, subscriptions: registry, done: make(chan struct{})}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	cause := errors.New("query failed")

	for _, id := range []string{"first", "second"} {
		queries.Acquire(plan)
		if err := queries.SetWindow(plan.Fingerprint(), id, bounds); err != nil {
			t.Fatal(err)
		}
		registry.Activate(id, activeSubscription{Fingerprint: plan.Fingerprint(), Plan: plan, Bounds: bounds})
		if _, err := hub.SubscribePreparedGrouped(id, "", plan, bounds); err != nil {
			t.Fatal(err)
		}
	}

	coordinator.failQuery(plan.Fingerprint(), cause)
	if _, ok := queries.Plan(plan.Fingerprint()); ok {
		t.Fatal("failed query remained registered")
	}
	for _, id := range []string{"first", "second"} {
		if _, ok := registry.Active(id); ok {
			t.Fatalf("subscription %q remained active", id)
		}
	}
}

func TestReleaseSharedQueryPrunesAbandonedWindowMembers(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "query", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	coordinator := &Coordinator{entities: entities, queries: queries, done: make(chan struct{})}
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	empty := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryEnd}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	queries.Acquire(plan)
	queries.Acquire(plan)
	if err := queries.SetWindow(plan.Fingerprint(), "abandoned", all); err != nil {
		t.Fatal(err)
	}
	if err := queries.SetWindow(plan.Fingerprint(), "remaining", empty); err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}})
	if _, _, err := queries.AddMember(plan.Fingerprint(), key); err != nil {
		t.Fatal(err)
	}

	coordinator.releaseQuery("abandoned", plan.Fingerprint())
	if failures, more := queries.Maintain(time.Now(), 128); len(failures) != 0 || more {
		t.Fatalf("prune failed: %v %v", failures, more)
	}
	if queries.IsMember(plan.Fingerprint(), key) {
		t.Fatal("member from abandoned window was not pruned")
	}
}

func TestStaleRelationResyncResultCannotReplaceNewAttempt(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	manager := recovery.NewManager(1, 1, 1)
	_, cancelFirst := context.WithCancel(context.Background())
	first, started := manager.StartRelation(relation, recovery.NewRelationSnapshot(), datastore.CommittedChanges{}, cancelFirst)
	if !started {
		t.Fatal("first recovery did not start")
	}
	manager.RemoveRelation(relation)
	_, cancelSecond := context.WithCancel(context.Background())
	second, started := manager.StartRelation(relation, recovery.NewRelationSnapshot(), datastore.CommittedChanges{}, cancelSecond)
	if !started {
		t.Fatal("second recovery did not start")
	}
	coordinator := &Coordinator{recovery: manager}
	coordinator.acceptResyncResult(context.Background(), resyncReadResult{
		relation: relation, generation: first.Generation(), err: errors.New("stale failure"),
	})
	current, ok := manager.Relation(relation)
	if !ok || current != second {
		t.Fatal("stale result removed or replaced the current recovery")
	}
}

func TestAbortWindowDoesNotReevaluateRetainedMembers(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	badPlan := fakePlan{fingerprint: "bad", relation: relation}
	healthyPlan := fakePlan{fingerprint: "healthy", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	hub := delivery.NewSubscriptionHub(queries)
	hub.OpenGroup("", 16)
	registry := subscriptions.NewRegistry()
	coordinator := &Coordinator{
		entities: entities, queries: queries, hub: hub, subscriptions: registry,
		commands: make(chan coordinatorCommand), done: make(chan struct{}),
	}
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	queries.Acquire(badPlan)
	queries.Acquire(healthyPlan)
	if err := queries.SetWindow(badPlan.Fingerprint(), "bad-sub", all); err != nil {
		t.Fatal(err)
	}
	if err := queries.SetWindow(badPlan.Fingerprint(), "bad-stage", all); err != nil {
		t.Fatal(err)
	}
	registry.Activate("bad-sub", activeSubscription{Fingerprint: badPlan.Fingerprint(), Plan: badPlan, Bounds: all})
	if _, ok := registry.BeginWindow("bad-sub", "token"); !ok {
		t.Fatal("failed to begin test window")
	}
	if _, err := hub.SubscribePreparedGrouped("bad-sub", "", badPlan, all); err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: relation, Value: "one"}
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}})
	if _, _, err := queries.AddMember(badPlan.Fingerprint(), key); err != nil {
		t.Fatal(err)
	}
	go func() {
		command := <-coordinator.commands
		command.run()
	}()
	coordinator.abortWindowUpdate(windowUpdateOperation{subscriptionID: "bad-sub", token: "token", stageID: "bad-stage", base: activeSubscription{Fingerprint: badPlan.Fingerprint(), Plan: badPlan}})
	if coordinator.hasTerminalError() {
		t.Fatalf("query-local abort stopped coordinator: %v", coordinator.currentTerminalError())
	}
	if _, ok := queries.Plan(badPlan.Fingerprint()); !ok {
		t.Fatal("window abort discarded a valid shared query")
	}
	if _, ok := queries.Plan(healthyPlan.Fingerprint()); !ok {
		t.Fatal("healthy query was removed")
	}
}
