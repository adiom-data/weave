package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/storage"
)

func initialCacheFixture(t *testing.T) (*Coordinator, fakePlan, storage.EntityKey) {
	t.Helper()
	plan := fakePlan{fingerprint: "cached", relation: storage.RelationKey{Source: "test", Name: "items"}}
	key := storage.EntityKey{Relation: plan.Relation(), Value: "1"}
	entities := entitycache.NewEntityCache()
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}})
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if err := queries.SetWindow(plan.Fingerprint(), "warm", bounds); err != nil {
		t.Fatal(err)
	}
	if _, _, err := queries.AddMember(plan.Fingerprint(), key); err != nil {
		t.Fatal(err)
	}
	// Deliberately no datastore or running actor: every phase under test must
	// be in-memory and complete in this single actor call.
	c := &Coordinator{entities: entities, queries: queries, subscriptions: subscriptions.NewRegistry(), recovery: recovery.NewManager(1, 1, 1), hub: delivery.NewSubscriptionHub(queries), lastCheckpoint: testPosition(1)}
	c.hub.OpenGroup("feed", 16)
	c.hub.OpenGroup("", 16)
	return c, plan, key
}

func TestCachedInitialStartPublishesAndRegistersInOneActorTurn(t *testing.T) {
	c, plan, key := initialCacheFixture(t)
	operation := initialWindowOperation{subscriptionID: "new", group: "feed", plan: plan}
	start, err := c.startSubscribeActor(operation, InitialWindow{Count: 1})
	if err != nil || start.opened == nil || !start.staged || start.materialization != nil {
		t.Fatalf("start = %+v, error = %v", start, err)
	}
	if _, ok := c.subscriptions.Active("new"); !ok || len(start.opened.Initial) != 1 || start.opened.Checkpoint != c.lastCheckpoint {
		t.Fatal("snapshot and live registration were not installed together")
	}
	start.opened.Initial[0].Record["id"].Data[0] = '9'
	current, _ := c.entities.Get(key)
	if string(current.Record["id"].Data) != "1" {
		t.Fatal("owned output aliased cached data")
	}
	if err := c.unsubscribeActor("new"); err != nil {
		t.Fatal(err)
	}
}

func TestCachedInitialStartDefersMissingProofAndReferenceWork(t *testing.T) {
	for _, mode := range []string{"no checkpoint", "hydrating", "reference prefetch", "partial coverage"} {
		t.Run(mode, func(t *testing.T) {
			c, plan, key := initialCacheFixture(t)
			operation := initialWindowOperation{subscriptionID: "new", plan: plan}
			switch mode {
			case "no checkpoint":
				c.lastCheckpoint = nil
			case "hydrating":
				load := c.entities.BeginRefresh(key)
				if _, _, err := c.recovery.StartHydration(key, load, []datastore.QueryFingerprint{plan.Fingerprint()}, func() {}); err != nil {
					t.Fatal(err)
				}
			case "reference prefetch":
				operation.reference = fixedPreparedReference{plan: plan}
			case "partial coverage":
				bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("1")}}
				if err := c.queries.SetWindow(plan.Fingerprint(), "warm", bounds); err != nil {
					t.Fatal(err)
				}
			}
			start, err := c.startSubscribeActor(operation, InitialWindow{Count: 2})
			if err != nil || start.opened != nil {
				t.Fatalf("premature publication: %+v, error = %v", start, err)
			}
			if mode == "reference prefetch" {
				if !start.staged || start.materialization != nil {
					t.Fatal("cached parent should wait only for reference prefetch")
				}
			} else if start.staged {
				t.Fatal("unresolved/unproven coverage skipped asynchronous discovery")
			}
		})
	}
}

func TestCachedInitialStartFailureLeavesExistingSubscriberAlone(t *testing.T) {
	c, plan, _ := initialCacheFixture(t)
	operation := initialWindowOperation{subscriptionID: "existing", plan: plan}
	if _, err := c.startSubscribeActor(operation, InitialWindow{Count: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.startSubscribeActor(operation, InitialWindow{Count: 1}); !errors.Is(err, delivery.ErrSubscriptionExists) {
		t.Fatalf("duplicate subscription error = %v", err)
	}
	if _, ok := c.subscriptions.Active("existing"); !ok {
		t.Fatal("rejected duplicate removed the original subscriber")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.startSubscribe(ctx, operation, InitialWindow{Count: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
}

func TestCachedInitialStartEvaluationFailureReleasesOnlyItsState(t *testing.T) {
	c, plan, key := initialCacheFixture(t)
	plan.keyError = errors.New("evaluation failed")
	if _, err := c.startSubscribeActor(initialWindowOperation{subscriptionID: "failed", plan: plan}, InitialWindow{Count: 1}); !errors.Is(err, plan.keyError) {
		t.Fatalf("initialization error = %v", err)
	}
	current, _ := c.entities.Get(key)
	if c.subscriptions.Exists("failed") || current.References != 1 {
		t.Fatal("failed initialization retained lifecycle state or changed shared membership ownership")
	}
	if _, ok := c.queries.Plan(plan.Fingerprint()); !ok {
		t.Fatal("failed initialization discarded the existing shared query")
	}
}

type cancelOnInitialKey struct {
	fakePlan
	cancel context.CancelFunc
}

func (p cancelOnInitialKey) Key(record storage.Record) (datastore.SortKey, error) {
	p.cancel()
	return p.fakePlan.Key(record)
}

func TestCachedInitialCancellationAfterAcceptanceCleansUpActiveState(t *testing.T) {
	c, plan, _ := initialCacheFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepared := cancelOnInitialKey{fakePlan: plan, cancel: cancel}
	c.commands, c.done = make(chan coordinatorCommand), make(chan struct{})
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case command := <-c.commands:
				command.run()
			case <-stop:
				return
			}
		}
	}()
	defer func() { close(stop); <-stopped }()
	_, err := c.startSubscribe(ctx, initialWindowOperation{subscriptionID: "canceled", plan: prepared}, InitialWindow{Count: 1})
	if !errors.Is(err, context.Canceled) || c.subscriptions.Exists("canceled") {
		t.Fatalf("accepted cancellation error = %v, leaked subscription = %v", err, c.subscriptions.Exists("canceled"))
	}
}
