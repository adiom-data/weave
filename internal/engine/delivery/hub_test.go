package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/storage"
)

func (h *SubscriptionHub) subscribe(id string, plan datastore.PreparedQuery, bounds datastore.Bounds, initial []entitycache.EntitySnapshot, queueLimit int) (*testSubscription, []entitycache.EntitySnapshot, error) {
	return h.subscribeGrouped(id, "test:"+id, plan, bounds, initial, queueLimit)
}

func (h *SubscriptionHub) subscribeGrouped(id, group string, plan datastore.PreparedQuery, bounds datastore.Bounds, initial []entitycache.EntitySnapshot, queueLimit int) (*testSubscription, []entitycache.EntitySnapshot, error) {
	filtered, err := FilterOwnedInitial(plan, bounds, initial)
	if err != nil {
		return nil, nil, err
	}
	queue := h.OpenGroup(group, queueLimit)
	public, err := h.SubscribePreparedGrouped(id, group, plan, bounds)
	if err != nil {
		return nil, filtered, err
	}
	return testHandle(public, queue), filtered, nil
}

func (h *SubscriptionHub) PublishRevision(checkpoint storage.Checkpoint, atomic bool, revision uint64, changes []changeprocessor.QueryChange) []string {
	return h.PublishDelivery(checkpoint, atomic, revision, DeliveryCommit, changes).Dropped
}

func (h *SubscriptionHub) Publish(checkpoint storage.Checkpoint, atomic bool, changes []changeprocessor.QueryChange) []string {
	return h.PublishRevision(checkpoint, atomic, 0, changes)
}

func (h *SubscriptionHub) publishRevisionDetailed(checkpoint storage.Checkpoint, atomic bool, revision uint64, changes []changeprocessor.QueryChange) PublishResult {
	return h.PublishDelivery(checkpoint, atomic, revision, DeliveryCommit, changes)
}

func (h *SubscriptionHub) stageDelivery(checkpoint storage.Checkpoint, atomic bool, revision uint64, delivery DeliveryKind, changes []changeprocessor.QueryChange) (*deliveryPlan, PublishResult) {
	return h.planDelivery(checkpoint, nil, atomic, revision, delivery, changes)
}

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
	orderField  string
}

func (p fakePlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p fakePlan) Relation() storage.RelationKey           { return p.relation }
func (p fakePlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (fakePlan) Matches(storage.Record) (bool, error) { return true, nil }
func (p fakePlan) Key(record storage.Record) (datastore.SortKey, error) {
	field := p.orderField
	if field == "" {
		field = "id"
	}
	return datastore.EncodedSortKey(record[field].Data), nil
}
func (fakePlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (fakePlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return key.(datastore.EncodedSortKey), nil
}
func (fakePlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(encoded), nil
}
func (fakePlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

type hubCheckpoint int

func (hubCheckpoint) StorageCheckpoint()       {}
func (p hubCheckpoint) StableID() string       { return fmt.Sprint(int(p)) }
func (hubCheckpoint) Source() storage.SourceID { return "test" }
func (p hubCheckpoint) Order() uint64          { return uint64(p) }

type hubFence int

func (hubFence) StorageFence()            {}
func (hubFence) Source() storage.SourceID { return "test" }
func (f hubFence) StableID() string       { return fmt.Sprint(int(f)) }
func (f hubFence) Order() uint64          { return uint64(f) }

func hubEntity(plan fakePlan, id string, visible bool) entitycache.EntitySnapshot {
	record := storage.Record{
		"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
		"visible": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(map[bool]string{true: "yes", false: "no"}[visible])},
	}
	key, _ := plan.EntityKey(record)
	return entitycache.EntitySnapshot{Key: key, Record: record, State: entitycache.EntityCurrent}
}

func TestFilterOwnedInitialTransfersDetachedRecords(t *testing.T) {
	plan := fakePlan{relation: storage.RelationKey{Source: "test", Name: "items"}}
	cache := entitycache.NewEntityCache()
	entity := hubEntity(plan, "one", true)
	cache.StoreCurrent(entity.Key, entity.Record)
	owned, _ := cache.Get(entity.Key)
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	filtered, err := FilterOwnedInitial(plan, bounds, []entitycache.EntitySnapshot{owned})
	if err != nil || len(filtered) != 1 {
		t.Fatal(filtered, err)
	}
	if &filtered[0].Record["id"].Data[0] != &owned.Record["id"].Data[0] {
		t.Fatal("owned record was unnecessarily copied")
	}
	filtered[0].Record["id"].Data[0] = 'X'
	current, _ := cache.Get(entity.Key)
	if string(current.Record["id"].Data) != "one" {
		t.Fatal("transferred record aliases cache")
	}
}

type acceptingWindowOrder struct{}

func (acceptingWindowOrder) PositionInBounds(datastore.QueryFingerprint, datastore.Bounds, storage.EntityKey) (int, bool, error) {
	return 0, true, nil
}

func newTestSubscriptionHub() *SubscriptionHub {
	return NewSubscriptionHub(acceptingWindowOrder{})
}

func TestSubscriptionHubAppliesPerClientWindows(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	all := []entitycache.EntitySnapshot{hubEntity(plan, "1", true), hubEntity(plan, "2", false), hubEntity(plan, "3", true)}
	first, initial, err := hub.subscribe("first", plan, datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: cursor("2")}, all, 4)
	if err != nil || len(initial) != 2 || initial[0].Key.Value != "1" || initial[1].Key.Value != "2" {
		t.Fatalf("initial = %+v, err=%v", initial, err)
	}
	second, secondInitial, err := hub.subscribe("second", plan, datastore.Bounds{Left: cursor("1"), Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, all, 4)
	if err != nil || len(secondInitial) != 2 {
		t.Fatalf("second initial = %+v, err=%v", secondInitial, err)
	}

	changed := hubEntity(plan, "2", true)
	hub.Publish(nil, true, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryUpdated, Key: changed.Key, PreviousRecord: all[1].Record, Record: changed.Record}})
	batch, err := first.Queue.Next(context.Background())
	event := batch.Events[0]
	if err != nil || event.Kind != changeprocessor.QueryUpdated || event.Key.Value != "2" {
		t.Fatalf("first event = %+v, %v", event, err)
	}
	batch, err = second.Queue.Next(context.Background())
	event = batch.Events[0]
	if err != nil || event.Kind != changeprocessor.QueryUpdated || event.Key.Value != "2" {
		t.Fatalf("second event = %+v, %v", event, err)
	}
}

func TestSubscriptionHubPoisonsOnlySlowClient(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	slow, _, _ := hub.subscribe("slow", plan, bounds, nil, 1)
	fast, _, _ := hub.subscribe("fast", plan, bounds, nil, 4)
	one, two := hubEntity(plan, "1", true), hubEntity(plan, "2", true)
	hub.Publish(nil, true, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: one.Key, Record: one.Record}})
	hub.Publish(nil, true, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: two.Key, Record: two.Record}})
	if _, err := slow.Queue.Next(context.Background()); !errors.Is(err, ErrClientQueueFull) {
		t.Fatalf("slow error = %v", err)
	}
	for _, expected := range []string{"1", "2"} {
		batch, err := fast.Queue.Next(context.Background())
		if err != nil || batch.Events[0].Key.Value != expected {
			t.Fatalf("fast batch = %+v, %v", batch, err)
		}
	}
}

func TestSubscriptionHubPoisonsWholeSlowDeliveryGroup(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	left, _, _ := hub.subscribeGrouped("left", "connection", plan, bounds, nil, 1)
	right, _, _ := hub.subscribeGrouped("right", "connection", plan, bounds, nil, 1)
	one, two := hubEntity(plan, "1", true), hubEntity(plan, "2", true)
	hub.publishRevisionDetailed(nil, true, 1, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: one.Key, Record: one.Record}})
	result := hub.publishRevisionDetailed(nil, true, 2, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: two.Key, Record: two.Record}})
	if len(result.Dropped) != 2 {
		t.Fatalf("dropped subscriptions = %v", result.Dropped)
	}
	for _, subscription := range []*testSubscription{left, right} {
		if _, err := subscription.Queue.Next(context.Background()); !errors.Is(err, ErrClientQueueFull) {
			t.Fatalf("group queue error = %v", err)
		}
	}
}

func TestSubscriptionHubKeepsCommittedChangesInOneQueueBatch(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, _ := hub.subscribe("viewer", plan, bounds, nil, 1)
	one, two := hubEntity(plan, "1", true), hubEntity(plan, "2", true)
	hub.Publish(hubCheckpoint(9), true, []changeprocessor.QueryChange{
		{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: one.Key, Record: one.Record},
		{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: two.Key, Record: two.Record},
	})
	batch, err := subscription.Queue.Next(context.Background())
	if err != nil || !batch.Atomic || batch.Checkpoint != hubCheckpoint(9) || len(batch.Events) != 2 {
		t.Fatalf("batch = %+v, err=%v", batch, err)
	}
}

func TestSubscriptionHubEmitsFinalOrderedDeltaWithoutWindowCopy(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "tasks", relation: relation, orderField: "order"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	record := func(id, order string) entitycache.EntitySnapshot {
		value := storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"order": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(order)},
		}
		key, _ := plan.EntityKey(value)
		return entitycache.EntitySnapshot{Key: key, Record: value, State: entitycache.EntityCurrent}
	}
	a, b, c := record("a", "1"), record("b", "2"), record("c", "3")
	for _, entity := range []entitycache.EntitySnapshot{a, b, c} {
		entities.StoreCurrent(entity.Key, entity.Record)
		if _, _, err := queries.AddMember(plan.Fingerprint(), entity.Key); err != nil {
			t.Fatal(err)
		}
	}
	hub := NewSubscriptionHub(queries)
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, err := hub.subscribe("viewer", plan, bounds, []entitycache.EntitySnapshot{a, b, c}, 2)
	if err != nil {
		t.Fatal(err)
	}
	boundedBounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("2")}}
	bounded, _, err := hub.subscribe("bounded", plan, boundedBounds, []entitycache.EntitySnapshot{a, b, c}, 2)
	if err != nil {
		t.Fatal(err)
	}

	movedC := record("c", "0")
	d := record("d", "3")
	entities.StoreCurrent(c.Key, movedC.Record)
	if err := queries.RefreshMember(plan.Fingerprint(), c.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := queries.RemoveMember(plan.Fingerprint(), b.Key); err != nil {
		t.Fatal(err)
	}
	entities.StoreCurrent(d.Key, d.Record)
	if _, _, err := queries.AddMember(plan.Fingerprint(), d.Key); err != nil {
		t.Fatal(err)
	}
	hub.Publish(nil, true, []changeprocessor.QueryChange{
		{Query: plan.Fingerprint(), Kind: changeprocessor.QueryUpdated, Key: c.Key, PreviousRecord: c.Record, Record: movedC.Record},
		{Query: plan.Fingerprint(), Kind: changeprocessor.QueryRemoved, Key: b.Key, PreviousRecord: b.Record},
		{Query: plan.Fingerprint(), Kind: changeprocessor.QueryAdded, Key: d.Key, Record: d.Record},
	})
	batch, err := subscription.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Membership.Removed) != 2 || batch.Membership.Removed[0] != c.Key && batch.Membership.Removed[1] != c.Key || batch.Membership.Removed[0] != b.Key && batch.Membership.Removed[1] != b.Key {
		t.Fatalf("removed membership = %v, want b and c", batch.Membership.Removed)
	}
	if len(batch.Membership.Insertions) != 2 || batch.Membership.Insertions[0] != (MembershipInsertion{Key: c.Key, FinalIndex: 0}) || batch.Membership.Insertions[1] != (MembershipInsertion{Key: d.Key, FinalIndex: 2}) {
		t.Fatalf("membership insertions = %+v", batch.Membership.Insertions)
	}
	boundedBatch, err := bounded.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(boundedBatch.Membership.Removed) != 1 || boundedBatch.Membership.Removed[0] != b.Key || len(boundedBatch.Membership.Insertions) != 1 || boundedBatch.Membership.Insertions[0] != (MembershipInsertion{Key: c.Key, FinalIndex: 0}) {
		t.Fatalf("bounded membership = %+v", boundedBatch.Membership)
	}
	temporaryC := record("c", "4")
	hub.Publish(nil, true, []changeprocessor.QueryChange{
		{Query: plan.Fingerprint(), Kind: changeprocessor.QueryUpdated, Key: c.Key, PreviousRecord: movedC.Record, Record: temporaryC.Record},
		{Query: plan.Fingerprint(), Kind: changeprocessor.QueryUpdated, Key: c.Key, PreviousRecord: temporaryC.Record, Record: movedC.Record},
	})
	for name, target := range map[string]*testSubscription{"full": subscription, "bounded": bounded} {
		collapsed, err := target.Queue.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(collapsed.Membership.Removed) != 0 || len(collapsed.Membership.Insertions) != 0 {
			t.Fatalf("%s intermediate movement leaked into membership: %+v", name, collapsed.Membership)
		}
	}
}

func TestSubscriptionHubAddsAndRemovesEqualSortKeyMembersIndependently(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "tasks", relation: relation, orderField: "order"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	record := func(id, order string) entitycache.EntitySnapshot {
		value := storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"order": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(order)},
		}
		key, _ := plan.EntityKey(value)
		return entitycache.EntitySnapshot{Key: key, Record: value, State: entitycache.EntityCurrent}
	}
	first, second, later := record("a", "same"), record("b", "same"), record("c", "z")
	for _, entity := range []entitycache.EntitySnapshot{first, later} {
		entities.StoreCurrent(entity.Key, entity.Record)
		if _, _, err := queries.AddMember(plan.Fingerprint(), entity.Key); err != nil {
			t.Fatal(err)
		}
	}

	hub := NewSubscriptionHub(queries)
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, err := hub.subscribe("viewer", plan, bounds, []entitycache.EntitySnapshot{first, later}, 2)
	if err != nil {
		t.Fatal(err)
	}

	entities.StoreCurrent(second.Key, second.Record)
	if _, added, err := queries.AddMember(plan.Fingerprint(), second.Key); err != nil || !added {
		t.Fatalf("add second tied member = %v, %v; want true, nil", added, err)
	}
	hub.Publish(nil, true, []changeprocessor.QueryChange{{Query: plan.Fingerprint(), Kind: changeprocessor.QueryAdded, Key: second.Key, Record: second.Record}})
	added, err := subscription.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(added.Membership.Insertions) != 1 || added.Membership.Insertions[0] != (MembershipInsertion{Key: second.Key, FinalIndex: 1}) {
		t.Fatalf("tied insertion = %+v, want second member at index 1", added.Membership)
	}

	if removed, err := queries.RemoveMember(plan.Fingerprint(), first.Key); err != nil || !removed {
		t.Fatalf("remove first tied member = %v, %v; want true, nil", removed, err)
	}
	hub.Publish(nil, true, []changeprocessor.QueryChange{{Query: plan.Fingerprint(), Kind: changeprocessor.QueryRemoved, Key: first.Key, PreviousRecord: first.Record}})
	deleted, err := subscription.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Membership.Removed) != 1 || deleted.Membership.Removed[0] != first.Key || len(deleted.Membership.Insertions) != 0 {
		t.Fatalf("tied removal = %+v, want only first member removed", deleted.Membership)
	}
	remaining := queries.Members(plan.Fingerprint())
	if len(remaining) != 2 || remaining[0] != second.Key || remaining[1] != later.Key {
		t.Fatalf("remaining members = %v, want [%v %v]", remaining, second.Key, later.Key)
	}
}

func TestDeliveryPlanCapturesSubscribersBeforeAuthorizationWait(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	old, _, err := hub.subscribe("old", plan, bounds, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	entity := hubEntity(plan, "1", true)
	staged, planned := hub.stageDelivery(hubCheckpoint(9), true, 1, DeliveryCommit, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record}})
	if len(planned.Dropped) != 0 {
		t.Fatalf("planning dropped = %v", planned.Dropped)
	}
	late, _, err := hub.subscribe("late", plan, bounds, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	hub.commitDelivery(staged)
	if batch, err := old.Queue.Next(context.Background()); err != nil || len(batch.Events) != 1 {
		t.Fatalf("captured batch = %+v, err=%v", batch, err)
	}
	emptyCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if batch, err := late.Queue.Next(emptyCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late subscription received an older planned transaction: batch=%+v err=%v", batch, err)
	}
}

func TestDeliveryPlanIgnoresSubscriberRemovedWhileWaiting(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if _, _, err := hub.subscribe("gone", plan, bounds, nil, 2); err != nil {
		t.Fatal(err)
	}
	entity := hubEntity(plan, "1", true)
	staged, _ := hub.stageDelivery(hubCheckpoint(9), true, 1, DeliveryCommit, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record}})
	if _, ok := hub.Unsubscribe("gone"); !ok {
		t.Fatal("unsubscribe failed")
	}
	if result := hub.commitDelivery(staged); len(result.Dropped) != 0 {
		t.Fatalf("commit dropped = %v", result.Dropped)
	}
}

func TestSubscriptionHubKeepsTruncateCompactAndAppliesLaterAdds(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	old := []entitycache.EntitySnapshot{hubEntity(plan, "1", true), hubEntity(plan, "2", true)}
	subscription, _, err := hub.subscribe("viewer", plan, bounds, old, 1)
	if err != nil {
		t.Fatal(err)
	}
	added := hubEntity(plan, "3", true)
	hub.Publish(hubCheckpoint(10), true, []changeprocessor.QueryChange{
		{Query: "tasks", Kind: changeprocessor.QueryCleared, ChangeIndex: 1},
		{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: added.Key, Record: added.Record, ChangeIndex: 2},
	})
	batch, err := subscription.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !batch.Clear || batch.ClearChangeIndex != 1 || len(batch.Events) != 1 || batch.Events[0].Kind != changeprocessor.QueryAdded || batch.Events[0].Key != added.Key {
		t.Fatalf("truncate delivery = %+v", batch)
	}
	if len(batch.Membership.Removed) != 0 || len(batch.Membership.Insertions) != 1 || batch.Membership.Insertions[0] != (MembershipInsertion{Key: added.Key, FinalIndex: 0}) {
		t.Fatalf("truncate membership = %+v", batch.Membership)
	}
}

func TestSubscriptionHubReportsAllConnectionDeliveriesForTransaction(t *testing.T) {
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	tasks := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	projects := fakePlan{fingerprint: "projects", relation: storage.RelationKey{Source: "test", Name: "projects"}}
	tasksSubscription, _, _ := hub.subscribeGrouped("tasks-sub", "connection-1", tasks, bounds, nil, 4)
	projectsSubscription, _, _ := hub.subscribeGrouped("projects-sub", "connection-1", projects, bounds, nil, 4)
	task := hubEntity(tasks, "task-1", true)
	project := hubEntity(projects, "project-1", true)
	result := hub.publishRevisionDetailed(hubCheckpoint(20), true, 7, []changeprocessor.QueryChange{
		{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: task.Key, Record: task.Record},
		{Query: "projects", Kind: changeprocessor.QueryAdded, Key: project.Key, Record: project.Record},
	})
	if len(result.Dropped) != 0 {
		t.Fatalf("publish result = %+v", result)
	}
	for _, subscription := range []*testSubscription{tasksSubscription, projectsSubscription} {
		batch, err := subscription.Queue.Next(context.Background())
		if err != nil || batch.Revision != 7 || batch.PreviousGroupRevision != 0 {
			t.Fatalf("grouped batch = %+v, err=%v", batch, err)
		}
		if batch.SubscriptionID != subscription.ID {
			t.Fatal("wrong contribution", batch.SubscriptionID)
		}
	}
}

func TestSubscriptionHubLinksOnlyRelevantGroupDeliveries(t *testing.T) {
	hub := newTestSubscriptionHub()
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	subscription, _, _ := hub.subscribeGrouped("tasks-sub", "connection-1", plan, bounds, nil, 4)
	one, two := hubEntity(plan, "1", true), hubEntity(plan, "2", true)
	hub.publishRevisionDetailed(hubCheckpoint(20), true, 7, []changeprocessor.QueryChange{{Query: "unrelated", Kind: changeprocessor.QueryAdded, Key: one.Key, Record: one.Record}})
	hub.publishRevisionDetailed(hubCheckpoint(21), true, 8, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: one.Key, Record: one.Record}})
	hub.publishRevisionDetailed(hubCheckpoint(22), true, 12, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: two.Key, Record: two.Record}})
	first, err := subscription.Queue.Next(context.Background())
	if err != nil || first.Revision != 7 || first.PreviousGroupRevision != 0 || len(first.Events) != 0 {
		t.Fatalf("first group delivery = %+v, err=%v", first, err)
	}
	second, err := subscription.Queue.Next(context.Background())
	if err != nil || second.Revision != 8 || second.PreviousGroupRevision != 7 {
		t.Fatalf("second group delivery = %+v, err=%v", second, err)
	}
	third, err := subscription.Queue.Next(context.Background())
	if err != nil || third.Revision != 12 || third.PreviousGroupRevision != 8 {
		t.Fatalf("third group delivery = %+v, err=%v", third, err)
	}
}

func TestSubscriptionHubQueuesWindowAfterOlderCDC(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	hub := newTestSubscriptionHub()
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, _ := hub.subscribe("viewer", plan, all, nil, 4)
	entity := hubEntity(plan, "1", true)
	hub.PublishRevision(hubCheckpoint(1), true, 1, []changeprocessor.QueryChange{{Query: "tasks", Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record}})
	empty := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryStart}}
	if _, err := hub.QueueWindowTransition("viewer", empty, WindowMembershipDelta{Removed: []storage.EntityKey{entity.Key}}, "request-1", 2, nil, nil); err != nil {
		t.Fatal(err)
	}
	first, err := subscription.Queue.Next(context.Background())
	if err != nil || first.Window != nil || first.Revision != 1 || len(first.Events) != 1 || first.Events[0].Kind != changeprocessor.QueryAdded {
		t.Fatalf("first queued item = %+v, err=%v", first, err)
	}
	second, err := subscription.Queue.Next(context.Background())
	if err != nil || second.Window == nil || second.Window.Token != "request-1" || second.Revision != 2 || len(second.Events) != 1 || second.Events[0].Kind != changeprocessor.QueryRemoved {
		t.Fatalf("window queued item = %+v, err=%v", second, err)
	}
}

func TestSubscriptionHubEmitsOneProgressMarkerPerConnectionGroup(t *testing.T) {
	for _, kind := range []DeliveryKind{DeliveryCommit, DeliveryProgress} {
		hub := newTestSubscriptionHub()
		plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
		bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
		first, _, err := hub.subscribeGrouped("first", "connection", plan, bounds, nil, 4)
		if err != nil {
			t.Fatal(err)
		}
		second, _, err := hub.subscribeGrouped("second", "connection", plan, bounds, nil, 4)
		if err != nil {
			t.Fatal(err)
		}
		hub.PublishDelivery(hubCheckpoint(9), true, 9, kind, nil)
		firstCount := first.Queue.queued()
		secondCount := second.Queue.queued()
		if firstCount+secondCount != 1 {
			t.Fatalf("kind %d marker counts = first:%d second:%d", kind, firstCount, secondCount)
		}
		marked := first
		if secondCount == 1 {
			marked = second
		}
		marker, err := marked.Queue.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if marker.Delivery != kind || marker.Revision != 9 || marker.Checkpoint.Order() != 9 || len(marker.Events) != 0 {
			t.Fatalf("kind %d marker = %+v", kind, marker)
		}
	}
}

func TestSubscriptionHubReportsProgressOnlyToRequestedGroup(t *testing.T) {
	hub := newTestSubscriptionHub()
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	requested, _, err := hub.subscribeGrouped("requested", "connection-1", plan, bounds, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, _, err := hub.subscribeGrouped("unrelated", "connection-2", plan, bounds, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	marker := hub.CaptureGroupProgress("connection-1", hubCheckpoint(9), 12)
	if marker.Revision != 12 || marker.Checkpoint.Order() != 9 || marker.PreviousGroupRevision != 0 {
		t.Fatalf("progress report = %+v", marker)
	}
	if queued := unrelated.Queue.queued(); queued != 0 {
		t.Fatalf("unrelated group received %d progress reports", queued)
	}

	if requested.Queue.queued() != 0 {
		t.Fatal("progress report was routed through a subscription queue")
	}
	repeated := hub.CaptureGroupProgress("connection-1", hubCheckpoint(9), 13)
	if repeated.Revision != 13 || repeated.PreviousGroupRevision != 12 || repeated.Checkpoint.Order() != 9 {
		t.Fatalf("repeated progress report = %+v", repeated)
	}
	hub.PublishDelivery(hubCheckpoint(10), true, 14, DeliveryCommit, nil)
	next, err := requested.Queue.Next(context.Background())
	if err != nil || next.PreviousGroupRevision != 13 {
		t.Fatalf("transaction after progress = %+v, %v", next, err)
	}
}

func TestSubscriptionHubCarriesHydrationFence(t *testing.T) {
	hub := newTestSubscriptionHub()
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, err := hub.subscribeGrouped("tasks", "connection", plan, bounds, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	entity := hubEntity(plan, "1", true)
	hub.PublishHydration(hubFence(4), hubCheckpoint(4), 7, []changeprocessor.QueryChange{{Query: plan.fingerprint, Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record}})
	batch, err := subscription.Queue.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch.Delivery != DeliveryHydration || batch.Fence == nil || batch.Fence.Source() != "test" || batch.Checkpoint == nil || batch.Checkpoint.StableID() != "4" {
		t.Fatalf("hydration batch = %+v", batch)
	}
}

func TestSubscriptionHubOrdersWindowBetweenConnectionDeliveries(t *testing.T) {
	hub := newTestSubscriptionHub()
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	subscription, _, err := hub.subscribeGrouped("tasks", "connection", plan, bounds, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	entity := hubEntity(plan, "1", true)
	hub.PublishRevision(hubCheckpoint(1), true, 1, []changeprocessor.QueryChange{{Query: plan.fingerprint, Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record}})
	first, err := subscription.Queue.Next(context.Background())
	if err != nil || first.Revision != 1 {
		t.Fatalf("first delivery = %+v, err=%v", first, err)
	}
	if _, err := hub.QueueWindowTransition("tasks", bounds, WindowMembershipDelta{}, "window", 2, hubFence(2), hubCheckpoint(2)); err != nil {
		t.Fatal(err)
	}
	window, err := subscription.Queue.Next(context.Background())
	if err != nil || window.Delivery != DeliveryWindow || window.PreviousGroupRevision != 1 || window.SubscriptionID != "tasks" || window.Window == nil || window.Window.Checkpoint.StableID() != "2" {
		t.Fatalf("window delivery = %+v, err=%v", window, err)
	}
	hub.PublishRevision(hubCheckpoint(3), true, 3, nil)
	next, err := subscription.Queue.Next(context.Background())
	if err != nil || next.Revision != 3 || next.PreviousGroupRevision != 2 {
		t.Fatalf("delivery after window = %+v, err=%v", next, err)
	}
}
