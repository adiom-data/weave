package projectionserver

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// Used by service tests as the prepared-query side of the local adapter. The
// connection model tests below use only the opaque querysubscription contract.
type modelTestPlan struct{ relation storage.RelationKey }

type testCheckpoint string

func (testCheckpoint) StorageCheckpoint()       {}
func (testCheckpoint) Source() storage.SourceID { return "test" }
func (p testCheckpoint) StableID() string       { return string(p) }
func (p testCheckpoint) Order() uint64 {
	value, _ := strconv.ParseUint(string(p), 10, 64)
	return value
}

func testDelivery(revision uint64) querysubscription.DeliveryMetadata {
	previous := uint64(0)
	if revision > 1 {
		previous = revision - 1
	}
	return querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision}, Revision: revision, PreviousRevision: previous}
}

func (p modelTestPlan) Fingerprint() datastore.QueryFingerprint { return "model-test" }
func (p modelTestPlan) Relation() storage.RelationKey           { return p.relation }
func (p modelTestPlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return modelKey(p.relation, string(record["id"].Data)), nil
}
func (modelTestPlan) Matches(storage.Record) (bool, error) { return true, nil }
func (modelTestPlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append(append([]byte(nil), record["position"].Data...), record["id"].Data...)), nil
}
func (modelTestPlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (modelTestPlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (modelTestPlan) DecodeKey(value []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), value...)), nil
}
func (modelTestPlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

func modelKey(relation storage.RelationKey, id string) storage.EntityKey {
	return storage.EntityKey{Relation: relation, Value: id}
}

func modelRecord(id, position, title string) storage.Record {
	return storage.Record{
		"id":       {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
		"position": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(position)},
		"title":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
	}
}

func modelQueryBounds() querysubscription.Bounds {
	return querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}}
}

func modelSnapshot(rows ...querysubscription.Row) querysubscription.QuerySnapshot {
	wrapped := make([]querysubscription.QueryRow, len(rows))
	for i, row := range rows {
		wrapped[i] = querysubscription.QueryRow{Row: row}
	}
	return querysubscription.QuerySnapshot{Bounds: modelQueryBounds(), Rows: wrapped}
}

func TestQueryBoundaryChangesRejectsDuplicateSides(t *testing.T) {
	_, err := queryBoundaryChanges(&reactivev1.UpdateWindow{Updates: []*reactivev1.BoundaryUpdate{
		{Side: reactivev1.BoundaryUpdate_LEFT, Operation: reactivev1.BoundaryUpdate_EXTEND, Count: 1},
		{Side: reactivev1.BoundaryUpdate_LEFT, Operation: reactivev1.BoundaryUpdate_CONTRACT, Count: 1},
	}})
	if err == nil {
		t.Fatal("expected duplicate side rejection")
	}
}

func TestConnectionModelSharesEntityAcrossOpaqueQueries(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	key := modelKey(relation, "one")
	row := querysubscription.Row{Key: key, Record: modelRecord("one", "1", "old")}
	model := newConnectionModel()
	first, err := model.openSubscription("a", "qa", "", modelSnapshot(row), nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := model.openSubscription("b", "qb", "", modelSnapshot(row), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Entities) != 1 || len(second.Entities) != 0 {
		t.Fatalf("snapshot payloads: %d %d", len(first.Entities), len(second.Entities))
	}
	change := querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("one", "1", "new"), ChangeIndex: 1}}}}
	a, err := model.applySubscriptionBatch(nil, "qa", testDelivery(1), change)
	if err != nil {
		t.Fatal(err)
	}
	b, err := model.applySubscriptionBatch(nil, "qb", testDelivery(1), change)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Entities) != 1 || len(b.Entities) != 0 {
		t.Fatalf("duplicate update: %d %d", len(a.Entities), len(b.Entities))
	}
}

func TestConnectionModelConsumesExactMembershipDelta(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	a, b, c := modelKey(relation, "a"), modelKey(relation, "b"), modelKey(relation, "c")
	model := newConnectionModel()
	opened, err := model.openSubscription("scope", "query", "", modelSnapshot(
		querysubscription.Row{Key: a, Record: modelRecord("a", "1", "a")},
		querysubscription.Row{Key: b, Record: modelRecord("b", "2", "b")},
		querysubscription.Row{Key: c, Record: modelRecord("c", "3", "c")}), nil)
	if err != nil || opened.Queries[0].GetReset_().Version != 1 {
		t.Fatalf("open=%+v err=%v", opened, err)
	}
	batch, err := model.applySubscriptionBatch(nil, "query", querysubscription.DeliveryMetadata{}, querysubscription.Batch{
		Changes:    []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: b, Record: modelRecord("b", "4", "b")}}},
		Membership: querysubscription.MembershipDelta{Removed: []storage.EntityKey{b}, Insertions: []querysubscription.Insertion{{Key: b, FinalIndex: 2}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	delta := batch.Queries[0].GetDelta()
	if delta == nil || delta.BaseVersion != 1 || delta.ResultVersion != 2 || len(delta.RemovedEntityIds) != 1 || delta.Insertions[0].FinalIndex != 2 {
		t.Fatalf("delta=%+v", delta)
	}
}

func TestConnectionModelPayloadUpdateDoesNotChangeMembership(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	key := modelKey(relation, "one")
	model := newConnectionModel()
	_, _ = model.openSubscription("scope", "query", "", modelSnapshot(querysubscription.Row{Key: key, Record: modelRecord("one", "1", "old")}), nil)
	batch, err := model.applySubscriptionBatch(nil, "query", testDelivery(1), querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("one", "1", "new"), ChangeIndex: 1}}}})
	if err != nil || len(batch.Entities) != 1 || len(batch.Queries) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestConnectionModelTruncateIsReset(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	a, c := modelKey(relation, "a"), modelKey(relation, "c")
	model := newConnectionModel()
	_, _ = model.openSubscription("scope", "query", "", modelSnapshot(querysubscription.Row{Key: a, Record: modelRecord("a", "1", "a")}), nil)
	batch, err := model.applySubscriptionBatch(nil, "query", querysubscription.DeliveryMetadata{}, querysubscription.Batch{Clear: true,
		Changes:    []querysubscription.Change{{Kind: querysubscription.Added, Row: querysubscription.Row{Key: c, Record: modelRecord("c", "3", "c")}}},
		Membership: querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: c, FinalIndex: 0}}}})
	if err != nil {
		t.Fatal(err)
	}
	reset := batch.Queries[0].GetReset_()
	if reset == nil || len(reset.EntityIds) != 1 || reset.EntityIds[0] != publicEntityID(c) {
		t.Fatalf("reset=%+v", reset)
	}
}

func TestConnectionModelPendingThenHydrated(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	key := modelKey(relation, "task")
	model := newConnectionModel()
	_, _ = model.openSubscription("scope", "query", "", modelSnapshot(), nil)
	pending, err := model.applySubscriptionBatch(nil, "query", testDelivery(1), querysubscription.Batch{
		Changes:    []querysubscription.Change{{Kind: querysubscription.Added, Row: querysubscription.Row{Key: key, Record: modelRecord("task", "1", ""), Pending: true, ChangeIndex: 1}}},
		Membership: querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: key, FinalIndex: 0}}}})
	if err != nil || len(pending.Entities) != 1 || pending.Entities[0].GetPending() == nil {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	hydrated, err := model.applySubscriptionBatch(nil, "query", testDelivery(2), querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("task", "1", "ready"), ChangeIndex: 1}}}})
	if err != nil || len(hydrated.Entities) != 1 || hydrated.Entities[0].GetUpsert() == nil || len(hydrated.Queries) != 0 {
		t.Fatalf("hydrated=%+v err=%v", hydrated, err)
	}
}

func TestConnectionModelDiamondSharesEntityPayload(t *testing.T) {
	relation := storage.RelationKey{Source: "postgres", Name: "tasks"}
	key := modelKey(relation, "shared")
	model := newConnectionModel()
	batch, err := model.applyProjection("p", proj.ProjectionBatch{Events: []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, Node: "task", EntityNode: true, InstanceID: "left"},
		{Type: proj.ProjectionRowChanged, Node: "task", EntityNode: true, InstanceID: "left", Kind: querysubscription.Added, Key: key, Record: modelRecord("shared", "1", "task")},
		{Type: proj.ProjectionInstanceOpened, Node: "task", EntityNode: true, InstanceID: "right"},
		{Type: proj.ProjectionRowChanged, Node: "task", EntityNode: true, InstanceID: "right", Kind: querysubscription.Added, Key: key, Record: modelRecord("shared", "1", "task")},
	}})
	if err != nil || len(batch.Entities) != 1 || len(batch.Projections) != 2 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestCoalesceEntityEventsKeepsFinalState(t *testing.T) {
	batch := &reactivev1.ModelBatch{Entities: []*reactivev1.EntityEvent{
		{EntityId: "entity", Event: &reactivev1.EntityEvent_Released{Released: &reactivev1.EntityReleased{}}},
		{EntityId: "entity", Event: &reactivev1.EntityEvent_Upsert{Upsert: &reactivev1.EntityUpsert{}}},
	}}
	coalesceEntityEvents(batch)
	if len(batch.Entities) != 1 || batch.Entities[0].GetUpsert() == nil {
		t.Fatalf("events=%+v", batch.Entities)
	}
}
