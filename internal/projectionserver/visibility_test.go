package projectionserver

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/membership"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
)

func TestRemovedProjectionRowOwnsMetadataWithoutPayload(t *testing.T) {
	event := projection.ProjectionEvent{Type: projection.ProjectionRowChanged, Node: "tasks", InstanceID: "tasks", Kind: querysubscription.Updated, Path: []string{"project", "tasks"}, Lineage: []projection.PathStep{{Node: "project"}}, Record: modelRecord("one", "1", "Task"), Cursor: []byte("cursor"), Pending: true, Version: querysubscription.RecordVersion{Revision: 7}}
	removed := removedProjectionRow(event)
	if removed.Kind != querysubscription.Removed || removed.Record != nil || removed.Cursor != nil || removed.Pending || removed.Version != event.Version {
		t.Fatalf("removal=%+v", removed)
	}
	removed.Path[0] = "changed"
	removed.Lineage[0].Node = "changed"
	if event.Path[0] != "project" || event.Lineage[0].Node != "project" || event.Record == nil || string(event.Cursor) != "cursor" {
		t.Fatal("removal modified source event")
	}
}

type visibilityTestFence struct{}

func (visibilityTestFence) StorageFence()            {}
func (visibilityTestFence) Source() storage.SourceID { return "test" }
func (visibilityTestFence) StableID() string         { return "visibility-test" }
func (visibilityTestFence) Order() uint64            { return 1 }

type visibilityTestView struct{}

func (visibilityTestView) Close() error { return nil }

type visibilityPointView struct{ checkpoint bool }

func (visibilityPointView) Close() error { return nil }

type visibilityTestPrepared struct {
	decide func(accesscontrol.Candidate) bool
}

func (visibilityTestPrepared) ID() accesscontrol.PreparedID { return "test" }
func (p visibilityTestPrepared) Evaluate(_ context.Context, _ accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	result := make([]bool, len(candidates))
	for index, candidate := range candidates {
		result[index] = p.decide(candidate)
	}
	return result, nil
}
func (visibilityTestPrepared) Close() error                                               { return nil }
func (visibilityTestPrepared) Release(context.Context, string, []storage.EntityKey) error { return nil }

type selectiveVisibilityErrorPrepared struct{}

func (selectiveVisibilityErrorPrepared) ID() accesscontrol.PreparedID { return "selective-error" }
func (selectiveVisibilityErrorPrepared) Evaluate(_ context.Context, _ accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	for _, candidate := range candidates {
		if candidate.Key.Value == "bad" {
			return nil, errors.New("policy lookup failed")
		}
	}
	visible := make([]bool, len(candidates))
	for index := range visible {
		visible[index] = true
	}
	return visible, nil
}
func (selectiveVisibilityErrorPrepared) Close() error { return nil }
func (selectiveVisibilityErrorPrepared) Release(context.Context, string, []storage.EntityKey) error {
	return nil
}

type visibilityTestController struct{}

func (visibilityTestController) Prepare(context.Context, accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	return visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}, nil
}
func (visibilityTestController) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	return accesscontrol.Checkpoint{Position: position, View: visibilityTestView{}}, nil
}
func (visibilityTestController) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: visibilityTestView{}}, nil
}
func (visibilityTestController) Close() error { return nil }

func visibilityRecord(visible bool) storage.Record {
	value := "false"
	if visible {
		value = "true"
	}
	return storage.Record{"visible": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}}
}

func TestProjectionAuthorizationPathIsOrderedAndDirectional(t *testing.T) {
	project := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p1"}
	link := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "project_tasks"}, Value: "pt1"}
	task := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "t1"}
	path := newPolicyCapture(newProjectionVisibility(), policyOverlay{}).candidate(projection.ProjectionEvent{
		Node: "tasks", Key: task,
		Lineage: []projection.PathStep{{Node: "projects", Key: project}, {Node: "project_tasks", Key: link}},
	}).Context.Projection
	var keys []storage.EntityKey
	for step := range path.Steps() {
		keys = append(keys, step.Key)
	}
	if len(keys) != 3 || keys[0] != project || keys[1] != link || keys[2] != task {
		t.Fatalf("authorization path = %+v", path)
	}
}

func TestVisibilityFiltersInitialSnapshot(t *testing.T) {
	controller := visibilityTestController{}
	prepared, err := controller.Prepare(context.Background(), accesscontrol.PrepareRequest{ID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	visibleKey := storage.EntityKey{Value: "visible"}
	hiddenKey := storage.EntityKey{Value: "hidden"}
	view, err := acquireVisibilityAt(context.Background(), controller, prepared, nil, sourceposition.FromSource(visibilityTestFence{}))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	filtered, err := newSubscriptionVisibility().filterSnapshot(context.Background(), prepared, view, querysubscription.QuerySnapshot{Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: visibleKey, Record: visibilityRecord(true)}}, {Row: querysubscription.Row{Key: hiddenKey, Record: visibilityRecord(false)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Rows) != 1 || filtered.Rows[0].Key != visibleKey {
		t.Fatalf("filtered snapshot = %#v", filtered)
	}
}

func TestInitialVisibilityPrefersMaterializationCheckpointOverSnapshotFence(t *testing.T) {
	controller := visibilityPointController{}
	prepared := visibilityPointPrepared{}
	key := storage.EntityKey{Value: "current"}
	view, err := acquireVisibilityAt(context.Background(), controller, prepared, sourceposition.FromSource(testCheckpoint("2")), sourceposition.FromSource(visibilityTestFence{}))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	filtered, err := newSubscriptionVisibility().filterSnapshot(context.Background(), prepared, view, querysubscription.QuerySnapshot{Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: visibilityRecord(true)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Rows) != 1 {
		t.Fatalf("checkpoint view was not used: %+v", filtered)
	}
}

type visibilityPointController struct{}

func (visibilityPointController) Prepare(context.Context, accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	return visibilityPointPrepared{}, nil
}
func (visibilityPointController) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	return accesscontrol.Checkpoint{Position: position, View: visibilityPointView{checkpoint: true}}, nil
}
func (visibilityPointController) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: visibilityPointView{}}, nil
}

type visibilityPointPrepared struct{}

func (visibilityPointPrepared) ID() accesscontrol.PreparedID { return "point" }
func (visibilityPointPrepared) Evaluate(_ context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	point, ok := view.(visibilityPointView)
	result := make([]bool, len(candidates))
	for index := range result {
		result[index] = ok && point.checkpoint
	}
	return result, nil
}
func (visibilityPointPrepared) Close() error { return nil }
func (visibilityPointPrepared) Release(context.Context, string, []storage.EntityKey) error {
	return nil
}

func TestVisibilityTurnsHiddenUpdateIntoIdempotentRemoval(t *testing.T) {
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	key := storage.EntityKey{Value: "task"}
	gate := newSubscriptionVisibility()
	if _, err := gate.filterSnapshot(context.Background(), prepared, visibilityTestView{}, querysubscription.QuerySnapshot{Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: visibilityRecord(true)}}}}); err != nil {
		t.Fatal(err)
	}
	filtered, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, querysubscription.Batch{Changes: []querysubscription.Change{{
		Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: visibilityRecord(false), ChangeIndex: 7},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Changes) != 1 || filtered.Changes[0].Kind != querysubscription.Removed || filtered.Changes[0].Key != key || filtered.Changes[0].ChangeIndex != 7 {
		t.Fatalf("filtered changes = %#v", filtered.Changes)
	}
}

func TestDirectVisibilityRetainsHiddenRawMemberForLaterRegain(t *testing.T) {
	key := storage.EntityKey{Value: "task"}
	gate := newSubscriptionVisibility()
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	initial, err := gate.filterSnapshot(context.Background(), prepared, visibilityTestView{}, querysubscription.QuerySnapshot{
		Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: visibilityRecord(false)}}},
	})
	if err != nil || len(initial.Rows) != 0 {
		t.Fatalf("hidden initial row: snapshot=%+v err=%v", initial, err)
	}
	part, err := gate.filterTransaction(context.Background(), prepared, visibilityTestView{}, subscriptionBatchMessage{
		delivery:     testDelivery(2),
		invalidation: accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: visibilityRecord(true), Complete: true}}},
	})
	filtered := part.batch
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Changes) != 1 || filtered.Changes[0].Kind != querysubscription.Added || len(filtered.Membership.Insertions) != 1 || filtered.Membership.Insertions[0].FinalIndex != 0 {
		t.Fatalf("visibility regain = %+v", filtered)
	}
}

func TestPolicyEvaluationFailureProducesScopedFailure(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	goodGate := newSubscriptionVisibility()
	badGate := newSubscriptionVisibility()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	for gate, key := range map[*subscriptionVisibility]string{goodGate: "good", badGate: "bad"} {
		if _, err := gate.filterSnapshot(context.Background(), allow, visibilityTestView{}, querysubscription.QuerySnapshot{Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{
			Key: storage.EntityKey{Relation: relation, Value: key}, Record: modelRecord(key, "1", key),
		}}}}); err != nil {
			t.Fatal(err)
		}
	}
	states := map[string]*sharedSubscription{
		"good": {externalID: "good", internalID: "good-generation", visibility: goodGate},
		"bad":  {externalID: "bad", internalID: "bad-generation", visibility: badGate},
	}
	part := func(id, generation, key string) subscriptionBatchMessage {
		return subscriptionBatchMessage{externalID: id, generation: generation, batch: querysubscription.Batch{Changes: []querysubscription.Change{{
			Kind: querysubscription.Updated, Row: querysubscription.Row{Key: storage.EntityKey{Relation: relation, Value: key}, Record: modelRecord(key, "1", key)},
		}}}}
	}
	filtered, failure, err := filterVisibilityPart(context.Background(), selectiveVisibilityErrorPrepared{}, visibilityTestView{}, part("good", "good-generation", "good"), states, nil)
	if err != nil || failure != nil || filtered == nil {
		t.Fatalf("good part: filtered=%#v failure=%#v err=%v", filtered, failure, err)
	}
	filtered, failure, err = filterVisibilityPart(context.Background(), selectiveVisibilityErrorPrepared{}, visibilityTestView{}, part("bad", "bad-generation", "bad"), states, nil)
	if err != nil || filtered != nil || failure == nil || failure.externalID != "bad" || failure.projection {
		t.Fatalf("bad part: filtered=%#v failure=%#v err=%v", filtered, failure, err)
	}
}

func TestVisibilityInvalidationReevaluatesUnchangedRawMember(t *testing.T) {
	key := storage.EntityKey{Value: "task"}
	gate := newSubscriptionVisibility()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	original := modelRecord("task", "1", "original")
	if _, err := gate.filterSnapshot(context.Background(), allow, visibilityTestView{}, querysubscription.QuerySnapshot{
		Revision: 1, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: original}}},
	}); err != nil {
		t.Fatal(err)
	}
	part, err := gate.filterTransaction(context.Background(), allow, visibilityTestView{}, subscriptionBatchMessage{
		delivery:     testDelivery(2),
		invalidation: accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("task", "1", "policy-only facts"), Complete: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.batch.Changes) != 0 || len(part.batch.Membership.Removed) != 0 || len(part.batch.Membership.Insertions) != 0 {
		t.Fatalf("unchanged decision emitted data: %+v", part)
	}
	if !reflect.DeepEqual(gate.rows[key].Record, original) || gate.rows[key].version.Revision != 1 {
		t.Fatal("authorization invalidation replaced the raw image")
	}
}

func TestVisibilityInvalidationDoesNotInsertUnknownRawMember(t *testing.T) {
	gate := newSubscriptionVisibility()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { t.Fatal("evaluated unknown row"); return true }}
	part, err := gate.filterTransaction(context.Background(), allow, visibilityTestView{}, subscriptionBatchMessage{
		delivery:     testDelivery(2),
		invalidation: accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: storage.EntityKey{Value: "unknown"}, Complete: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.batch.Changes) != 0 || len(gate.rows) != 0 {
		t.Fatalf("unknown member produced changes: %+v", part)
	}
}

func TestVisibilityLossAndRegainInOneTransactionDoesNotFlicker(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "public", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "task"}
	record := func(visible bool) storage.Record {
		result := visibilityRecord(visible)
		result["id"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("task")}
		result["position"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}
		return result
	}
	model := newConnectionModel()
	if _, err := model.openSubscription("scope", "query", "", querysubscription.QuerySnapshot{
		Bounds: querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}},
		Rows:   []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record(true)}}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	gate := newSubscriptionVisibility()
	if _, err := gate.filterSnapshot(context.Background(), prepared, visibilityTestView{}, querysubscription.QuerySnapshot{Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record(true)}}}}); err != nil {
		t.Fatal(err)
	}
	filtered, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, querysubscription.Batch{Changes: []querysubscription.Change{
		{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: record(false), ChangeIndex: 1}},
		{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: record(true), ChangeIndex: 2}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := model.applySubscriptionBatch(nil, "query", testDelivery(2), filtered)
	if err != nil {
		t.Fatal(err)
	}
	coalesceEntityEvents(batch)
	if len(batch.Entities) != 0 {
		t.Fatalf("unchanged final entity flickered: %#v", batch.Entities)
	}
	if len(batch.Queries) != 0 {
		t.Fatalf("query membership flickered: %#v", batch.Queries)
	}
}

func TestProjectionVisibilityPrunesAndRestoresOnlyAffectedDiamondPath(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "public", Name: "entities"}
	record := func(id, position string, visible bool) storage.Record {
		result := modelRecord(id, position, id)
		result["visible"] = visibilityRecord(visible)["visible"]
		return result
	}
	key := func(id string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: id} }
	rootOpen := projection.ProjectionEvent{Type: projection.ProjectionInstanceOpened, InstanceID: "root", Node: "projects"}
	p1 := projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: "root", Node: "projects", Kind: querysubscription.Added, Key: key("p1"), Record: record("p1", "1", false)}
	p2 := projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: "root", Node: "projects", Kind: querysubscription.Added, Key: key("p2"), Record: record("p2", "2", true)}
	child := func(id, parent string) projection.ProjectionEvent {
		return projection.ProjectionEvent{Type: projection.ProjectionInstanceOpened, InstanceID: id, ParentInstanceID: "root", ParentEntityKey: key(parent), Node: "tasks"}
	}
	task := func(instance string) projection.ProjectionEvent {
		return projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: instance, Node: "tasks", Kind: querysubscription.Added, Key: key("shared-task"), Record: record("shared-task", "1", true)}
	}
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	gate := newProjectionVisibility()
	initial, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		rootOpen, p1, child("p1-tasks", "p1"), task("p1-tasks"), p2, child("p2-tasks", "p2"), task("p2-tasks"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range initial.Events {
		if event.InstanceID == "p1-tasks" {
			t.Fatalf("hidden diamond path was exposed: %+v", initial.Events)
		}
	}
	if len(initial.Events) != 4 {
		t.Fatalf("initial visible events = %+v", initial.Events)
	}

	p1.Record = record("p1", "1", true)
	p1.Kind = querysubscription.Updated
	restored, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{p1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Events) != 3 || restored.Events[0].InstanceID != "root" || restored.Events[0].Kind != querysubscription.Added || restored.Events[1].InstanceID != "p1-tasks" || restored.Events[1].Type != projection.ProjectionInstanceOpened || restored.Events[2].InstanceID != "p1-tasks" || restored.Events[2].Kind != querysubscription.Added {
		t.Fatalf("restored path events = %+v", restored.Events)
	}
	for _, event := range restored.Events {
		if event.InstanceID == "p2-tasks" {
			t.Fatalf("unaffected diamond path was replayed: %+v", restored.Events)
		}
	}
	p1.Record = record("p1", "1", false)
	pruned, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{p1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned.Events) != 2 || pruned.Events[0].InstanceID != "p1-tasks" || pruned.Events[0].Type != projection.ProjectionInstanceClosed || pruned.Events[1].InstanceID != "root" || pruned.Events[1].Kind != querysubscription.Removed {
		t.Fatalf("re-pruned path events = %+v", pruned.Events)
	}
}

func TestProjectionVisibilityMaintainsIncrementalVisibleRanks(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "public", Name: "tasks"}
	key := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }
	row := func(value string, visible bool, kind querysubscription.ChangeKind) projection.ProjectionEvent {
		return projection.ProjectionEvent{
			Type: projection.ProjectionRowChanged, InstanceID: "tasks", Node: "tasks", Kind: kind,
			Key: key(value), Record: visibilityRecord(visible),
		}
	}
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	gate := newProjectionVisibility()
	initial, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		{Type: projection.ProjectionInstanceOpened, InstanceID: "tasks", Node: "tasks"},
		row("a", true, querysubscription.Added), row("b", false, querysubscription.Added), row("c", true, querysubscription.Added),
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantInitial := querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: key("a"), FinalIndex: 0}, {Key: key("c"), FinalIndex: 1}}}
	if got := initial.Membership["tasks"]; !reflect.DeepEqual(got, wantInitial) {
		t.Fatalf("initial membership = %+v, want %+v", got, wantInitial)
	}

	// Moving only an invisible occurrence cannot alter client membership.
	hiddenMove, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Membership: map[string]querysubscription.MembershipDelta{
		"tasks": {Removed: []storage.EntityKey{key("b")}, Insertions: []querysubscription.Insertion{{Key: key("b"), FinalIndex: 2}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(hiddenMove.Membership) != 0 {
		t.Fatalf("hidden move membership = %+v", hiddenMove.Membership)
	}

	visibleMove, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Membership: map[string]querysubscription.MembershipDelta{
		"tasks": {Removed: []storage.EntityKey{key("c")}, Insertions: []querysubscription.Insertion{{Key: key("c"), FinalIndex: 0}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantMove := querysubscription.MembershipDelta{Removed: []storage.EntityKey{key("c")}, Insertions: []querysubscription.Insertion{{Key: key("c"), FinalIndex: 0}}}
	if got := visibleMove.Membership["tasks"]; !reflect.DeepEqual(got, wantMove) {
		t.Fatalf("visible move membership = %+v, want %+v", got, wantMove)
	}

	becameVisible, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		row("b", true, querysubscription.Updated),
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantVisible := querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: key("b"), FinalIndex: 2}}}
	if got := becameVisible.Membership["tasks"]; !reflect.DeepEqual(got, wantVisible) {
		t.Fatalf("visibility insertion = %+v, want %+v", got, wantVisible)
	}

	// Multiple policy decisions in one transaction collapse to final state.
	noFlicker, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		row("a", false, querysubscription.Updated), row("a", true, querysubscription.Updated),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(noFlicker.Membership) != 0 {
		t.Fatalf("visibility flickered: %+v", noFlicker.Membership)
	}
}

func TestProjectionVisibilityAppliesNewInstanceMembershipOrder(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "public", Name: "tasks"}
	a := storage.EntityKey{Relation: relation, Value: "a"}
	b := storage.EntityKey{Relation: relation, Value: "b"}
	prepared := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	gate := newProjectionVisibility()
	filtered, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{
		Events: []projection.ProjectionEvent{
			{Type: projection.ProjectionInstanceOpened, InstanceID: "tasks", Node: "tasks"},
			{Type: projection.ProjectionRowChanged, InstanceID: "tasks", Node: "tasks", Kind: querysubscription.Added, Key: a, Record: visibilityRecord(true)},
			{Type: projection.ProjectionRowChanged, InstanceID: "tasks", Node: "tasks", Kind: querysubscription.Added, Key: b, Record: visibilityRecord(true)},
		},
		Membership: map[string]querysubscription.MembershipDelta{"tasks": {Insertions: []querysubscription.Insertion{
			{Key: b, FinalIndex: 0}, {Key: a, FinalIndex: 1},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: b, FinalIndex: 0}, {Key: a, FinalIndex: 1}}}
	if got := filtered.Membership["tasks"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("membership = %+v, want %+v", got, want)
	}
}

func TestProjectionVisibilityDeltasMatchVisibleSliceModel(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "public", Name: "tasks"}
	key := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }
	event := func(entity storage.EntityKey, visible bool, kind querysubscription.ChangeKind) projection.ProjectionEvent {
		result := projection.ProjectionEvent{
			Type: projection.ProjectionRowChanged, InstanceID: "tasks", Node: "tasks", Kind: kind, Key: entity,
		}
		if kind != querysubscription.Removed {
			result.Record = visibilityRecord(visible)
		}
		return result
	}
	prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	gate := newProjectionVisibility()
	rng := rand.New(rand.NewSource(19))
	allowed := make(map[storage.EntityKey]bool)
	raw := make([]storage.EntityKey, 0, 32)
	initialEvents := []projection.ProjectionEvent{{Type: projection.ProjectionInstanceOpened, InstanceID: "tasks", Node: "tasks"}}
	nextID := 0
	for range 20 {
		entity := key(strconv.Itoa(nextID))
		nextID++
		visible := rng.Intn(2) == 0
		raw = append(raw, entity)
		allowed[entity] = visible
		initialEvents = append(initialEvents, event(entity, visible, querysubscription.Added))
	}
	filtered, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: initialEvents})
	if err != nil {
		t.Fatal(err)
	}
	clientOrder, err := querysubscription.ApplyMembership(nil, filtered.Membership["tasks"])
	if err != nil {
		t.Fatal(err)
	}
	assertVisibleModelOrder(t, 0, clientOrder, raw, allowed)

	for step := 1; step <= 500; step++ {
		before := append([]storage.EntityKey(nil), raw...)
		var events []projection.ProjectionEvent

		removeCount := rng.Intn(3)
		for range removeCount {
			if len(raw) == 0 {
				break
			}
			position := rng.Intn(len(raw))
			entity := raw[position]
			raw = append(raw[:position], raw[position+1:]...)
			delete(allowed, entity)
			events = append(events, event(entity, false, querysubscription.Removed))
		}

		for range rng.Intn(3) {
			entity := key(strconv.Itoa(nextID))
			nextID++
			visible := rng.Intn(2) == 0
			position := rng.Intn(len(raw) + 1)
			raw = insertVisibilityModelKey(raw, position, entity)
			allowed[entity] = visible
			events = append(events, event(entity, visible, querysubscription.Added))
		}

		for range rng.Intn(4) {
			if len(raw) < 2 {
				break
			}
			from := rng.Intn(len(raw))
			entity := raw[from]
			raw = append(raw[:from], raw[from+1:]...)
			to := rng.Intn(len(raw) + 1)
			raw = insertVisibilityModelKey(raw, to, entity)
		}

		for range rng.Intn(4) {
			if len(raw) == 0 {
				break
			}
			entity := raw[rng.Intn(len(raw))]
			allowed[entity] = !allowed[entity]
			events = append(events, event(entity, allowed[entity], querysubscription.Updated))
		}

		rawDelta := membership.Diff(before, raw)
		batch := projection.ProjectionBatch{Events: events}
		if len(rawDelta.Removed) > 0 || len(rawDelta.Insertions) > 0 {
			batch.Membership = map[string]querysubscription.MembershipDelta{"tasks": rawDelta}
		}
		filtered, err = gate.filterBatch(context.Background(), prepared, visibilityTestView{}, batch)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if delta, changed := filtered.Membership["tasks"]; changed {
			clientOrder, err = querysubscription.ApplyMembership(clientOrder, delta)
			if err != nil {
				t.Fatalf("step %d: apply visible delta: %v", step, err)
			}
		}
		assertVisibleModelOrder(t, step, clientOrder, raw, allowed)
	}
}

func insertVisibilityModelKey(order []storage.EntityKey, position int, key storage.EntityKey) []storage.EntityKey {
	order = append(order, storage.EntityKey{})
	copy(order[position+1:], order[position:])
	order[position] = key
	return order
}

func assertVisibleModelOrder(t *testing.T, step int, got, raw []storage.EntityKey, allowed map[storage.EntityKey]bool) {
	t.Helper()
	want := make([]storage.EntityKey, 0, len(raw))
	for _, key := range raw {
		if allowed[key] {
			want = append(want, key)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("step %d: visible order = %v, want %v", step, got, want)
	}
}
