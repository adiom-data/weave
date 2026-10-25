package bench

import (
	"testing"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/protobuf/proto"
)

func projectionFixture() *reactivev1.ModelBatch {
	integer := func(value string) *reactivev1.Value {
		return &reactivev1.Value{Kind: &reactivev1.Value_IntegerValue{IntegerValue: value}}
	}
	return &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_SNAPSHOT,
		Entities: []*reactivev1.EntityEvent{{EntityId: "task-1", Event: &reactivev1.EntityEvent_Upsert{Upsert: &reactivev1.EntityUpsert{Values: map[string]*reactivev1.Value{
			"id": integer("1"), "project_id": integer("1"), "revision": integer("9007199254740993"), "payload": {Kind: &reactivev1.Value_TextValue{TextValue: "x"}},
		}}}}},
		Queries: []*reactivev1.QueryEvent{{Event: &reactivev1.QueryEvent_Reset_{Reset_: &reactivev1.QueryReset{
			Left: &reactivev1.Boundary{Value: &reactivev1.Boundary_Sentinel_{Sentinel: reactivev1.Boundary_START}}, Right: &reactivev1.Boundary{Value: &reactivev1.Boundary_Sentinel_{Sentinel: reactivev1.Boundary_END}}, EntityIds: []string{"task-1"},
		}}}},
	}
}

func TestProjectionAdapterPreservesIntegersAndReplacesRows(t *testing.T) {
	image := projectionImage{rows: make(map[string]Task), count: 1}
	batch := projectionFixture()
	if _, err := image.apply(batch); err != nil {
		t.Fatal(err)
	}
	if image.rows["task-1"].Revision != 9007199254740993 {
		t.Fatal("integer precision lost")
	}
	image.ready = true
	update := proto.Clone(batch).(*reactivev1.ModelBatch)
	update.Kind, update.Queries = reactivev1.ModelBatch_COMMIT, nil
	update.Entities[0].GetUpsert().Values["revision"] = &reactivev1.Value{Kind: &reactivev1.Value_IntegerValue{IntegerValue: "9007199254740994"}}
	changes, err := image.apply(update)
	if err != nil || len(changes) != 1 || changes[0].Task.Revision != 9007199254740994 {
		t.Fatalf("update: %v %v", changes, err)
	}
	delete(update.Entities[0].GetUpsert().Values, "payload")
	if _, err := image.apply(update); err == nil {
		t.Fatal("missing authoritative field was silently merged")
	}
	for _, changeID := range []bool{false, true} {
		update = projectionFixture()
		update.Kind, update.Queries = reactivev1.ModelBatch_COMMIT, nil
		if changeID {
			update.Entities[0].GetUpsert().Values["id"] = &reactivev1.Value{Kind: &reactivev1.Value_IntegerValue{IntegerValue: "2"}}
		} else {
			update.Entities[0].EntityId = "unknown"
		}
		if _, err := image.apply(update); err == nil {
			t.Fatal("broken normalized identity was hidden from the oracle")
		}
	}
}

func TestProjectionAdapterRejectsIncompleteOrUnexpectedPublications(t *testing.T) {
	for _, name := range []string{"missing-query", "bounded-window", "missing-entity", "unknown-reference", "duplicate-reference", "wrong-type", "pending", "early-commit"} {
		t.Run(name, func(t *testing.T) {
			batch := projectionFixture()
			image := projectionImage{rows: make(map[string]Task), count: 1}
			switch name {
			case "missing-query":
				batch.Queries = nil
			case "bounded-window":
				batch.Queries[0].GetReset_().Right = nil
			case "missing-entity":
				batch.Entities = nil
			case "unknown-reference":
				batch.Queries[0].GetReset_().EntityIds[0] = "missing"
			case "duplicate-reference":
				image.count = 2
				batch.Queries[0].GetReset_().EntityIds = []string{"task-1", "task-1"}
				other := proto.Clone(batch.Entities[0]).(*reactivev1.EntityEvent)
				other.EntityId = "task-2"
				batch.Entities = append(batch.Entities, other)
			case "wrong-type":
				batch.Entities[0].GetUpsert().Values["id"] = &reactivev1.Value{Kind: &reactivev1.Value_TextValue{TextValue: "1"}}
			case "pending":
				batch.Entities[0].Event = &reactivev1.EntityEvent_Pending{Pending: &reactivev1.EntityPending{}}
			case "early-commit":
				batch.Kind = reactivev1.ModelBatch_COMMIT
			}
			if _, err := image.apply(batch); err == nil {
				t.Fatal("invalid publication accepted")
			}
		})
	}
	image := projectionImage{rows: make(map[string]Task), count: 1}
	batch := projectionFixture()
	if _, err := image.apply(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := image.apply(batch); err == nil {
		t.Fatal("resnapshot silently accepted")
	}
}

func TestProjectionTopologyModesUseTheSameRequest(t *testing.T) {
	for _, backend := range []string{"weave-embedded", "weave-remote"} {
		configuration := testConfig()
		configuration.Backend = backend
		if err := configuration.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	request := projectionRequest(123, 100).GetSubscribeProjection()
	list := request.Root.GetList()
	if !request.WaitForSync || list.Window.Count != 101 || list.Query.Filter.GetComparison().Value.GetIntegerValue() != "123" || len(list.Query.Columns) != 4 {
		t.Fatal("incorrect projection workload")
	}
}
