package projectionserver

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestSelectionViewBorrowsValuesButPublicationOwnsThem(t *testing.T) {
	object := func(fields storage.Record) storage.Value {
		return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: fields}
	}
	text := func(s string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(s)}
	}
	record := storage.Record{"title": text("Title"), "properties": object(storage.Record{"owner": object(storage.Record{"id": text("owner"), "name": text("Name")}), "labels": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{text("label")}}, "odd.name": object(storage.Record{"value": text("Value")})})}
	for _, test := range []struct {
		name  string
		paths []storage.FieldPath
	}{
		{"all", nil},
		{"scalar", []storage.FieldPath{storage.MustFieldPath("title")}},
		{"whole_root_and_descendant", []storage.FieldPath{storage.MustFieldPath("properties"), storage.MustFieldPath("properties", "owner", "id")}},
		{"nested_ancestor_and_sibling", []storage.FieldPath{storage.MustFieldPath("properties", "owner"), storage.MustFieldPath("properties", "owner", "id"), storage.MustFieldPath("properties", "labels")}},
		{"nested_leaves_and_literal_dot", []storage.FieldPath{storage.MustFieldPath("properties", "owner", "id"), storage.MustFieldPath("properties", "owner", "name"), storage.MustFieldPath("properties", "odd.name", "value")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			paths := test.paths
			model := newConnectionModel()
			model.addSelection("one", selectColumns(paths), 1)
			want := make(storage.Record)
			if paths == nil {
				want = storage.CloneRecord(record)
			} else {
				for _, path := range paths {
					if value, ok := storage.Lookup(record, path); ok {
						if err := storage.SetPath(want, path, value); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			before := storage.CloneRecord(record)
			for attempt := 0; attempt < 20; attempt++ {
				view := model.selectRecordView("one", record)
				if !reflect.DeepEqual(view, want) || !reflect.DeepEqual(record, before) {
					t.Fatal("selection mutated source or changed overlapping-path semantics")
				}
				for _, path := range paths {
					a, ok := storage.Lookup(record, path)
					b, selected := storage.Lookup(view, path)
					if ok && selected && len(a.Data) > 0 && &a.Data[0] != &b.Data[0] {
						t.Fatal("selection eagerly copied a scalar")
					}
				}
			}
			entity := &modelEntity{}
			entity.accept(querysubscription.RecordVersion{Revision: 1}, record, false)
			publication, err := entity.prepare("one", func(image storage.Record) storage.Record { return model.selectRecordView("one", image) })
			if err != nil {
				t.Fatal(err)
			}
			if publication == nil || !reflect.DeepEqual(publication.record, want) {
				t.Fatalf("publication=%+v", publication)
			}
			// Even nested data borrowed during comparison must be owned by the
			// publication before it can outlive or be separated from the source.
			entity.record["title"].Data[0] = 'X'
			entity.record["properties"].Object["owner"].Object["id"].Data[0] = 'X'
			if !reflect.DeepEqual(publication.record, want) {
				t.Fatal("publication aliases accepted source values")
			}
		})
	}
}

func applyEntityImage(t *testing.T, model *connectionModel, scope string, version uint64, record storage.Record, pending, open bool, selected ...storage.FieldPath) *reactivev1.ModelBatch {
	t.Helper()
	key := modelKey(snapshotTestRelation(), "shared")
	row := projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: scope, Node: scope, EntityNode: true, Kind: querysubscription.Updated, Key: key, Record: record, Pending: pending, Version: querysubscription.RecordVersion{Revision: version}}
	events := []projection.ProjectionEvent{row}
	if open {
		events = append([]projection.ProjectionEvent{{Type: projection.ProjectionInstanceOpened, InstanceID: scope, Node: scope, EntityNode: true}}, events...)
	}
	batch, err := model.applyProjection(scope, projection.ProjectionBatch{Events: events}, map[string]*fieldSelection{scope: selectColumns(selected)})
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestModelEntityUnchangedHydrationPublishesCompleteness(t *testing.T) {
	for _, hydrationVersion := range []uint64{2, 3} {
		model := newConnectionModel()
		record := modelRecord("shared", "1", "unchanged")
		applyEntityImage(t, model, "scope", 1, record, false, true)
		pending := applyEntityImage(t, model, "scope", 2, nil, true, false)
		if len(pending.Entities) != 1 || pending.Entities[0].GetPending() == nil {
			t.Fatalf("pending output = %v", pending)
		}
		hydrated := applyEntityImage(t, model, "scope", hydrationVersion, record, false, false)
		if len(hydrated.Entities) != 1 || hydrated.Entities[0].GetUpsert() == nil {
			t.Fatalf("hydration at version %d did not resolve equal payload: %v", hydrationVersion, hydrated)
		}
		if repeated := applyEntityImage(t, model, "scope", hydrationVersion, record, false, false); len(repeated.Entities) != 0 {
			t.Fatalf("duplicate hydration was emitted: %v", repeated)
		}
		if stale := applyEntityImage(t, model, "scope", 1, nil, true, false); len(stale.Entities) != 0 {
			t.Fatalf("older pending image replaced hydrated state: %v", stale)
		}
	}
}

func TestModelEntitySelectionChangesDoNotChangeCompleteness(t *testing.T) {
	model := newConnectionModel()
	record := modelRecord("shared", "1", "current")
	title, position := storage.MustFieldPath("title"), storage.MustFieldPath("position")
	applyEntityImage(t, model, "title", 4, record, false, true, title)
	// A slow projection introduces a new selection with an older pending image.
	// Its reference is retained, but only accepted current state can supply data.
	expanded := applyEntityImage(t, model, "position", 2, nil, true, true, position)
	if len(expanded.Entities) != 1 || expanded.Entities[0].GetUpsert() == nil || expanded.Entities[0].GetUpsert().Values["position"] == nil || expanded.Entities[0].GetUpsert().Values["title"] == nil {
		t.Fatalf("older pending reference did not expand current selection: %v", expanded)
	}
	key := modelKey(snapshotTestRelation(), "shared")
	entity := model.entities[publicEntityID(key)]
	if entity.version.Revision != 4 || entity.pending {
		t.Fatalf("selection changed accepted source state: %+v", entity)
	}
	applyEntityImage(t, model, "title", 5, nil, true, false, title)
	if entity.record != nil {
		t.Fatal("pending source retained stale bytes as a hydration fallback")
	}
	shrunk, err := model.closeScope("position")
	if err != nil {
		t.Fatal(err)
	}
	if len(shrunk.Entities) != 0 || !entity.pending || entity.output != entityPending {
		t.Fatalf("selection shrink fabricated hydration: %v state=%+v", shrunk, entity)
	}
	hydrated := applyEntityImage(t, model, "title", 6, record, false, false, title)
	if len(hydrated.Entities) != 1 || hydrated.Entities[0].GetUpsert() == nil {
		t.Fatalf("final hydration = %v", hydrated)
	}
	values := hydrated.Entities[0].GetUpsert().Values
	if len(values) != 1 || values["title"] == nil {
		t.Fatalf("hydration did not use final selection: %v", values)
	}
}

func TestModelEntityBatchPublishesOnlyFinalCompletenessAndSelection(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		model := newConnectionModel()
		record := modelRecord("shared", "1", "same")
		title, position := storage.MustFieldPath("title"), storage.MustFieldPath("position")
		applyEntityImage(t, model, "title", 1, record, false, true, title)
		applyEntityImage(t, model, "position", 1, record, false, true, position)
		batch := &reactivev1.ModelBatch{}
		if err := model.assembleBatch(batch, func() error {
			appendModelBatch(batch, applyEntityImage(t, model, "title", 2, nil, true, false, title))
			if shrink {
				closed, err := model.closeScope("position")
				if err != nil {
					return err
				}
				appendModelBatch(batch, closed)
			}
			appendModelBatch(batch, applyEntityImage(t, model, "title", 3, record, false, false, title))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		expected := 0
		if shrink {
			expected = 1
		}
		if len(batch.Entities) != expected {
			t.Fatalf("intermediate completeness escaped the batch: %v", batch)
		}
		if shrink && (batch.Entities[0].GetUpsert() == nil || len(batch.Entities[0].GetUpsert().Values) != 1) {
			t.Fatalf("batch did not publish final field selection: %v", batch)
		}
		if model.batchDepth != 0 || len(model.dirtyEntities) != 0 {
			t.Fatal("publication work leaked past the batch")
		}
	}
}

func TestModelEntityPublicationErrorIsReturnedAndCleanupDoesNotReleaseUnpublishedEntity(t *testing.T) {
	model := newConnectionModel()
	key := modelKey(snapshotTestRelation(), "shared")
	_, err := model.applyProjection("scope", projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		{Type: projection.ProjectionInstanceOpened, InstanceID: "scope", EntityNode: true},
		{Type: projection.ProjectionRowChanged, InstanceID: "scope", EntityNode: true, Kind: querysubscription.Added, Key: key, Version: querysubscription.RecordVersion{Revision: 1}, Record: storage.Record{"bad": {Type: "unsupported", State: storage.ValuePresent}}},
	}})
	if err == nil {
		t.Fatal("publication discarded its conversion error")
	}
	if model.batchDepth != 0 || len(model.dirtyEntities) == 0 {
		t.Fatal("failed publication did not leave deferred state for cleanup")
	}
	cleanup, err := model.closeScope("scope")
	if err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Entities) != 0 || len(model.entities) != 0 || len(model.dirtyEntities) != 0 {
		t.Fatalf("cleanup released or retained an unpublished entity: %v", cleanup)
	}
}

func TestModelEntityFailedBatchDoesNotCommitPartialPublicOutput(t *testing.T) {
	model := newConnectionModel()
	applyEntityImage(t, model, "healthy", 1, modelRecord("shared", "1", "before"), false, true)
	batch := &reactivev1.ModelBatch{}
	err := model.assembleBatch(batch, func() error {
		appendModelBatch(batch, applyEntityImage(t, model, "healthy", 2, modelRecord("shared", "1", "after"), false, false))
		badKey := modelKey(snapshotTestRelation(), "bad")
		_, err := model.applyProjection("bad", projection.ProjectionBatch{Events: []projection.ProjectionEvent{
			{Type: projection.ProjectionInstanceOpened, InstanceID: "bad", EntityNode: true},
			{Type: projection.ProjectionRowChanged, InstanceID: "bad", EntityNode: true, Kind: querysubscription.Added, Key: badKey, Version: querysubscription.RecordVersion{Revision: 2}, Record: storage.Record{"bad": {Type: "unsupported", State: storage.ValuePresent}}},
		}})
		return err
	})
	if err == nil || len(batch.Entities) != 0 {
		t.Fatalf("failed batch published a partial entity output: %v err=%v", batch, err)
	}
	entity := model.entities[publicEntityID(modelKey(snapshotTestRelation(), "shared"))]
	if string(entity.outputRecord["title"].Data) != "before" {
		t.Fatal("failed batch advanced a healthy entity's public output")
	}
	cleanup, err := model.closeScope("bad")
	if err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Entities) != 1 || cleanup.Entities[0].GetUpsert() == nil || cleanup.Entities[0].GetUpsert().Values["title"].GetTextValue() != "after" {
		t.Fatalf("scope cleanup lost deferred healthy output: %v", cleanup)
	}
}
