package projectionserver

import (
	"testing"

	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestModelBatchReferenceTransferPreservesEntityFreshness(t *testing.T) {
	for _, crossScope := range []bool{false, true} {
		name := "one_projection"
		if crossScope {
			name = "across_projections"
		}
		t.Run(name, func(t *testing.T) {
			model := newConnectionModel()
			key := modelKey(snapshotTestRelation(), "shared")
			open := func(id string) projection.ProjectionEvent {
				return projection.ProjectionEvent{Type: projection.ProjectionInstanceOpened, InstanceID: id, EntityNode: true}
			}
			row := func(id, title string, revision uint64) projection.ProjectionEvent {
				return projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: id, EntityNode: true, Kind: querysubscription.Added, Key: key, Record: modelRecord("shared", "1", title), Version: querysubscription.RecordVersion{Revision: revision}}
			}
			if _, err := model.applyProjection("left", projection.ProjectionBatch{Events: []projection.ProjectionEvent{open("left"), row("left", "T4", 4)}}); err != nil {
				t.Fatal(err)
			}
			if _, err := model.applyProjection("right", projection.ProjectionBatch{Events: []projection.ProjectionEvent{open("right")}}); err != nil {
				t.Fatal(err)
			}
			remove := projection.ProjectionEvent{Type: projection.ProjectionRowChanged, InstanceID: "left", EntityNode: true, Kind: querysubscription.Removed, Key: key}
			var batch *reactivev1.ModelBatch
			if crossScope {
				batch = &reactivev1.ModelBatch{}
				if err := model.assembleBatch(batch, func() error {
					part, err := model.applyProjection("left", projection.ProjectionBatch{Events: []projection.ProjectionEvent{remove}})
					if err != nil {
						return err
					}
					appendModelBatch(batch, part)
					part, err = model.applyProjection("right", projection.ProjectionBatch{Events: []projection.ProjectionEvent{row("right", "T2", 2)}})
					if err != nil {
						return err
					}
					appendModelBatch(batch, part)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				var err error
				batch, err = model.applyProjection("left", projection.ProjectionBatch{Events: []projection.ProjectionEvent{remove, row("right", "T2", 2)}})
				if err != nil {
					t.Fatal(err)
				}
			}
			id := publicEntityID(key)
			if got := string(model.entities[id].record["title"].Data); got != "T4" || model.entities[id].version.Revision != 4 {
				t.Fatalf("atomic transfer rewound entity: title=%q revision=%d", got, model.entities[id].version.Revision)
			}
			if len(batch.Entities) != 0 || model.entityRefs[id] != 1 {
				t.Fatalf("entity flickered during transfer: %v refs=%d", batch, model.entityRefs[id])
			}
			if model.batchDepth != 0 || len(model.dirtyEntities) != 0 {
				t.Fatal("batch retained deferred work")
			}
			cleanup, err := model.closeScope("right")
			if err != nil {
				t.Fatal(err)
			}
			if len(cleanup.Entities) != 1 || cleanup.Entities[0].GetReleased() == nil {
				t.Fatalf("last reference was not released: %v", cleanup)
			}
			if len(model.entityRefs) != 0 || len(model.entities) != 0 || len(model.entityKeys) != 0 {
				t.Fatal("completed batch retained unreferenced entity state")
			}
		})
	}
}
