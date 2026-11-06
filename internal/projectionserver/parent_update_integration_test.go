package projectionserver

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestParentNonbindingUpdateKeepsExpandedQueriesIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, hinted := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote=%v/hint=%v", remote, hinted), func(t *testing.T) {
				ctx, pool, schema, source, service, _ := newReferenceIntegration(t, remote, false)
				viewerCtx, cancel := context.WithCancel(ctx)
				stream, done := referenceOpen(t, viewerCtx, service, referenceProjection(schema, hinted))
				defer func() { cancel(); <-done }()
				referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool { return message.GetReady() != nil })
				joins, batches, reads, linkReads, plans := source.counts()
				if _, err := pool.Exec(ctx, "UPDATE "+schema+".projects SET title='changed' WHERE id=1"); err != nil {
					t.Fatal(err)
				}
				referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
					batch := message.GetModelBatch()
					if batch == nil {
						return false
					}
					if len(batch.Queries) != 0 || len(batch.Projections) != 0 {
						t.Fatalf("unrelated parent update changed child structure: %v", batch)
					}
					for _, entity := range batch.Entities {
						if entity.GetReleased() != nil {
							t.Fatal("parent update released a child entity")
						}
						if upsert := entity.GetUpsert(); upsert != nil && upsert.Values["title"].GetTextValue() == "changed" {
							if batch.Kind != pb.ModelBatch_KIND_COMMIT || !batch.Atomic {
								t.Fatal("parent update lost its transaction")
							}
							return true
						}
					}
					return false
				})
				j2, b2, r2, l2, p2 := source.counts()
				if j2 != joins || b2 != batches || r2 != reads || l2 != linkReads || p2 != plans {
					t.Fatal("parent-only update reloaded child dependencies")
				}
			})
		}
	}
}
