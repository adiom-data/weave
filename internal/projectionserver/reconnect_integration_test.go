package projectionserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// Warm fact selectors finish without database latency to pace their progress
// requests. Reopening a fan-out projection must not flood the consumer's queue.
func TestPolicyProjectionReopensAfterAllViewersDisconnectIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, _, _, queries := newReferenceIntegration(t, remote, false)
			const rows = 100
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s.links; DELETE FROM %[1]s.tasks;
				INSERT INTO %[1]s.tasks SELECT n,'task' FROM generate_series(1,%[2]d) n;
				INSERT INTO %[1]s.links SELECT n,1,n FROM generate_series(1,%[2]d) n`, schema, rows)); err != nil {
				t.Fatal(err)
			}
			definitions := policy.Map{}
			for _, table := range []string{"projects", "links", "tasks"} {
				definitions[storage.RelationName{Namespace: schema, Name: table}] = policy.Relation{
					Facts: []policy.Fact{{ID: "grant", Projection: &plan.Node{
						Name: "grant", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "tasks"}},
						Bindings: []plan.Binding{{Field: "id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}},
					}}},
					Allow: []policy.Rule{{ID: "grant", When: "facts.grant.exists(g,true)"}},
				}
			}
			configured, err := New(ctx, queries, Options{Policies: &PolicyOptions{Definitions: definitions}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = configured.Close() })
			root := referenceProjection(schema, false)
			root.Children[0].GetList().Window.Count = rows + 1
			for round := 0; round < 3; round++ {
				var cancels []context.CancelFunc
				var completions []chan error
				for viewer := 0; viewer < 2; viewer++ {
					viewerCtx, cancel := context.WithCancel(ctx)
					cancels = append(cancels, cancel)
					t.Cleanup(cancel)
					stream, done := referenceOpen(t, viewerCtx, configured.(*server), root)
					completions = append(completions, done)
					entities := make(map[string]bool)
					referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
						if batch := message.GetModelBatch(); batch != nil {
							for _, entity := range batch.Entities {
								if entity.GetUpsert() != nil {
									entities[entity.EntityId] = true
								}
							}
						}
						return message.GetReady() != nil
					})
					if len(entities) != 1+2*rows {
						t.Fatalf("round %d viewer %d: incomplete projection: %d entities", round, viewer, len(entities))
					}
				}
				for _, cancel := range cancels {
					cancel()
				}
				for _, done := range completions {
					select {
					case <-done:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
			}
		})
	}
}
