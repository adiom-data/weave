package evaluation

import (
	"context"
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

// Measures production fact binding, cache reads, expansion assembly and policy
// execution separately. The cold path supplies snapshots in memory: it includes
// async installation/fencing/leases, but no database or network latency.
func BenchmarkFactStages(b *testing.B) {
	for _, count := range []int{1, 16} {
		b.Run(fmt.Sprintf("links=%d", count), func(b *testing.B) {
			for _, stage := range []string{"Bind", "Fingerprint", "ReadMemoized", "ReadFreshView", "AssembleCached", "AuthorizeCachedSharedView", "AuthorizeCachedFreshView", "AuthorizeColdLocal"} {
				b.Run(stage, func(b *testing.B) {
					ctx := context.Background()
					root := &plan.Node{Name: "links", Query: storage.QuerySpec{Relation: relation}, Bindings: []plan.Binding{{Field: "task_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}}, Children: []*plan.Node{{Name: "project", Entity: true, Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "demo", Name: "projects"}}, Bindings: []plan.Binding{{Field: "id", Operator: storage.ComparisonEqual, Parent: "project_id"}}}}}
					evaluator, err := relationdef.Compile(relationdef.Definitions{tasks: {Facts: []relationdef.Fact{{ID: "access", Projection: root}}, Visibility: `facts.access.exists(link, link.children.project.item.owner == claims.user_id)`}}, relationdef.DefaultLimits())
					if err != nil {
						b.Fatal(err)
					}
					facts, feed := newFacts(b, Options{MaxQueries: 10, MaxRows: 100, LoadConcurrency: 2})
					in := input("u1")
					in.Candidate.Record["id"] = value("t1")
					parameters := storage.Record{"item": {Type: storage.TypeObject, State: storage.ValuePresent, Object: in.Candidate.Record}, "claims": {Type: storage.TypeObject, State: storage.ValuePresent, Object: in.Claims}}
					request := relationdef.FactRequest{ID: "access", Projection: root, Parameters: parameters, WorkLimit: 100000}
					query, matchable, err := plan.Bind(root, parameters, nil)
					if err != nil || !matchable {
						b.Fatalf("bind: %v", err)
					}
					var revision uint64
					load := func(scope *Scope) {
						b.Helper()
						for step := 0; step < 2; step++ {
							decision, err := scope.Evaluate(ctx, evaluator, in, at(10))
							if err != nil || decision.Resolved {
								b.Fatalf("expected pending discovery: %+v %v", decision, err)
							}
							installation := receive(b, feed)
							image := snapshot(10, revision+1)
							if step == 0 {
								for i := 0; i < count; i++ {
									link := row(fmt.Sprintf("l%d", i), "link")
									link.Record["project_id"] = value("p1")
									image.Rows = append(image.Rows, q.QueryRow{Row: link})
								}
							} else {
								project := row("p1", "project")
								project.Key.Relation.Name = "projects"
								project.Record["owner"] = value("u1")
								image.Rows = append(image.Rows, q.QueryRow{Row: project})
							}
							install(b, feed, installation, image)
							if err := facts.Observe(progress(10, revision+1, revision)); err != nil {
								b.Fatal(err)
							}
							revision++
						}
						decision, err := scope.Evaluate(ctx, evaluator, in, at(10))
						if err != nil || !decision.Resolved || !decision.Visible {
							b.Fatalf("loaded decision: %+v %v", decision, err)
						}
					}
					scope, err := NewScope(facts)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = scope.Close(ctx) })
					if stage != "AuthorizeColdLocal" {
						load(scope)
					}
					view, err := facts.OpenView(at(10))
					if err != nil {
						b.Fatal(err)
					}
					defer view.Close()
					var lease *Lease
					if stage == "ReadMemoized" || stage == "ReadFreshView" {
						lease, err = facts.Acquire(QueryRequest{ID: "links", Query: query})
						if err != nil {
							b.Fatal(err)
						}
						defer lease.Close(ctx)
						if _, err := view.read(lease); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					for b.Loop() {
						switch stage {
						case "Bind":
							_, ok, err := plan.Bind(root, parameters, nil)
							if err != nil || !ok {
								b.Fatal(err)
							}
						case "Fingerprint":
							if _, _, err := queryKey(query); err != nil {
								b.Fatal(err)
							}
						case "ReadMemoized":
							result, err := view.read(lease)
							if err != nil || len(result.Rows) != count {
								b.Fatalf("read: %v", err)
							}
						case "ReadFreshView":
							fresh, err := facts.OpenView(at(10))
							if err != nil {
								b.Fatal(err)
							}
							result, err := fresh.read(lease)
							_ = fresh.Close()
							if err != nil || len(result.Rows) != count {
								b.Fatalf("read: %v", err)
							}
						case "AssembleCached":
							resolver := scopeResolver{scope: scope, view: view, used: map[string]struct{}{}}
							result, err := resolver.Resolve(ctx, request)
							if err != nil || !result.Complete || len(result.Value.List) != count {
								b.Fatalf("assemble: %v", err)
							}
						case "AuthorizeCachedSharedView":
							decision, err := scope.EvaluatePath(ctx, evaluator, in.Claims, in.Candidate, view)
							if err != nil || !decision.Visible {
								b.Fatalf("evaluate: %v", err)
							}
						case "AuthorizeCachedFreshView":
							decision, err := scope.Evaluate(ctx, evaluator, in, at(10))
							if err != nil || !decision.Visible {
								b.Fatalf("evaluate: %v", err)
							}
						case "AuthorizeColdLocal":
							cold, err := NewScope(facts)
							if err != nil {
								b.Fatal(err)
							}
							load(cold)
							if err := cold.Close(ctx); err != nil {
								b.Fatal(err)
							}
						}
					}
					if stage == "AuthorizeColdLocal" {
						b.ReportMetric(2, "upstream-loads/op")
					}
				})
			}
		})
	}
}
