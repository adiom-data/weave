package projectionserver

import (
	"context"
	"fmt"
	"testing"
)

// Run with -benchtime=1x -count=3. Each sample owns a fresh PostgreSQL schema,
// replication slot, coordinator and projection service. Fixture creation and
// warm-up are excluded. "grpc" uses the production codec over bufconn: it
// measures serialization, not a separate-process or cross-host deployment.
func BenchmarkReferenceExpansion(b *testing.B) {
	for _, remote := range []bool{false, true} {
		transport := "embedded"
		if remote {
			transport = "grpc"
		}
		for _, rows := range []int{10, 100, 500} {
			for _, hinted := range []bool{false, true} {
				for _, warm := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/rows=%d/hint=%v/warm=%v", transport, rows, hinted, warm), func(b *testing.B) {
						if b.N != 1 {
							b.Fatal("use -benchtime=1x: cold samples require a fresh backend")
						}
						b.StopTimer()
						ctx, pool, schema, source, service, _ := newReferenceIntegration(b, remote, false)
						_, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s.links; DELETE FROM %[1]s.tasks;
							INSERT INTO %[1]s.tasks SELECT n, repeat('x',256) FROM generate_series(1,%[2]d) n;
							INSERT INTO %[1]s.links SELECT n,1,n FROM generate_series(1,%[2]d) n;
							CREATE INDEX ON %[1]s.links(project_id,id);`, schema, rows))
						if err != nil {
							b.Fatal(err)
						}
						root := referenceProjection(schema, hinted)
						root.Children[0].GetList().Window.Count = uint32(rows + 1)
						open := func() {
							streamCtx, cancel := context.WithCancel(ctx)
							stream, done := referenceOpen(b, streamCtx, service, root)
							b.Cleanup(func() { cancel(); <-done })
							entities := make(map[string]bool)
							links := 0
							for {
								select {
								case message := <-stream.sent:
									if failure := message.GetError(); failure != nil {
										b.Fatalf("projection failed: %v", failure)
									}
									if batch := message.GetModelBatch(); batch != nil {
										for _, entity := range batch.Entities {
											if entity.GetUpsert() != nil {
												entities[entity.EntityId] = true
											}
										}
										for _, query := range batch.Queries {
											if query.NodeId == "links" && query.GetReset_() != nil {
												links = len(query.GetReset_().EntityIds)
											}
										}
									}
									if message.GetReady() != nil {
										if links != rows || len(entities) != 1+2*rows {
											b.Fatalf("incomplete image: links=%d entities=%d, want %d/%d", links, len(entities), rows, 1+2*rows)
										}
										return
									}
								case <-ctx.Done():
									b.Fatal(ctx.Err())
								}
							}
						}
						if warm {
							open() // Keep the first viewer alive to define fully warm coverage.
						}
						joins, batches, reads, linkReads, plans := source.counts()
						b.ReportAllocs()
						b.StartTimer()
						open()
						b.StopTimer()
						j2, b2, r2, l2, p2 := source.counts()
						b.ReportMetric(float64(j2-joins), "joins/op")
						b.ReportMetric(float64(b2-batches), "target-batches/op")
						b.ReportMetric(float64(r2-reads), "target-reads/op")
						b.ReportMetric(float64(l2-linkReads), "link-reads/op")
						b.ReportMetric(float64(p2-plans), "child-plans/op")
					})
				}
			}
		}
	}
}
