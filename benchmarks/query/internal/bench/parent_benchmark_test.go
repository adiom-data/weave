package bench

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/postgres"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
)

// BenchmarkWarmQueryParent profiles only the query layer. Seed an isolated
// benchmark DB with 100 projects, 500 rows/project and 256-byte payloads using
// expansion mode first. Hinted parents include parent-owned task interests;
// no explicit child RPCs or projection runtime are used.
// The first parent remains live to pin shared coverage. A gRPC case reuses its
// connection but opens a fresh feed per operation; local cases avoid transport.
// ns/op and allocation/CPU profiles include feed cleanup. parent-ns/op excludes
// cleanup. Stage metrics are non-overlapping PrepareQuery / PrepareReference /
// Bind call totals; each is included in parent-ns/op, not additive to it.
func BenchmarkWarmQueryParent(b *testing.B) {
	for _, remote := range []bool{false, true} {
		transport := "local"
		if remote {
			transport = "grpc"
		}
		for _, count := range []int{10, 100, 500} {
			for _, hint := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/rows=%d/hint=%v", transport, count, hint), func(b *testing.B) {
					benchmarkWarmQueryParent(b, remote, count, hint, warmParentOnly)
				})
			}
		}
	}
}

// BenchmarkWarmQueryExpansion adds the same sequential child subscriptions as
// the network expansion harness. Compare with WarmQueryParent to separate
// snapshot reconstruction from per-child subscription and transport costs.
func BenchmarkWarmQueryExpansion(b *testing.B) {
	for _, remote := range []bool{false, true} {
		transport := "local"
		if remote {
			transport = "grpc"
		}
		for _, count := range []int{10, 100, 500} {
			b.Run(fmt.Sprintf("%s/rows=%d", transport, count), func(b *testing.B) {
				benchmarkWarmQueryParent(b, remote, count, true, warmEntityExpansion)
			})
		}
	}
}

// BenchmarkWarmQueryBatch is a topology diagnostic, not an equivalent generic
// many-to-many API. This fixture's task IDs are contiguous, so a bounded task
// query yields the same rows with two subscriptions instead of 501. Electric's
// expansion harness likewise uses two shapes, but selects tasks by an ID array.
func BenchmarkWarmQueryBatch(b *testing.B) {
	for _, remote := range []bool{false, true} {
		transport := "local"
		if remote {
			transport = "grpc"
		}
		b.Run(transport+"/rows=500", func(b *testing.B) {
			benchmarkWarmQueryParent(b, remote, 500, true, warmBatchExpansion)
		})
	}
}

type warmReadMode uint8

const (
	warmParentOnly warmReadMode = iota
	warmEntityExpansion
	warmBatchExpansion
)

type parentProfileSource struct {
	datastore.Storage
	prepareNS, referenceNS, bindNS, binds, snapshots atomic.Int64
	resolveNS, resolves                              atomic.Int64
}

func (s *parentProfileSource) PrepareQuery(ctx context.Context, query storage.QuerySpec) (datastore.PreparedQuery, error) {
	start := time.Now()
	defer func() { s.prepareNS.Add(time.Since(start).Nanoseconds()) }()
	return s.Storage.PrepareQuery(ctx, query)
}

func (s *parentProfileSource) PrepareReference(ctx context.Context, query datastore.PreparedQuery, shape storage.ReferenceFetch) (datastore.PreparedReference, error) {
	start := time.Now()
	defer func() { s.referenceNS.Add(time.Since(start).Nanoseconds()) }()
	prepared, err := s.Storage.(datastore.ReferencePreparer).PrepareReference(ctx, query, shape)
	if err != nil {
		return nil, err
	}
	return parentProfileReference{PreparedReference: prepared, source: s}, nil
}

func (s *parentProfileSource) BeginSnapshot(ctx context.Context, query datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	s.snapshots.Add(1)
	return s.Storage.BeginSnapshot(ctx, query)
}

func (s *parentProfileSource) RequestProgress(ctx context.Context, fence storage.Fence) error {
	return s.Storage.(datastore.ProgressRequester).RequestProgress(ctx, fence)
}

type parentProfileReference struct {
	datastore.PreparedReference
	source *parentProfileSource
}

func (p parentProfileReference) Resolve(record storage.Record) (datastore.ReferenceLookup, bool, error) {
	start := time.Now()
	defer func() { p.source.resolveNS.Add(time.Since(start).Nanoseconds()); p.source.resolves.Add(1) }()
	return p.PreparedReference.Resolve(record)
}

func (p parentProfileReference) Bind(record storage.Record) (datastore.PreparedQuery, storage.QuerySpec, storage.EntityKey, bool, error) {
	start := time.Now()
	defer func() { p.source.binds.Add(1); p.source.bindNS.Add(time.Since(start).Nanoseconds()) }()
	return p.PreparedReference.Bind(record)
}

func benchmarkWarmQueryParent(b *testing.B, remote bool, count int, hint bool, mode warmReadMode) {
	b.StopTimer()
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		b.Skip("set WEAVE_TEST_DATABASE_URL to a seeded disposable expansion benchmark database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	b.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)
	if err := CheckDatabase(ctx, pool); err != nil {
		b.Fatal(err)
	}
	seed := Config{Backend: "weave", Workload: "expansion", Scenario: "shared", Clients: 1, Projects: 100, Rows: 500, PayloadBytes: 256, Rate: 1, Duration: time.Second, InitConcurrency: 1, InitTimeout: time.Minute, DrainTimeout: time.Second}
	if err := VerifySeed(ctx, pool, seed); err != nil {
		b.Fatal(err)
	}
	var nonce [6]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		b.Fatal(err)
	}
	slot := fmt.Sprintf("parent_profile_%x", nonce)
	b.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active", slot); err != nil {
			b.Error(err)
		}
	})
	options := postgres.DefaultOptions()
	options.Publication, options.Slot = "bench_publication", slot
	options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := postgres.NewStorage(ctx, url, options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	source := &parentProfileSource{Storage: store}
	runCtx, stop := context.WithCancel(ctx)
	coordinator, err := engine.StartCoordinator(runCtx, source)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { stop(); _ = coordinator.Wait(context.Background()) })
	var client querysubscription.Client = querylocal.New(coordinator)
	if remote {
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		querygrpc.NewServer(client).Register(server)
		go func() { _ = server.Serve(listener) }()
		b.Cleanup(server.Stop)
		connection, err := grpc.NewClient("passthrough:///parent-profile", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = connection.Close() })
		client = querygrpc.NewClient(connection)
	}
	request := querysubscription.QuerySubscribeRequest{ID: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "bench", Name: "links"}, Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{integerEqual("project_id", 1), {Comparison: &storage.Comparison{Field: storage.MustFieldPath("id"), Operator: storage.ComparisonLessOrEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte(strconv.Itoa(count))}}}}}}}, Window: querysubscription.InitialWindow{Count: count + 1}}
	if hint {
		request.Reference = &storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "bench", Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: storage.MustFieldPath("task_id"), Target: storage.MustFieldPath("id")}}}
	}
	var lastParent time.Duration
	open := func() querysubscription.Feed {
		start := time.Now()
		feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 1024})
		if err != nil {
			b.Fatal(err)
		}
		var links []querysubscription.QueryRow
		if err := feed.SubscribeQuery(ctx, request, func(_ querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
			if len(snapshot.Rows) != count || snapshot.Bounds.Left.Kind != querysubscription.BoundaryStart || snapshot.Bounds.Right.Kind != querysubscription.BoundaryEnd {
				return fmt.Errorf("incomplete warm parent image")
			}
			for index, row := range snapshot.Rows {
				if string(row.Record["id"].Data) != strconv.Itoa(index+1) {
					return fmt.Errorf("incorrect parent order")
				}
			}
			if mode == warmEntityExpansion {
				links = snapshot.Rows
			}
			return nil
		}); err != nil {
			_ = feed.Close(context.WithoutCancel(ctx))
			b.Fatal(err)
		}
		lastParent = time.Since(start)
		if mode == warmBatchExpansion {
			query := request.Query
			query.Relation = storage.RelationName{Namespace: "bench", Name: "tasks"}
			if err := feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{ID: "tasks", Query: query, Window: request.Window}, func(_ querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
				if len(snapshot.Rows) != count {
					return fmt.Errorf("incomplete batched task image")
				}
				for index, row := range snapshot.Rows {
					task, err := canonicalTask(row.Record)
					if err != nil {
						return err
					}
					if task.ID != int64(index+1) || task.ProjectID != 1 || task.Revision != 0 || task.Payload != strings.Repeat("x", 256) {
						return fmt.Errorf("incorrect batched child %d", index+1)
					}
				}
				return nil
			}); err != nil {
				_ = feed.Close(context.WithoutCancel(ctx))
				b.Fatal(err)
			}
		}
		for index, link := range links {
			child := querysubscription.EntitySubscribeRequest{
				ID:        fmt.Sprintf("task-%d", index+1),
				Query:     storage.QuerySpec{Relation: storage.RelationName{Namespace: "bench", Name: "tasks"}, Filter: integerEqual("id", int64(index+1))},
				FetchFrom: &querysubscription.ReferenceSource{SubscriptionID: "links", ParentKey: link.Key},
			}
			if err := feed.SubscribeEntity(ctx, child, func(_ querysubscription.EntitySubscription, snapshot querysubscription.EntitySnapshot) error {
				if snapshot.Row == nil {
					return fmt.Errorf("missing child %d", index+1)
				}
				task, err := canonicalTask(snapshot.Row.Record)
				if err != nil {
					return err
				}
				if task.ID != int64(index+1) || task.ProjectID != 1 || task.Revision != 0 || task.Payload != strings.Repeat("x", 256) {
					return fmt.Errorf("incorrect child %d", index+1)
				}
				return nil
			}); err != nil {
				_ = feed.Close(context.WithoutCancel(ctx))
				b.Fatal(err)
			}
		}
		return feed
	}
	closeFeed := func(feed querysubscription.Feed) {
		if err := feed.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
			b.Fatal(err)
		}
	}
	prime := open()
	b.Cleanup(func() { closeFeed(prime) })
	source.prepareNS.Store(0)
	source.referenceNS.Store(0)
	source.bindNS.Store(0)
	source.binds.Store(0)
	source.resolveNS.Store(0)
	source.resolves.Store(0)
	source.snapshots.Store(0)
	var parentNS, readNS int64
	b.ReportAllocs()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		start := time.Now()
		feed := open()
		readNS += time.Since(start).Nanoseconds()
		parentNS += lastParent.Nanoseconds()
		closeFeed(feed)
	}
	b.StopTimer()
	if source.snapshots.Load() != 0 {
		b.Fatalf("warm parent unexpectedly opened %d database snapshots", source.snapshots.Load())
	}
	wantBinds := int64(0)
	if hint {
		wantBinds = int64(count) * int64(b.N)
	}
	if source.binds.Load() != wantBinds {
		b.Fatalf("prepared %d target queries, expected %d", source.binds.Load(), wantBinds)
	}
	if hint && source.resolves.Load() != int64(count)*int64(b.N) {
		b.Fatalf("unexpected lookup count: %d", source.resolves.Load())
	}
	divisor := float64(b.N)
	b.ReportMetric(float64(parentNS)/divisor, "parent-ns/op")
	if mode != warmParentOnly {
		b.ReportMetric(float64(readNS)/divisor, "read-ns/op")
		b.ReportMetric(float64(readNS-parentNS)/divisor, "children-ns/op")
	}
	b.ReportMetric(float64(source.prepareNS.Load())/divisor, "prepare-ns/op")
	b.ReportMetric(float64(source.referenceNS.Load())/divisor, "reference-prepare-ns/op")
	b.ReportMetric(float64(source.bindNS.Load())/divisor, "bind-ns/op")
	b.ReportMetric(float64(source.binds.Load())/divisor, "binds/op")
	b.ReportMetric(float64(source.resolveNS.Load())/divisor, "resolve-ns/op")
	b.ReportMetric(float64(source.resolves.Load())/divisor, "resolves/op")
	b.ReportMetric(float64(source.snapshots.Load())/divisor, "snapshots/op")
}
