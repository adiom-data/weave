package queryserver_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/pkg/queryserver"
	q "github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type identityCountingStorage struct {
	storage.Storage
	reads atomic.Int32
}

func (s *identityCountingStorage) RequestProgress(ctx context.Context, fence storage.Fence) error {
	return s.Storage.(storage.ProgressRequester).RequestProgress(ctx, fence)
}

func (s *identityCountingStorage) BeginSnapshot(ctx context.Context, plan storage.PreparedQuery) (storage.SnapshotSession, error) {
	s.reads.Add(1)
	return s.Storage.BeginSnapshot(ctx, plan)
}

func TestIdentitySubscriptionsEmbeddedAndRemoteIntegration(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL")
	}
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool, err := pgxpool.New(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			name := fmt.Sprintf("identity_%d", time.Now().UnixNano())
			if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE public.%s(tenant bigint,id bigint,allowed boolean NOT NULL,payload text NOT NULL,PRIMARY KEY(tenant,id)); ALTER TABLE public.%s ALTER COLUMN payload SET STORAGE EXTERNAL; CREATE PUBLICATION %s FOR TABLE public.%s", name, name, name, name)); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				for _, sql := range []string{"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='" + name + "' AND NOT active", "DROP PUBLICATION " + name, "DROP TABLE public." + name} {
					if _, err := pool.Exec(cleanup, sql); err != nil {
						t.Error(err)
					}
				}
			}()
			exec := func(sql string) {
				t.Helper()
				if _, err := pool.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			exec("INSERT INTO public." + name + " VALUES(1,1,true,repeat('x',10000)),(1,2,true,'two')")
			store, err := storage.NewPostgres(ctx, url, storage.PostgresOptions{Publication: name, Slot: name})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			counted := &identityCountingStorage{Storage: store}
			backend, err := queryserver.New(ctx, counted, queryserver.Options{Limits: queryserver.Limits{ProgressBarrierDelay: time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			var client q.Client = backend
			if remote {
				host := grpc.NewServer()
				backend.Register(host)
				conn := connectTestHost(t, host)
				client = querygrpc.NewClient(conn)
			}
			feed, err := client.OpenFeed(ctx, q.FeedOptions{BufferLimit: 128})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(context.Background())
			equal := func(field string, typ storage.ValueType, value string) *storage.Predicate {
				return &storage.Predicate{Comparison: &storage.Comparison{Field: storage.FieldPath(field), Operator: storage.ComparisonEqual, Value: storage.Literal{Type: typ, Data: []byte(value)}}}
			}
			selector := func(id string) storage.QuerySpec {
				return storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: name}, Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{equal("allowed", storage.TypeBool, "true"), equal("tenant", storage.TypeInteger, "1"), equal("id", storage.TypeInteger, id)}}}}
			}
			open := func(id string, query storage.QuerySpec, want bool) {
				t.Helper()
				if err := feed.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: id, Query: query}, func(_ q.EntitySubscription, s q.EntitySnapshot) error {
					if (s.Row != nil) != want {
						return fmt.Errorf("%s: unexpected initial presence", id)
					}
					if s.Row != nil && len(s.Row.Cursor) != 0 {
						return fmt.Errorf("entity cursor leaked")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := counted.reads.Load()
			open("one", selector("1"), true)
			if got := counted.reads.Load() - before; got != 1 {
				t.Fatalf("cold identity used %d snapshots, want 1 (no discovery)", got)
			}
			before = counted.reads.Load()
			open("two", selector("1"), true)
			if counted.reads.Load() != before {
				t.Fatal("warm identity reread storage")
			}
			open("missing", selector("999"), false)
			before = counted.reads.Load()
			open("missing-two", selector("999"), false)
			if counted.reads.Load() != before {
				t.Fatal("known absence reread storage")
			}
			if err := feed.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "list", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: name}}, Window: q.InitialWindow{Count: 10}}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
				t.Fatal(err)
			}
			// Same predicate through both APIs must not share incompatible cache state.
			if err := feed.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "exact-list", Query: selector("1"), Window: q.InitialWindow{Count: 10}}, func(_ q.QuerySubscription, s q.QuerySnapshot) error {
				if len(s.Rows) != 1 {
					return fmt.Errorf("exact list lost row")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			await := func(ids []string, kind q.ChangeKind, payload string) {
				t.Helper()
				for {
					event, err := feed.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if event.Failure != nil {
						t.Fatal(event.Failure.Cause)
					}
					if event.Delivery == nil {
						continue
					}
					found := map[string]bool{}
					for effect := range event.Delivery.SubscriptionEffects() {
						for _, change := range effect.Batch.Changes {
							if change.Kind == kind {
								if payload != "" && string(change.Record["payload"].Data) != payload {
									continue
								}
								found[effect.SubscriptionID] = true
							}
						}
					}
					ready := true
					for _, id := range ids {
						ready = ready && found[id]
					}
					if ready {
						if !event.Delivery.Delivery.Atomic {
							t.Fatal("CDC delivery lost transaction atomicity")
						}
						return
					}
				}
			}
			exec("UPDATE public." + name + " SET allowed=false WHERE id=1")
			await([]string{"one", "two", "exact-list"}, q.Removed, "")
			// payload remains TOASTed and unchanged; its cached image must survive.
			exec("UPDATE public." + name + " SET allowed=true WHERE id=1")
			await([]string{"one", "two", "exact-list"}, q.Added, strings.Repeat("x", 10000))
			exec("INSERT INTO public." + name + " VALUES(1,999,true,'created')")
			await([]string{"missing", "missing-two", "list"}, q.Added, "created")
			exec("UPDATE public." + name + " SET id=3 WHERE id=1")
			await([]string{"one", "two", "exact-list"}, q.Removed, "")
			exec("INSERT INTO public." + name + " VALUES(1,1,true,'recreated')")
			await([]string{"one", "two", "exact-list"}, q.Added, "recreated")
			exec("DELETE FROM public." + name + " WHERE id=999")
			await([]string{"missing", "missing-two", "list"}, q.Removed, "")
			exec("TRUNCATE public." + name)
			for {
				event, err := feed.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Failure != nil {
					t.Fatal(event.Failure.Cause)
				}
				if event.Delivery == nil {
					continue
				}
				cleared := false
				for effect := range event.Delivery.SubscriptionEffects() {
					cleared = cleared || effect.Batch.Clear
				}
				if cleared {
					break
				}
			}
			exec("INSERT INTO public." + name + " VALUES(1,1,true,'after truncate')")
			await([]string{"one", "two", "exact-list"}, q.Added, "after truncate")
		})
	}
}
