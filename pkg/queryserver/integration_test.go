package queryserver_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/adiom-data/weave/pkg/transactionmessage"
	messagegrpc "github.com/adiom-data/weave/pkg/transactionmessage/grpc"
)

// This test deliberately imports only public Weave packages. A host outside the
// repository can assemble both topologies without touching internal machinery.
func TestPublicBackendEmbeddedAndRemote(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to a disposable logical-replication database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_public_%d", time.Now().UnixNano())
	// The generated name contains only a fixed prefix and decimal digits.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(id bigint PRIMARY KEY, title text NOT NULL); INSERT INTO public.%s VALUES (1, 'initial'); CREATE PUBLICATION %s FOR TABLE public.%s`, name, name, name, name)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, name)
		_, _ = pool.Exec(cleanup, fmt.Sprintf(`DROP PUBLICATION %s; DROP TABLE public.%s`, name, name))
	}()
	store, err := storage.NewPostgres(ctx, databaseURL, storage.PostgresOptions{
		Publication: name, Slot: name, AllowedTables: []string{"public." + name},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	backend, err := queryserver.New(ctx, store, queryserver.Options{Limits: queryserver.Limits{ProgressBarrierDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	queryHost := grpc.NewServer()
	backend.Register(queryHost)
	embedded, err := projectionserver.New(ctx, backend, projectionserver.Options{TransactionMessages: backend.Messages()})
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	embedded.Register(queryHost)
	if len(queryHost.GetServiceInfo()) != 3 {
		t.Fatal("combined host must serve queries, messages, and projections")
	}
	connection := connectTestHost(t, queryHost)
	remoteQueries := querygrpc.NewClient(connection)
	remoteMessages := messagegrpc.NewClient(connection)
	remote, err := projectionserver.New(ctx, remoteQueries, projectionserver.Options{TransactionMessages: remoteMessages})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	projectionHost := grpc.NewServer()
	remote.Register(projectionHost)
	projectionConnection := connectTestHost(t, projectionHost)
	request := &reactivev1.ProjectionSnapshotRequest{WaitForSync: true, Root: &reactivev1.ProjectionNode{
		NodeId: "items", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{
			Query: &reactivev1.Query{Schema: "public", Table: name, Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"title"}}}}, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10},
		}},
	}}
	for _, conn := range []*grpc.ClientConn{connection, projectionConnection} {
		image, err := reactivev1.NewReactiveQueriesClient(conn).GetProjectionSnapshot(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if !image.GetExtensions().GetSynchronized() || len(image.GetData().GetFields()["items"].GetListValue().GetValues()) != 1 {
			t.Fatalf("projection image = %+v", image)
		}
	}
	var feeds []querysubscription.Feed
	for _, client := range []querysubscription.Client{backend, remoteQueries} {
		feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 64})
		if err != nil {
			t.Fatal(err)
		}
		defer feed.Close(context.Background())
		err = feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{ID: "items", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: name}}, Window: querysubscription.InitialWindow{Count: 10}}, func(_ querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
			if len(snapshot.Rows) != 1 || string(snapshot.Rows[0].Record["title"].Data) != "initial" {
				return fmt.Errorf("unexpected snapshot: %+v", snapshot.Rows)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		feeds = append(feeds, feed)
	}
	var streams []transactionmessage.Stream
	for _, client := range []transactionmessage.Client{backend.Messages(), remoteMessages} {
		stream, err := client.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		streams = append(streams, stream)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE public.%s SET title='updated' WHERE id=1`, name)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_logical_emit_message(true, 'public-api-test', 'receipt')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var position *querysubscription.Position
	for _, feed := range feeds {
		found := false
		for !found {
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
			for _, effect := range event.Delivery.Effects {
				for _, change := range effect.Batch.Changes {
					if string(change.Record["title"].Data) == "updated" {
						if position != nil && !querysubscription.SamePosition(position, event.Delivery.Delivery.Checkpoint) {
							t.Fatal("local and remote feeds disagree on transaction")
						}
						position = event.Delivery.Delivery.Checkpoint
						found = true
					}
				}
			}
		}
	}
	for _, stream := range streams {
		found := false
		for !found {
			batch, err := stream.Receive(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, transaction := range batch.Transactions {
				for _, message := range transaction.Messages {
					if message.Kind == "public-api-test" && string(message.Payload) == "receipt" {
						if !querysubscription.SamePosition(position, transaction.Position) {
							t.Fatal("messages and query effects disagree on transaction")
						}
						found = true
					}
				}
			}
		}
	}
}

func connectTestHost(t *testing.T, server *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///public-api", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func TestPublicBackendFailedStartupReleasesResources(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to a disposable logical-replication database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_startup_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `CREATE PUBLICATION `+name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, name)
		_, _ = pool.Exec(cleanup, `DROP PUBLICATION `+name)
	}()
	store, err := storage.NewPostgres(ctx, databaseURL, storage.PostgresOptions{Publication: name, Slot: name})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if instance, err := queryserver.New(ctx, store, queryserver.Options{Messages: transactionmessage.Options{MaxQueuedTransactions: -1}}); err == nil || instance != nil {
		t.Fatalf("startup returned %v, %v", instance, err)
	}
	var active bool
	if err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&active); err != nil || !active {
		t.Fatalf("failed query startup affected independently owned storage: active=%v, err=%v", active, err)
	}
	instance, err := queryserver.New(ctx, store, queryserver.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("normal shutdown = %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&active); err != nil || !active {
		t.Fatalf("closing query server closed borrowed storage: %v, %v", active, err)
	}
	// Storage can outlive and serve a replacement server; source shutdown then
	// propagates to that server without transferring storage ownership to it.
	replacement, err := queryserver.New(ctx, store, queryserver.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("source shutdown did not terminate query server: %v", err)
	}
}
