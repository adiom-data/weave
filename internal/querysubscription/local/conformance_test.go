package local

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/deliverytest"
	querygrpc "github.com/adiom-data/weave/internal/querysubscription/grpc"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// Exercise the same real local feed directly and through the production gRPC
// server/client. Assertions use only the consumer contract, not feed internals.
type contractFixture struct {
	feed     querysubscription.Feed
	mu       sync.Mutex
	native   map[string]*deliverytest.Interest
	revision uint64
	blocked  chan struct{}
}

func newContractFixture(t *testing.T, ctx context.Context, remote bool) *contractFixture {
	t.Helper()
	f := &contractFixture{native: make(map[string]*deliverytest.Interest), blocked: make(chan struct{}, 1)}
	relation := storage.RelationKey{Source: "test", Name: "items"}
	coordinator := coordinatorStub{
		subscribe: func(ctx context.Context, id, group string, spec storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			if spec.Relation.Name == "blocked" {
				f.blocked <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			sub := fixtureInterest(id, group)
			f.mu.Lock()
			f.native[id] = sub
			revision := f.revision
			f.mu.Unlock()
			return &engine.OpenSubscription{Subscription: sub.Subscription, Plan: nonComparingPlan{relation: relation}, Revision: revision,
				Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision},
				Bounds:     datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}, nil
		},
		unsubscribe: func(_ context.Context, id string) error {
			f.mu.Lock()
			sub := f.native[id]
			delete(f.native, id)
			f.mu.Unlock()
			if sub != nil {
				sub.Queue.Retire(delivery.ErrClientClosed)
			}
			return nil
		},
		progress: func(context.Context, string) (*delivery.ProgressReport, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			previous := f.revision
			f.revision++
			return &delivery.ProgressReport{Checkpoint: &querysubscription.Position{SourceID: "test", Value: previous}, Revision: f.revision, PreviousGroupRevision: previous}, nil
		},
	}
	var client querysubscription.Client = New(coordinator)
	if remote {
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer()
		querygrpc.NewServer(client).Register(server)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		connection, err := grpc.NewClient("passthrough:///conformance", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		client = querygrpc.NewClient(connection)
	}
	var err error
	f.feed, err = client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = f.feed.Close(closeCtx)
	})
	return f
}

func contractRequest(id string) querysubscription.QuerySubscribeRequest {
	return querysubscription.QuerySubscribeRequest{ID: id, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}}
}

func (f *contractFixture) subscription(t *testing.T, id string) *deliverytest.Interest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, sub := range f.native {
		if strings.HasSuffix(key, "\x00"+id) {
			return sub
		}
	}
	t.Fatalf("native subscription %q missing", id)
	return nil
}

func (f *contractFixture) publish(t *testing.T, ids ...string) {
	t.Helper()
	var subs []*deliverytest.Interest
	for _, id := range ids {
		sub := f.subscription(t, id)
		subs = append(subs, sub)
	}
	f.mu.Lock()
	previous := f.revision
	f.revision++
	revision := f.revision
	f.mu.Unlock()
	var batches []delivery.SubscriptionBatch
	for i, sub := range subs {
		key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: ids[i]}
		record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(ids[i])}}
		batches = append(batches, delivery.SubscriptionBatch{SubscriptionID: sub.ID, Delivery: delivery.DeliveryCommit, Revision: revision, PreviousGroupRevision: previous, Atomic: true,
			Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision},
			Events:     []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: key, Record: record}},
			Membership: delivery.MembershipDelta{Insertions: []delivery.MembershipInsertion{{Key: key, FinalIndex: 0}}},
		})
	}
	if err := subs[0].Queue.OfferGroup(batches); err != nil {
		t.Fatal(err)
	}
}

func TestFeedContractLocalAndGRPC(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "grpc"
		}
		t.Run(name, func(t *testing.T) {
			for _, reject := range []bool{false, true} {
				name := "installation_order_and_progress"
				if reject {
					name = "failed_installation_preserves_other_effects"
				}
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					f := newContractFixture(t, ctx, remote)
					if _, _, err := subscribeLocalTest(ctx, f.feed, contractRequest("healthy")); err != nil {
						t.Fatal(err)
					}
					started, release := make(chan struct{}), make(chan struct{})
					done := make(chan error, 1)
					go func() {
						done <- f.feed.SubscribeQuery(ctx, contractRequest("loading"), func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error {
							close(started)
							select {
							case <-release:
							case <-ctx.Done():
								return ctx.Err()
							}
							if reject {
								return errors.New("installer rejected snapshot")
							}
							return nil
						})
					}()
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("installer did not start")
					}
					f.publish(t, "healthy", "loading")
					probe, stop := context.WithTimeout(ctx, 20*time.Millisecond)
					if event, err := f.feed.Next(probe); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("event escaped installer: %+v %v", event, err)
					}
					stop()
					close(release)
					if err := <-done; (err != nil) != reject {
						t.Fatalf("subscribe=%v reject=%v", err, reject)
					}
					event, err := f.feed.Next(ctx)
					if err != nil || event.Delivery == nil || !event.Delivery.Delivery.Atomic {
						t.Fatalf("transaction=%+v err=%v", event, err)
					}
					want := 2
					if reject {
						want = 1
					}
					if len(event.Delivery.Effects) != want {
						t.Fatalf("effects=%+v want=%d", event.Delivery.Effects, want)
					}
					if reject && event.Delivery.Effects[0].SubscriptionID != "healthy" {
						t.Fatal("failed scope displaced healthy effect")
					}
					if err := f.feed.RequestProgress(ctx); err != nil {
						t.Fatal(err)
					}
					event, err = f.feed.Next(ctx)
					if err != nil || event.Progress == nil || event.Progress.Position.Value < 1 {
						t.Fatalf("progress=%+v err=%v", event, err)
					}
				})
			}
			t.Run("cancellation_failure_isolation_and_close", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				f := newContractFixture(t, ctx, remote)
				request := contractRequest("blocked")
				request.Query.Relation.Name = "blocked"
				callCtx, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { _, _, err := subscribeLocalTest(callCtx, f.feed, request); done <- err }()
				select {
				case <-f.blocked:
				case <-ctx.Done():
					t.Fatal("blocked request did not start")
				}
				stop()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation=%v", err)
				}
				for _, id := range []string{"broken", "healthy"} {
					if _, _, err := subscribeLocalTest(ctx, f.feed, contractRequest(id)); err != nil {
						t.Fatal(err)
					}
				}
				f.subscription(t, "broken").Queue.Retire(errors.New("query failed"))
				event, err := f.feed.Next(ctx)
				if err != nil || event.Failure == nil || event.Failure.SubscriptionID != "broken" {
					t.Fatalf("isolated failure=%+v err=%v", event, err)
				}
				f.publish(t, "healthy")
				event, err = f.feed.Next(ctx)
				if err != nil || event.Delivery == nil || len(event.Delivery.Effects) != 1 {
					t.Fatalf("healthy delivery=%+v err=%v", event, err)
				}
				if err := f.feed.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := f.feed.Next(ctx); !errors.Is(err, querysubscription.ErrClosed) {
					t.Fatalf("next after close=%v", err)
				}
			})
		})
	}
}
