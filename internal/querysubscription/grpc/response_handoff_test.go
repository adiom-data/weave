package querygrpc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	querysubscription "github.com/adiom-data/weave/internal/querysubscription/model"
	queryv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

func responseTestFeed() *remoteFeed {
	feed := &remoteFeed{
		pending:       make(map[string]chan response),
		subscriptions: make(map[string]*remoteSubscription),
		requests:      make(chan outboundRequest),
		controls:      make(chan *queryv1.ClientMessage, 4),
		done:          make(chan struct{}),
		events:        feedbuffer.New(16),
	}
	for _, id := range []string{"canceled", "healthy"} {
		feed.subscriptions[id] = &remoteSubscription{id: id, owner: feed}
		_ = feed.events.RegisterSubscription(id)
	}
	return feed
}

func routeTestSnapshot(feed *remoteFeed, kind, requestID string) {
	if kind == "query" {
		feed.routeSnapshot(requestID, "canceled", &queryv1.QuerySnapshot{})
	} else {
		feed.routeEntitySnapshot(requestID, "canceled", &queryv1.EntitySnapshot{})
	}
}

func TestAbandonedSnapshotResponseReleasesOnlyItsInstallation(t *testing.T) {
	for _, kind := range []string{"query", "entity"} {
		t.Run(kind, func(t *testing.T) {
			feed := responseTestFeed()
			wait := make(chan response, 1)
			feed.pending["request"] = wait
			feed.beginInstallation() // another subscription is still installing
			routeTestSnapshot(feed, kind, "request")
			batch := querysubscription.Event{Delivery: &querysubscription.DeliveryEnvelope{
				Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryHydration, Revision: 1},
				Effects: []querysubscription.SubscriptionEffect{
					{SubscriptionID: "canceled"}, {SubscriptionID: "healthy"},
				},
			}}
			if err := feed.events.Offer(batch); err != nil {
				t.Fatal(err)
			}
			if err := feed.events.Offer(querysubscription.Event{Failure: &querysubscription.SubscriptionFailure{SubscriptionID: "canceled", Cause: errors.New("retired")}}); err != nil {
				t.Fatal(err)
			}
			feed.abandonRequest("request", wait, "canceled")
			if feed.subscriptions["canceled"] != nil || feed.subscriptions["healthy"] == nil {
				t.Fatal("cancellation retired the wrong membership")
			}
			if len(wait) != 0 || feed.events.Len() != 0 {
				t.Fatal("abandoned response was not reclaimed or another installation was released")
			}
			feed.finishInstallation() // finish only the healthy subscription
			if feed.events.Len() != 1 {
				t.Fatal("canceled installation stranded delivery or retained its failure event")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := feed.Next(ctx)
			if err != nil || got.Delivery == nil || len(got.Delivery.Effects) != 1 || got.Delivery.Effects[0].SubscriptionID != "healthy" {
				t.Fatalf("canceled effects leaked into delivery: %+v, %v", got, err)
			}
		})
	}
}

func TestCancellationBeforeSnapshotDoesNotAcquireInstallation(t *testing.T) {
	for _, kind := range []string{"query", "entity"} {
		t.Run(kind, func(t *testing.T) {
			feed := responseTestFeed()
			wait := make(chan response, 1)
			feed.pending["request"] = wait
			feed.abandonRequest("request", wait, "canceled")
			routeTestSnapshot(feed, kind, "request")
			if len(wait) != 0 || feed.subscriptions["canceled"] != nil {
				t.Fatal("late snapshot revived a canceled request")
			}
			if err := feed.events.Offer(querysubscription.Event{Progress: &querysubscription.Progress{Position: &querysubscription.Position{SourceID: "source", Value: 1}, Revision: 1}}); err != nil {
				t.Fatal(err)
			}
			if feed.events.Len() != 1 {
				t.Fatal("ignored snapshot paused unrelated progress")
			}
		})
	}
}

func TestCanceledRequestReclaimsRacingSnapshotResponse(t *testing.T) {
	// Keep the requester asleep briefly after cancellation while the receiver
	// routes its response. This reproduces the old abandoned-mailbox race;
	// either select winner is valid, but neither may strand installation.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, kind := range []string{"query", "entity"} {
		t.Run(kind, func(t *testing.T) {
			for attempt := 0; attempt < 100; attempt++ {
				feed := responseTestFeed()
				ctx, cancel := context.WithCancel(context.Background())
				message := &queryv1.ClientMessage{}
				if kind == "query" {
					message.Operation = &queryv1.ClientMessage_SubscribeQuery{SubscribeQuery: &queryv1.SubscribeQuery{SubscriptionId: "canceled"}}
				} else {
					message.Operation = &queryv1.ClientMessage_SubscribeEntity{SubscribeEntity: &queryv1.SubscribeEntity{SubscriptionId: "canceled"}}
				}
				result := make(chan response, 1)
				go func() {
					got, err := feed.request(ctx, message)
					got.err = err
					result <- got
				}()
				request := <-feed.requests
				cancel()
				routeTestSnapshot(feed, kind, request.message.RequestId)
				got := <-result
				if got.err == nil {
					if !got.installation {
						t.Fatal("accepted snapshot lost its installation ownership")
					}
					feed.finishInstallation() // successful Subscribe owns this
				} else if !errors.Is(got.err, context.Canceled) {
					t.Fatal(got.err)
				}
				if err := feed.events.Offer(querysubscription.Event{Progress: &querysubscription.Progress{Position: &querysubscription.Position{SourceID: "source", Value: 1}, Revision: 1}}); err != nil {
					t.Fatal(err)
				}
				if feed.events.Len() != 1 {
					t.Fatal("snapshot/cancellation race stranded unrelated progress")
				}
			}
		})
	}
}

func TestAbandonSnapshotAndDeliveryPublicationAreAtomic(t *testing.T) {
	for attempt := 0; attempt < 200; attempt++ {
		feed := responseTestFeed()
		wait := make(chan response, 1)
		feed.pending["request"] = wait
		feed.beginInstallation() // retain delivery until both contenders finish
		routeTestSnapshot(feed, "query", "request")
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		offered := make(chan error, 1)
		go func() {
			defer workers.Done()
			<-start
			offered <- feed.offerDelivery(&querysubscription.DeliveryEnvelope{
				Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryHydration, Revision: 1},
				Effects:  []querysubscription.SubscriptionEffect{{SubscriptionID: "canceled"}, {SubscriptionID: "healthy"}},
			})
		}()
		go func() {
			defer workers.Done()
			<-start
			feed.abandonRequest("request", wait, "canceled")
		}()
		close(start)
		workers.Wait()
		if err := <-offered; err != nil {
			t.Fatal(err)
		}
		feed.finishInstallation()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := feed.Next(ctx)
		cancel()
		if err != nil || got.Delivery == nil || len(got.Delivery.Effects) != 1 || got.Delivery.Effects[0].SubscriptionID != "healthy" {
			t.Fatalf("retired effects escaped concurrent publication: %+v, %v", got, err)
		}
	}
}

func TestCancelNonSubscribeRequestKeepsMembership(t *testing.T) {
	feed := responseTestFeed()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := feed.request(ctx, &queryv1.ClientMessage{Operation: &queryv1.ClientMessage_UpdateWindow{UpdateWindow: &queryv1.UpdateWindow{SubscriptionId: "healthy"}}})
		result <- err
	}()
	<-feed.requests
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("window cancellation: %v", err)
	}
	if feed.subscriptions["healthy"] == nil {
		t.Fatal("canceling a window operation retired the subscription")
	}
}
