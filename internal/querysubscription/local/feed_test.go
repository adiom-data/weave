package local

import (
	"context"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func newFeedTestFeed() *localFeed {
	return &localFeed{group: "feed", events: feedbuffer.New(8), bufferLimit: 8,
		subscriptions: make(map[string]*localSubscription), registered: make(chan struct{}, 1),
		deliveries: make(map[uint64]*pendingDelivery)}
}

func TestClosedFeedCannotPublishInFlightConversion(t *testing.T) {
	ctx := context.Background()
	feed, err := New(coordinatorStub{unsubscribe: func(context.Context, string) error { return nil }}).openFeed(ctx, q.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	relation := storage.RelationKey{Source: "test", Name: "items"}
	plan := blockingSnapshotPlan{nonComparingPlan: nonComparingPlan{relation: relation}, started: started, release: release, once: &sync.Once{}}
	sub := &localSubscription{id: "items", nativeID: feed.nativeID("items"), owner: feed, coordinator: feed.coordinator, plan: plan, installed: true}
	feed.mu.Lock()
	feed.subscriptions[sub.id] = sub
	feed.mu.Unlock()
	message := testGroup(feed, 1, 0, sub.id)
	message.Batches[0].Events = []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}}}
	done := make(chan error, 1)
	go func() { done <- feed.acceptGroup(message) }()
	<-started
	if err := feed.Close(ctx); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != q.ErrClosed {
		t.Fatal("closed feed accepted an in-flight delivery", err)
	}
	feed.retireAfter(sub.nativeID, 2)
	if len(feed.retirements) != 0 || len(feed.deliveries) != 0 {
		t.Fatal("closed feed retained new work")
	}
}

func testGroup(feed *localFeed, revision, previous uint64, ids ...string) delivery.GroupMessage {
	var message delivery.GroupMessage
	for _, id := range ids {
		message.Batches = append(message.Batches, delivery.SubscriptionBatch{
			SubscriptionID: feed.nativeID(id), Delivery: delivery.DeliveryCommit,
			Checkpoint: &q.Position{SourceID: "test", Value: revision}, Revision: revision, PreviousGroupRevision: previous,
		})
	}
	return message
}

func TestFeedGroupsSubscriptionEffectsIntoOneDelivery(t *testing.T) {
	feed := newFeedTestFeed()
	if err := feed.acceptGroup(testGroup(feed, 1, 0, "left", "right")); err != nil {
		t.Fatal(err)
	}
	if feed.events.Len() != 1 {
		t.Fatal("one complete group must publish exactly one delivery")
	}
	event, err := feed.Next(context.Background())
	if err != nil || event.Delivery == nil || event.Delivery.Delivery.Revision != 1 {
		t.Fatal(event, err)
	}
}

func TestFeedRejectsMalformedGroupWithoutPublishing(t *testing.T) {
	for _, mode := range []string{"duplicate member", "metadata mismatch", "foreign feed"} {
		t.Run(mode, func(t *testing.T) {
			feed := newFeedTestFeed()
			group := testGroup(feed, 1, 0, "left", "right")
			switch mode {
			case "duplicate member":
				group.Batches[1].SubscriptionID = group.Batches[0].SubscriptionID
			case "metadata mismatch":
				group.Batches[1].Revision = 2
			case "foreign feed":
				group.Batches[1].SubscriptionID = "other\x00right"
			}
			if err := feed.acceptGroup(group); err == nil {
				t.Fatal("malformed group accepted")
			}
			if feed.events.Len() != 0 {
				t.Fatal("partial malformed group published")
			}
		})
	}
}

func TestFeedProgressWaitsForCompletePredecessorDelivery(t *testing.T) {
	feed := newFeedTestFeed()
	if err := feed.acceptProgress(delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: 8}, Revision: 8, PreviousGroupRevision: 7}); err != nil {
		t.Fatal(err)
	}
	if err := feed.acceptGroup(testGroup(feed, 9, 8, "left")); err != nil {
		t.Fatal(err)
	}
	if feed.events.Len() != 0 {
		t.Fatal("progress overtook predecessor")
	}
	if err := feed.acceptGroup(testGroup(feed, 7, 0, "left", "right")); err != nil {
		t.Fatal(err)
	}
	for want := uint64(7); want <= 9; want++ {
		event, err := feed.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got := uint64(0)
		if event.Progress != nil {
			got = event.Progress.Revision
		} else {
			got = event.Delivery.Delivery.Revision
		}
		if got != want {
			t.Fatalf("got revision %d, want %d", got, want)
		}
	}
}

func TestFeedReordersConcurrentProgressResponses(t *testing.T) {
	feed := newFeedTestFeed()
	position := &q.Position{SourceID: "test", Value: 42}
	if err := feed.acceptProgress(delivery.ProgressReport{Checkpoint: position, Revision: 2, PreviousGroupRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if feed.events.Len() != 0 {
		t.Fatal("progress overtook predecessor")
	}
	if err := feed.acceptProgress(delivery.ProgressReport{Checkpoint: position, Revision: 1}); err != nil {
		t.Fatal(err)
	}
	for want := uint64(1); want <= 2; want++ {
		event, err := feed.Next(context.Background())
		if err != nil || event.Progress == nil || event.Progress.Revision != want {
			t.Fatal(event, err)
		}
	}
}

func TestFeedBoundsSharedReorderBuffer(t *testing.T) {
	for _, progress := range []bool{false, true} {
		feed := newFeedTestFeed()
		feed.bufferLimit = 1
		accept := func(revision uint64) error {
			if progress {
				return feed.acceptProgress(delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: revision}, Revision: revision, PreviousGroupRevision: revision - 1})
			}
			return feed.acceptGroup(testGroup(feed, revision, revision-1, "items"))
		}
		if err := accept(2); err != nil {
			t.Fatal(err)
		}
		if err := accept(3); err != q.ErrBackpressure {
			t.Fatalf("overflow=%v", err)
		}
	}
}

func TestFeedRetirementWaitsForAppliedRevisionWithoutQueueSpace(t *testing.T) {
	for _, alreadyApplied := range []bool{false, true} {
		feed := newFeedTestFeed()
		sub := &localSubscription{id: "items", nativeID: feed.nativeID("items"), owner: feed, installed: true, closed: true}
		feed.subscriptions[sub.id] = sub
		apply := func() {
			if err := feed.acceptGroup(testGroup(feed, 1, 0, sub.id)); err != nil {
				t.Fatal(err)
			}
		}
		if alreadyApplied {
			apply()
		}
		feed.retireAfter(sub.nativeID, 1)
		if !alreadyApplied {
			if feed.subscriptions[sub.id] == nil {
				t.Fatal("routing retired before accepted delivery")
			}
			apply()
		}
		if len(feed.subscriptions) != 0 || len(feed.retirements) != 0 {
			t.Fatal("retirement leaked routing")
		}
		if feed.events.Len() != 1 {
			t.Fatal("retirement consumed output capacity")
		}
	}
}

func TestFeedRetirementFloorCanBeAnOutstandingProgressResponse(t *testing.T) {
	feed := newFeedTestFeed()
	sub := &localSubscription{id: "items", nativeID: feed.nativeID("items"), owner: feed, installed: true, closed: true}
	feed.subscriptions[sub.id] = sub
	feed.retireAfter(sub.nativeID, 2)
	if err := feed.acceptGroup(testGroup(feed, 1, 0, sub.id)); err != nil {
		t.Fatal(err)
	}
	if feed.subscriptions[sub.id] == nil {
		t.Fatal("retired before progress floor")
	}
	if err := feed.acceptProgress(delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: 1}, Revision: 2, PreviousGroupRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if len(feed.subscriptions) != 0 || len(feed.retirements) != 0 {
		t.Fatal("progress did not release routing")
	}
}
