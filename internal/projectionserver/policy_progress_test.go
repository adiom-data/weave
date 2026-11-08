package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type observedProgressFeed struct {
	q.Feed
	requests chan struct{}
}

func TestPolicyLoadedFactsAtConsumedCheckpointNeedNoFurtherReport(t *testing.T) {
	for _, mode := range []string{"projection", "direct", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			release := make(chan struct{})
			backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
				if request.Query.Relation.Name == "grants" {
					select {
					case <-release:
					case <-ctx.Done():
					}
					return policySnapshot(20, policyRow("grants", "grant", "allow"))
				}
				return policySnapshot(20, policyRow("projects", "project", "project"))
			})
			service, err := New(ctx, backend, policyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			var done <-chan error
			var stream *authorizationCommitStream
			if mode == "snapshot" {
				result := make(chan error, 1)
				done = result
				go func() {
					_, err := service.(*server).GetProjectionSnapshot(ctx, snapshotTestRequest(false, false))
					result <- err
				}()
			} else {
				stream = &authorizationCommitStream{ctx: ctx, received: make(chan *pb.ClientMessage, 1), sent: make(chan *pb.ServerMessage, 20)}
				result := make(chan error, 1)
				done = result
				go func() { result <- service.ServeStream(stream) }()
				request := &pb.ClientMessage{SubscriptionId: "view", RequestId: "open"}
				root := snapshotTestRequest(false, false).Root
				if mode == "projection" {
					request.Operation = &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}
				} else {
					request.Operation = &pb.ClientMessage_Subscribe{Subscribe: &pb.Subscribe{Query: root.GetList().Query, Window: root.GetList().Window}}
				}
				stream.received <- request
			}
			receivePolicyRequest(t, ctx, backend, "grants")
			backend.mu.Lock()
			feed := backend.feed
			backend.mu.Unlock()
			feed.mu.Lock()
			reports := feed.revision
			feed.mu.Unlock()
			if reports != 1 {
				t.Fatalf("initial progress reports = %d, want 1", reports)
			}
			close(release)
			if stream != nil {
				referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool { return message.GetReady() != nil })
			}
			if stream == nil {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else {
				cancel()
				<-done
			}
			feed.mu.Lock()
			final := feed.revision
			feed.mu.Unlock()
			if final != reports {
				t.Fatalf("local fact completion requested another report: %d -> %d", reports, final)
			}
		})
	}
}

func TestPolicyNewSubscriptionsReuseConsumedProgress(t *testing.T) {
	for _, projection := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "projection"}[projection], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
				return policySnapshot(20, policyRow("projects", "p1", "project"))
			})
			options := policyOptions()
			options.Policies.Definitions = policy.Map{{Namespace: "app", Name: "projects"}: {Allow: []policy.Rule{{ID: "all", When: "true"}}}}
			stream, done := openPolicyStreamOptions(t, ctx, backend, options)
			defer func() { cancel(); <-done }()
			for _, id := range []string{"first", "second"} {
				request := &pb.ClientMessage{SubscriptionId: id, RequestId: id}
				root := snapshotTestRequest(false, false).Root
				if projection {
					request.Operation = &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}
				} else {
					request.Operation = &pb.ClientMessage_Subscribe{Subscribe: &pb.Subscribe{Query: root.GetList().Query, Window: root.GetList().Window}}
				}
				stream.received <- request
				referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool { return message.SubscriptionId == id && message.GetReady() != nil })
			}
			backend.mu.Lock()
			feed := backend.feed
			backend.mu.Unlock()
			feed.mu.Lock()
			reports := feed.revision
			feed.mu.Unlock()
			if reports != 1 {
				t.Fatalf("subscriptions requested %d reports at the same checkpoint, want 1", reports)
			}
		})
	}
}

func TestPolicyMissingSnapshotPositionFailsOnlyThatSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		if request.Query.Relation.Name == "broken" {
			return q.QuerySnapshot{}
		}
		return policySnapshot(20, policyRow("projects", "p1", "project"))
	})
	options := policyOptions()
	options.Policies.Definitions = policy.Map{{Namespace: "app", Name: "projects"}: {Allow: []policy.Rule{{ID: "all", When: "true"}}}}
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	defer func() { cancel(); <-done }()
	for _, id := range []string{"broken", "projects"} {
		root := snapshotTestRequest(false, false).Root
		root.GetList().Query.Table = id
		stream.received <- &pb.ClientMessage{SubscriptionId: id, RequestId: id, Operation: &pb.ClientMessage_Subscribe{Subscribe: &pb.Subscribe{Query: root.GetList().Query, Window: root.GetList().Window}}}
		for {
			select {
			case message := <-stream.sent:
				if id == "broken" && message.SubscriptionId == id && message.GetError() != nil {
					if message.GetError().Code != "SUBSCRIBE_FAILED" {
						t.Fatal(message)
					}
					goto next
				}
				if id == "projects" && message.GetReady() != nil {
					goto next
				}
			case <-ctx.Done():
				t.Fatalf("healthy connection did not survive malformed snapshot: %v", ctx.Err())
			}
		}
	next:
	}
}

func (f *observedProgressFeed) RequestProgress(ctx context.Context) error {
	select {
	case f.requests <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPolicyProgressRequestsArePinnedToTargetsNotReplies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	evaluator, err := policy.Compile(policy.Map{}, policy.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	feed := &observedProgressFeed{requests: make(chan struct{}, 1024)}
	access, err := newPolicyAccess(ctx, feed, evaluator, nil, authorization.Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer access.Close()
	done := make(chan error, 1)
	go func() { done <- access.runProgress(ctx) }()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("watch stopped: %v", err)
		}
	}()
	receiveRequest := func() {
		t.Helper()
		select {
		case <-feed.requests:
		case <-ctx.Done():
			t.Fatal("progress request not issued")
		}
	}
	assertQuiet := func() {
		t.Helper()
		select {
		case <-feed.requests:
			t.Fatal("requested another report without a higher uncovered target")
		case <-time.After(25 * time.Millisecond):
		}
	}
	position := func(value uint64) *q.Position { return &q.Position{SourceID: "test", Value: value} }
	access.requestProgress(position(20))
	receiveRequest()
	// Repeated admission retries must not request the same target again.
	for range 1000 {
		access.requestProgress(position(20))
	}
	assertQuiet()
	// No reply is needed to unlock a higher demand. A request can emit nothing
	// when the upstream has not observed any source position yet.
	access.requestProgress(position(30))
	receiveRequest()
	if _, err := access.observe(q.Event{Progress: &q.Progress{Position: position(10), Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	access.requestProgress(position(30))
	assertQuiet() // An insufficient report does not start a polling loop.
	if _, err := access.observe(q.Event{Progress: &q.Progress{Position: position(40), Revision: 2, PreviousRevision: 1}}); err != nil {
		t.Fatal(err)
	}
	access.requestProgress(position(35))
	assertQuiet() // Already-consumed coverage needs no report.
}

func TestPolicyProgressCoalescesTargetsAndPreservesSourceIdentity(t *testing.T) {
	p := newPolicyProgress()
	point := func(source storage.SourceID, value uint64) *q.Position {
		return &q.Position{SourceID: source, Value: value}
	}
	p.require(point("a", 10), point("a", 20), point("b", 0))
	if !p.take() || p.take() {
		t.Fatal("demands did not coalesce")
	}
	p.observe(point("a", 25), 1)
	p.require(point("a", 24), point("b", 1))
	if !p.take() || p.sources["a"].requested != 20 || p.sources["b"].requested != 1 {
		t.Fatal("covered demand or another source changed the request watermark")
	}
}
