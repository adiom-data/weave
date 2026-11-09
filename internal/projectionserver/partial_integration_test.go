package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type failedPartialClient struct{ q.Client }
type failedPartialFeed struct{ q.Feed }

func (c failedPartialClient) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	feed, err := c.Client.OpenFeed(ctx, options)
	return failedPartialFeed{feed}, err
}
func (failedPartialFeed) SubscribeQuery(context.Context, q.QuerySubscribeRequest, q.QuerySnapshotInstaller) error {
	return errors.New("source query unavailable")
}

func TestPartialInitialSourceFailureReturnsAnAddressedErrorWithoutAFence(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "allow_all", true: "policy"}[policy], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			backend := failedPartialClient{newPolicyBackend(nil)}
			options := DefaultOptions()
			options.ProjectionLimits.RecoveryInitialDelay = time.Hour
			options.ProjectionLimits.RecoveryMaxDelay = time.Hour
			if policy {
				options.Relations = &RelationOptions{Definitions: relationdef.Definitions{{Namespace: "app", Name: "projects"}: {Visibility: `true`}}}
			}
			service, err := New(ctx, backend, options)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			request := snapshotTestRequest(true, false)
			request.AllowPartialErrors = true
			response, err := service.(*server).GetProjectionSnapshot(ctx, request)
			if err != nil || len(response.Errors) != 1 || response.Errors[0].Code != "QUERY_FAILED" || len(response.Errors[0].Path) != 1 {
				t.Fatalf("failed initial branch did not settle: %v, %v", response, err)
			}
			if response.Data.Fields["projects"].GetListValue() != nil {
				t.Fatal("failed expansion was returned as a successful list")
			}
		})
	}
}

func partialRows() (q.Row, q.Row) {
	good, bad := policyRow("projects", "good", "Good"), policyRow("projects", "bad", "Bad")
	good.Record["visible"] = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}
	bad.Record["visible"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("not a bool")}
	return good, bad
}

func TestPartialProjectionRecoversOnRowAndDefinitionUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	good, bad := partialRows()
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot { return policySnapshot(10, good, bad) })
	name := storage.RelationName{Namespace: "app", Name: "projects"}
	store, err := relationdef.NewStore(relationdef.Definitions{name: {Visibility: `item.visible`}}, relationdef.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions()
	options.Relations = &RelationOptions{Store: store}
	service, err := New(ctx, backend, options)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *pb.ClientMessage, 1), sent: make(chan *pb.ServerMessage, 32)}
	done := make(chan error, 1)
	go func() { done <- service.ServeStream(stream) }()
	defer func() { cancel(); <-done }()
	stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: "open", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: snapshotTestRequest(false, false).Root, AllowPartialErrors: true}}}
	initial := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if initial == nil || len(initial.Branches) != 1 || initial.Branches[0].GetError() == nil || len(initial.Entities) != 1 || initial.Entities[0].EntityId != publicEntityID(good.Key) {
		t.Fatalf("partial initial image: %v", initial)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("partial image never became ready: %v", message)
	}
	query := receivePolicyRequest(t, ctx, backend, "projects")
	bad.Record = storage.CloneRecord(bad.Record)
	bad.Record["visible"] = good.Record["visible"]
	backend.commit(t, 11, q.SubscriptionEffect{SubscriptionID: query.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: bad}}}})
	recovery := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if recovery == nil || recovery.Kind != pb.ModelBatch_KIND_COMMIT || !recovery.Atomic || len(recovery.Branches) != 1 || recovery.Branches[0].GetRecovered() == nil || len(recovery.Entities) != 1 || recovery.Entities[0].EntityId != publicEntityID(bad.Key) {
		t.Fatalf("row update did not recover atomically: %v", recovery)
	}
	for _, query := range recovery.Queries {
		if query.GetReset_() != nil {
			t.Fatal("row recovery unnecessarily reset healthy list")
		}
	}
	bad.Record = storage.CloneRecord(bad.Record)
	bad.Record["visible"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("broken again")}
	backend.commit(t, 12, q.SubscriptionEffect{SubscriptionID: query.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: bad}}}})
	failure := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if failure == nil || len(failure.Branches) != 1 || failure.Branches[0].GetError() == nil {
		t.Fatalf("second failure missing: %v", failure)
	}
	if err := store.Replace(relationdef.Definitions{name: {Visibility: `true`}}); err != nil {
		t.Fatal(err)
	}
	recovery = receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if recovery == nil || recovery.Kind != pb.ModelBatch_KIND_HYDRATION || recovery.Atomic || len(recovery.Branches) != 1 || recovery.Branches[0].GetRecovered() == nil {
		t.Fatalf("definition update did not recover at current state: %v", recovery)
	}
	select {
	case request := <-backend.opened:
		t.Fatalf("evaluation recovery reloaded ordinary query: %+v", request)
	default:
	}
}

func TestPartialOneShotReturnsDataAndAddressedErrors(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "fail_fast", true: "partial"}[partial], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			good, bad := partialRows()
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot { return policySnapshot(10, good, bad) })
			options := DefaultOptions()
			options.Relations = &RelationOptions{Definitions: relationdef.Definitions{{Namespace: "app", Name: "projects"}: {Visibility: `item.visible`}}}
			service, err := New(ctx, backend, options)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			request := snapshotTestRequest(false, false)
			request.AllowPartialErrors = partial
			response, err := service.(*server).GetProjectionSnapshot(ctx, request)
			if !partial {
				if err == nil || response != nil {
					t.Fatal("default one-shot unexpectedly returned partial data")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows := response.Data.Fields["projects"].GetListValue().Values
			if len(rows) != 2 || rows[0].GetStructValue().Fields["title"].GetStringValue() != "Good" || len(response.Errors) != 1 {
				t.Fatalf("partial snapshot: %v", response)
			}
			failure := response.Errors[0]
			if len(failure.Path) != 2 || failure.Path[0].GetField() != "projects" || failure.Path[1].GetIndex() != 1 || failure.Code != "EVALUATION_FAILED" {
				t.Fatalf("unaddressed snapshot error: %v", failure)
			}
		})
	}
}

// Fail the public query only after authorization has discovered a fact whose
// loader remains blocked. No later source traffic or fact completion can hide
// a missing admission wakeup. The production dependency cache still owns ID
// translation, ordered progress, and cancellation.
type admissionFailureClient struct{ backend *policyBackend }
type admissionFailureFeed struct {
	*policyBackendFeed
	root chan string
}

func (c admissionFailureClient) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	feed, err := c.backend.OpenFeed(ctx, options)
	if err != nil {
		return nil, err
	}
	return &admissionFailureFeed{policyBackendFeed: feed.(*policyBackendFeed), root: make(chan string, 1)}, nil
}

func (f *admissionFailureFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if request.Query.Relation.Name == "projects" {
		select {
		case f.root <- request.ID:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if request.Query.Relation.Name != "grants" {
		return f.policyBackendFeed.SubscribeQuery(ctx, request, install)
	}
	var root string
	select {
	case root = <-f.root:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case f.events <- q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: root, Cause: errors.New("public query failed during authorization")}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestPartialFailureDuringInitialAuthorizationReadmitsWithoutMoreTraffic(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		for _, synchronized := range []bool{false, true} {
			name := "live"
			if snapshot {
				name = "snapshot"
			}
			if synchronized {
				name += "-synchronized"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				backend := newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
					return policySnapshot(10, policyRow(request.Query.Relation.Name, "one", "Available"))
				})
				options := DefaultOptions()
				options.ProjectionLimits.RecoveryInitialDelay = time.Hour
				options.ProjectionLimits.RecoveryMaxDelay = time.Hour
				options.Relations = &RelationOptions{Definitions: relationdef.Definitions{
					{Namespace: "app", Name: "projects"}: {
						Facts:      []relationdef.Fact{{ID: "grant", Projection: &plan.Node{Name: "grants", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: "grants"}}}}},
						Visibility: `facts.grant.exists(g, true)`,
					},
					{Namespace: "app", Name: "notes"}: {Visibility: `true`},
				}}
				service, err := New(ctx, admissionFailureClient{backend}, options)
				if err != nil {
					t.Fatal(err)
				}
				defer service.Close()
				request := snapshotTestRequest(synchronized, false)
				request.AllowPartialErrors = true
				if snapshot {
					response, err := service.(*server).GetProjectionSnapshot(ctx, request)
					if err != nil {
						t.Fatal("partial failure stalled admission:", err)
					}
					if len(response.Errors) != 1 || response.Errors[0].Code != "QUERY_FAILED" || response.Data.Fields["projects"].GetListValue() != nil {
						t.Fatalf("failed branch was not returned explicitly: %v", response)
					}
					return
				}
				stream := &authorizationCommitStream{ctx: ctx, received: make(chan *pb.ClientMessage, 2), sent: make(chan *pb.ServerMessage, 16)}
				done := make(chan error, 1)
				go func() { done <- service.ServeStream(stream) }()
				defer func() { cancel(); <-done }()
				stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: "open", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: request.Root, WaitForSync: synchronized, AllowPartialErrors: true}}}
				message := receiveAuthorizationMessage(t, stream.sent)
				initial := message.GetModelBatch()
				if message.SubscriptionId != "view" || initial == nil || initial.Kind != pb.ModelBatch_KIND_SNAPSHOT || len(initial.Entities) != 0 || len(initial.Branches) != 1 || initial.Branches[0].GetError().GetCode() != "QUERY_FAILED" {
					t.Fatalf("partial initial error did not publish: %v", message)
				}
				if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
					t.Fatalf("partial initial error did not become ready: %v", message)
				}
				// Recovery isolation includes the rest of the connection, not just
				// siblings of the failed branch.
				notes := snapshotTestRequest(false, false).Root
				notes.NodeId, notes.GetList().Query.Table = "notes", "notes"
				stream.received <- &pb.ClientMessage{SubscriptionId: "healthy", RequestId: "notes", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: notes}}}
				message = receiveAuthorizationMessage(t, stream.sent)
				if message.SubscriptionId != "healthy" || message.GetModelBatch() == nil || len(message.GetModelBatch().Entities) != 1 {
					t.Fatalf("partial failure blocked a healthy projection: %v", message)
				}
				if message := receiveAuthorizationMessage(t, stream.sent); message.SubscriptionId != "healthy" || message.GetReady() == nil {
					t.Fatalf("healthy projection did not become ready: %v", message)
				}
			})
		}
	}
}
