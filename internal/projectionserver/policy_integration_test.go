package projectionserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// This backend supplies complete snapshots and ordered transaction/progress
// envelopes. The production dependency cache, fact runtime, projection actor,
// visibility gate, and public model assembly all run unchanged in these tests.
type policyBackend struct {
	mu       sync.Mutex
	opened   chan q.QuerySubscribeRequest
	feed     *policyBackendFeed
	snapshot func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot
}
type policyBackendFeed struct {
	mu       sync.Mutex
	backend  *policyBackend
	events   chan q.Event
	ctx      context.Context
	cancel   context.CancelFunc
	revision uint64
	through  uint64
}

func newPolicyBackend(snapshot func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot) *policyBackend {
	return &policyBackend{opened: make(chan q.QuerySubscribeRequest, 100), snapshot: snapshot}
}
func (b *policyBackend) OpenFeed(ctx context.Context, _ q.FeedOptions) (q.Feed, error) {
	run, cancel := context.WithCancel(ctx)
	f := &policyBackendFeed{backend: b, ctx: run, cancel: cancel, events: make(chan q.Event, 100)}
	b.mu.Lock()
	b.feed = f
	b.mu.Unlock()
	return f, nil
}
func (f *policyBackendFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	f.backend.opened <- request
	snapshot := f.backend.snapshot(ctx, request)
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	snapshot.Revision = f.revision
	f.mu.Unlock()
	if err := install(&policyBackendHandle{id: request.ID}, snapshot); err != nil {
		return err
	}
	f.mu.Lock()
	if snapshot.Checkpoint != nil && snapshot.Checkpoint.Value > f.through {
		f.through = snapshot.Checkpoint.Value
	}
	f.mu.Unlock()
	return nil
}
func (f *policyBackendFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	return f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: request.ID, Query: request.Query, Window: q.InitialWindow{Count: 1}}, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
		entity := q.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			row := snapshot.Rows[0].Row
			entity.Row = &row
		}
		return install(handle, entity)
	})
}
func (f *policyBackendFeed) RequestProgress(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	previous := f.revision
	f.revision++
	select {
	case f.events <- q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: f.through}, Revision: f.revision, PreviousRevision: previous}}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *policyBackendFeed) Next(ctx context.Context) (q.Event, error) {
	select {
	case event := <-f.events:
		return event, nil
	case <-ctx.Done():
		return q.Event{}, ctx.Err()
	case <-f.ctx.Done():
		return q.Event{}, q.ErrClosed
	}
}
func (f *policyBackendFeed) Close(context.Context) error { f.cancel(); return nil }
func (b *policyBackend) commit(t *testing.T, position uint64, effects ...q.SubscriptionEffect) {
	t.Helper()
	b.mu.Lock()
	f := b.feed
	b.mu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	if position <= f.through {
		t.Fatalf("test commit %d must follow %d", position, f.through)
	}
	f.through = position
	previous := f.revision
	f.revision++
	f.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: position}, Atomic: true, Revision: f.revision, PreviousRevision: previous}, Effects: effects}}
}

type policyBackendHandle struct{ id string }

func (h *policyBackendHandle) ID() string                { return h.id }
func (*policyBackendHandle) Close(context.Context) error { return nil }
func (*policyBackendHandle) UpdateWindow(context.Context, []q.BoundaryChange, string) error {
	return errors.New("not used in this test")
}

func policyRow(table, id, title string) q.Row {
	return q.Row{Key: modelKey(storage.RelationKey{Source: "test", Namespace: "app", Name: table, OpaqueID: table}, id), Record: modelRecord(id, "1", title), Cursor: []byte(id)}
}
func policySnapshot(point uint64, rows ...q.Row) q.QuerySnapshot {
	snapshot := modelSnapshot(rows...)
	snapshot.Checkpoint = &q.Position{SourceID: "test", Value: point}
	snapshot.Fence = &q.Position{SourceID: "test", Value: point}
	return snapshot
}
func policyOptions() Options {
	return Options{Policies: &PolicyOptions{Definitions: policy.Map{{Namespace: "app", Name: "projects"}: {Facts: []policy.Fact{testPolicyFact("grant", storage.RelationName{Namespace: "app", Name: "grants"}, true)}, Allow: []policy.Rule{{ID: "grant", When: `facts.grant.exists(g,true)`}}}}, Limits: policy.Limits{MaxNodes: 1000, MaxDepth: 30, MaxEvaluationSteps: 10000}, MaxFactQueries: 100, MaxFactRows: 10, FactLoadConcurrency: 2}, ClaimsProvider: func(context.Context) (accesscontrol.Claims, error) { return accesscontrol.Claims{}, nil }}
}
func openPolicyStream(t *testing.T, ctx context.Context, backend *policyBackend) (*authorizationCommitStream, chan error) {
	t.Helper()
	return openPolicyStreamOptions(t, ctx, backend, policyOptions())
}
func openPolicyStreamOptions(t *testing.T, ctx context.Context, backend *policyBackend, options Options) (*authorizationCommitStream, chan error) {
	t.Helper()
	service, err := New(ctx, backend, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 10), sent: make(chan *reactivev1.ServerMessage, 100)}
	done := make(chan error, 1)
	go func() { done <- service.(*server).Open(stream) }()
	return stream, done
}

func TestDynamicPolicyInvalidationReevaluatesIdleProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		return policySnapshot(10, policyRow("projects", "p1", "retained"))
	})
	var mu sync.Mutex
	current := policy.Snapshot{Version: "allow", Policy: &policy.Relation{Allow: []policy.Rule{{ID: "visible", When: "true"}}}}
	hints := make(chan []policy.Key, 1)
	options := policyOptions()
	options.Policies.Definitions = nil
	options.Policies.Provider = policy.ProviderFunc(func(context.Context, policy.Key) (policy.Snapshot, error) {
		mu.Lock()
		defer mu.Unlock()
		return current, nil
	})
	options.Policies.Invalidations = hints
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream did not stop")
		}
	}()
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(false, false).Root}}}
	initial := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_SNAPSHOT, "")
	if len(initial.Entities) != 1 {
		t.Fatalf("missing initial entity: %v", initial)
	}
	mu.Lock()
	current = policy.Snapshot{Version: "deny", Policy: &policy.Relation{}}
	mu.Unlock()
	hints <- []policy.Key{{Namespace: "app", Name: "projects"}}
	revoked := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_HYDRATION, "")
	if len(revoked.Entities) != 1 || revoked.Entities[0].GetReleased() == nil {
		t.Fatalf("idle revocation not published: %v", revoked)
	}
	mu.Lock()
	current = policy.Snapshot{Version: "allow-again", Policy: &policy.Relation{Allow: []policy.Rule{{ID: "visible", When: "true"}}}}
	mu.Unlock()
	hints <- []policy.Key{{Namespace: "app", Name: "projects"}}
	restored := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_HYDRATION, "")
	if len(restored.Entities) != 1 || restored.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "retained" {
		t.Fatalf("idle restoration lost data: %v", restored)
	}
}
func receivePolicyRequest(t *testing.T, ctx context.Context, backend *policyBackend, table string) q.QuerySubscribeRequest {
	t.Helper()
	for {
		select {
		case request := <-backend.opened:
			if request.Query.Relation.Name == table {
				return request
			}
		case <-ctx.Done():
			t.Fatalf("no %s subscription", table)
			return q.QuerySubscribeRequest{}
		}
	}
}
func receivePolicyBatch(t *testing.T, ctx context.Context, stream *authorizationCommitStream, kind reactivev1.ModelBatch_Kind, checkpoint string) *reactivev1.ModelBatch {
	t.Helper()
	for {
		select {
		case message := <-stream.sent:
			if failure := message.GetError(); failure != nil {
				t.Fatalf("policy stream failed: %v", failure)
			}
			if batch := message.GetModelBatch(); batch != nil && batch.Kind == kind && (checkpoint == "" || batch.Checkpoint == checkpoint) {
				return batch
			}
		case <-ctx.Done():
			t.Fatal("no expected policy batch")
			return nil
		}
	}
}

func TestPolicyInitializationWaitsForFactsAndKeepsCurrentImage(t *testing.T) {
	for _, projection := range []bool{true, false} {
		t.Run(map[bool]string{true: "projection", false: "direct"}[projection], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			release := make(chan struct{})
			backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
				if request.Query.Relation.Name == "grants" {
					select {
					case <-release:
					case <-ctx.Done():
					}
					return policySnapshot(20, policyRow("grants", "g1", "grant"))
				}
				return policySnapshot(10, policyRow("projects", "p1", "old"))
			})
			stream, done := openPolicyStream(t, ctx, backend)
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("policy stream did not stop")
				}
			}()
			root := snapshotTestRequest(false, false).Root
			message := &reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "subscribe"}
			if projection {
				message.Operation = &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root}}
			} else {
				message.Operation = &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{Query: root.GetList().Query, Window: root.GetList().Window}}
			}
			stream.received <- message
			projectRequest := receivePolicyRequest(t, ctx, backend, "projects")
			grantRequest := receivePolicyRequest(t, ctx, backend, "grants")
			select {
			case early := <-stream.sent:
				t.Fatalf("published before required facts: %v", early)
			default:
			}
			updated := policyRow("projects", "p1", "new")
			backend.commit(t, 15, q.SubscriptionEffect{SubscriptionID: projectRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: updated}}}})
			close(release)
			initial := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_SNAPSHOT, "")
			if len(initial.Entities) != 1 || initial.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "new" {
				t.Fatalf("initial snapshot was stale: %v", initial)
			}
			select {
			case ready := <-stream.sent:
				if ready.GetReady() == nil {
					t.Fatalf("expected readiness: %v", ready)
				}
			case <-ctx.Done():
				t.Fatal("no readiness")
			}
			grant := policyRow("grants", "g1", "grant")
			backend.commit(t, 25, q.SubscriptionEffect{SubscriptionID: grantRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Removed, Row: q.Row{Key: grant.Key}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{grant.Key}}}})
			revoked := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_COMMIT, "25")
			if len(revoked.Entities) != 1 || revoked.Entities[0].GetReleased() == nil {
				t.Fatalf("grant revocation not propagated: %v", revoked)
			}
			backend.commit(t, 30, q.SubscriptionEffect{SubscriptionID: grantRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Added, Row: grant}}, Membership: q.MembershipDelta{Insertions: []q.Insertion{{Key: grant.Key, FinalIndex: 0}}}}})
			restored := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_COMMIT, "30")
			if len(restored.Entities) != 1 || restored.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "new" {
				t.Fatalf("visibility restoration lost retained data: %v", restored)
			}
		})
	}
}

func TestPolicyOneShotSnapshotWaitsForVisibility(t *testing.T) {
	for _, synchronized := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "synchronized"}[synchronized], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			release := make(chan struct{})
			backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
				if request.Query.Relation.Name == "grants" {
					select {
					case <-release:
					case <-ctx.Done():
					}
					return policySnapshot(20, policyRow("grants", "g1", "grant"))
				}
				return policySnapshot(10, policyRow("projects", "p1", "old"))
			})
			service, err := New(ctx, backend, policyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			type outcome struct {
				response *reactivev1.ProjectionSnapshotResponse
				err      error
			}
			done := make(chan outcome, 1)
			go func() {
				response, err := service.(*server).GetProjectionSnapshot(ctx, snapshotTestRequest(synchronized, false))
				done <- outcome{response, err}
			}()
			root := receivePolicyRequest(t, ctx, backend, "projects")
			receivePolicyRequest(t, ctx, backend, "grants")
			backend.commit(t, 15, q.SubscriptionEffect{SubscriptionID: root.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("projects", "p1", "new")}}}})
			close(release)
			select {
			case result := <-done:
				if result.err != nil {
					t.Fatal(result.err)
				}
				rows := result.response.Data.Fields["projects"].GetListValue().Values
				if len(rows) != 1 || rows[0].GetStructValue().Fields["title"].GetStringValue() != "new" {
					t.Fatalf("unauthorized or stale snapshot: %v", result.response)
				}
				if result.response.Extensions.Synchronized != synchronized {
					t.Fatal("changed snapshot mode")
				}
			case <-ctx.Done():
				t.Fatal("one-shot snapshot did not finish authorization")
			}
		})
	}
}

func TestPolicyLiveUnknownKeepsPublishedImageUntilCurrentDecision(t *testing.T) {
	for _, mode := range []string{"direct", "projection", "nested-projection"} {
		projection := mode != "direct"
		for _, granted := range []bool{false, true} {
			name := mode + "/" + map[bool]string{false: "deny", true: "allow"}[granted]
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				release := make(chan struct{})
				backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
					switch request.Query.Relation.Name {
					case "grants":
						return policySnapshot(10, policyRow("grants", "g1", "grant"))
					case "backup":
						select {
						case <-release:
						case <-ctx.Done():
						}
						if granted {
							return policySnapshot(30, policyRow("backup", "b1", "grant"))
						}
						return policySnapshot(30)
					case "notes":
						return policySnapshot(10, policyRow("notes", "n1", "note"))
					case "tasks":
						return policySnapshot(10, policyRow("tasks", "t1", "task"))
					default:
						return policySnapshot(10, policyRow("projects", "p1", "old"))
					}
				})
				options := policyOptions()
				project := storage.RelationName{Namespace: "app", Name: "projects"}
				definition := options.Policies.Definitions[project]
				definition.Facts = append(definition.Facts, testPolicyFact("backup", storage.RelationName{Namespace: "app", Name: "backup"}, true))
				definition.Allow[0].When = `facts.grant.exists(g,true) || facts.backup.exists(g,true)`
				if mode == "nested-projection" {
					definition.Propagate = []policy.Edge{{ID: "tasks", To: storage.RelationName{Namespace: "app", Name: "tasks"}, When: `parent.id == parent.id`}}
					options.Policies.Definitions[storage.RelationName{Namespace: "app", Name: "tasks"}] = policy.Relation{}
				}
				options.Policies.Definitions[project] = definition
				options.Policies.Definitions[storage.RelationName{Namespace: "app", Name: "notes"}] = policy.Relation{Allow: []policy.Rule{{ID: "public", When: `item.id == item.id`}}}
				service, err := New(ctx, backend, options)
				if err != nil {
					t.Fatal(err)
				}
				defer service.Close()
				stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 10), sent: make(chan *reactivev1.ServerMessage, 100)}
				done := make(chan error, 1)
				go func() { done <- service.(*server).Open(stream) }()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("stream failed to stop")
					}
				}()
				root := snapshotTestRequest(false, mode == "nested-projection").Root
				request := &reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "project"}
				if projection {
					request.Operation = &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root}}
				} else {
					request.Operation = &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{Query: root.GetList().Query, Window: root.GetList().Window}}
				}
				stream.received <- request
				projectRequest := receivePolicyRequest(t, ctx, backend, "projects")
				grantRequest := receivePolicyRequest(t, ctx, backend, "grants")
				receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_SNAPSHOT, "")
				<-stream.sent // Ready
				stream.received <- &reactivev1.ClientMessage{SubscriptionId: "note", RequestId: "note", Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{Query: &reactivev1.Query{Schema: "app", Table: "notes", Columns: root.GetList().Query.Columns}, Window: root.GetList().Window}}}
				noteRequest := receivePolicyRequest(t, ctx, backend, "notes")
				receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_SNAPSHOT, "")
				<-stream.sent // Ready
				projectKey := policyRow("projects", "p1", "").Key
				for _, point := range []uint64{20, 25} {
					effects := []q.SubscriptionEffect{
						{SubscriptionID: projectRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("projects", "p1", "latest")}}}},
						{SubscriptionID: noteRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("notes", "n1", checkpointString(&q.Position{Value: point}))}}}},
					}
					if point == 20 {
						grant := policyRow("grants", "g1", "")
						effects = append(effects, q.SubscriptionEffect{SubscriptionID: grantRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Removed, Row: q.Row{Key: grant.Key}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{grant.Key}}}})
					}
					backend.commit(t, point, effects...)
					if point == 20 {
						receivePolicyRequest(t, ctx, backend, "backup")
					}
					batch := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_COMMIT, checkpointString(&q.Position{Value: point}))
					for _, entity := range batch.Entities {
						if entity.EntityId == publicEntityID(projectKey) {
							t.Fatalf("published unresolved project data or flickered: %v", batch)
						}
					}
					if len(batch.Entities) != 1 {
						t.Fatalf("unrelated subscription stalled: %v", batch)
					}
				}
				close(release)
				resolved := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_HYDRATION, "30")
				expectedEntities := 1
				if mode == "nested-projection" && !granted {
					expectedEntities++
				}
				if len(resolved.Entities) != expectedEntities {
					t.Fatalf("bad reconciliation: %v", resolved)
				}
				var entity *reactivev1.EntityEvent
				for _, candidate := range resolved.Entities {
					if candidate.EntityId == publicEntityID(projectKey) {
						entity = candidate
					}
				}
				if entity == nil {
					t.Fatalf("no project reconciliation: %v", resolved)
				}
				if granted {
					if entity.GetUpsert().GetValues()["title"].GetTextValue() != "latest" {
						t.Fatalf("did not publish the current record: %v", resolved)
					}
				} else if entity.GetReleased() == nil {
					t.Fatalf("did not remove denied entity: %v", resolved)
				}
				for _, query := range resolved.Queries {
					if query.GetReset_() != nil {
						t.Fatalf("existing list was reset instead of diffed: %v", resolved)
					}
				}
			})
		}
	}
}

func TestPolicyPropagationReevaluatesDescendantsOnParentChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	project := policyRow("projects", "p1", "project")
	project.Record["propagate"] = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}
	link := policyRow("project_tasks", "l1", "link")
	link.Record["project_id"] = project.Record["id"]
	link.Record["task_id"] = policyRow("tasks", "t1", "task").Record["id"]
	backend := newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		switch request.Query.Relation.Name {
		case "projects":
			return policySnapshot(10, project)
		case "project_tasks":
			return policySnapshot(10, link)
		case "tasks":
			return policySnapshot(10, policyRow("tasks", "t1", "task"))
		default:
			return policySnapshot(10, policyRow("grants", "g1", "grant"))
		}
	})
	options := policyOptions()
	projectName := storage.RelationName{Namespace: "app", Name: "projects"}
	linkName := storage.RelationName{Namespace: "app", Name: "project_tasks"}
	taskName := storage.RelationName{Namespace: "app", Name: "tasks"}
	definition := options.Policies.Definitions
	rootPolicy := definition[projectName]
	rootPolicy.Propagate = []policy.Edge{{ID: "project-links", To: linkName, When: `parent.id == item.project_id && parent.propagate == true`}}
	definition[projectName] = rootPolicy
	definition[linkName] = policy.Relation{Propagate: []policy.Edge{{ID: "link-task", To: taskName, When: `parent.task_id == item.id`}}}
	definition[taskName] = policy.Relation{}
	service, err := New(ctx, backend, options)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 10), sent: make(chan *reactivev1.ServerMessage, 100)}
	done := make(chan error, 1)
	go func() { done <- service.(*server).Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream failed to stop")
		}
	}()
	root := snapshotTestRequest(false, true).Root
	task := root.Children[0]
	root.Children = []*reactivev1.ProjectionNode{{NodeId: "links", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{Query: &reactivev1.Query{Schema: "app", Table: "project_tasks", Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"title"}}}}, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10}}}, Children: []*reactivev1.ProjectionNode{task}}}
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "p", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root}}}
	rootRequest := receivePolicyRequest(t, ctx, backend, "projects")
	initial := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_SNAPSHOT, "")
	if len(initial.Entities) != 3 {
		t.Fatalf("propagation did not expose the valid path: %v", initial)
	}
	<-stream.sent // Ready
	updated := policyRow("projects", "p1", "project")
	updated.Record["propagate"] = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("false")}
	backend.commit(t, 20, q.SubscriptionEffect{SubscriptionID: rootRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: updated}}}})
	pruned := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_COMMIT, "20")
	released := 0
	for _, entity := range pruned.Entities {
		if entity.GetReleased() != nil {
			released++
		}
	}
	if released != 2 {
		t.Fatalf("descendants retained stale propagation: %v", pruned)
	}
	backend.commit(t, 25, q.SubscriptionEffect{SubscriptionID: rootRequest.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: project}}}})
	upserts := 0
	// Parent updates intentionally re-expand descendants. The commit removes
	// old occurrences; newly loaded descendants publish through later windows.
	for upserts < 2 {
		select {
		case message := <-stream.sent:
			if failure := message.GetError(); failure != nil {
				t.Fatal(failure)
			}
			if batch := message.GetModelBatch(); batch != nil {
				for _, entity := range batch.Entities {
					if entity.GetUpsert() != nil {
						upserts++
					}
				}
			}
		case <-ctx.Done():
			t.Fatal("descendants could not be restored")
		}
	}
}
