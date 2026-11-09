package projectionserver

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func computedOptions() Options {
	options := policyOptions()
	options.Relations.AllowAll = true
	options.Relations.Definitions = relationdef.Definitions{{Namespace: "app", Name: "projects"}: {
		Facts: []relationdef.Fact{testPolicyFact("grant", storage.RelationName{Namespace: "app", Name: "grants"}, true)},
		Computed: map[string]relationdef.Computed{
			"canEdit": {Type: storage.TypeBool, Expression: `facts.grant.exists(g,true)`},
			"unused":  {Type: storage.TypeBool, Expression: `facts.grant.exists(g,true)`},
		},
	}}
	return options
}

func TestPrivateComputedSelectionsFailOnlyTheirProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	options := computedOptions()
	options.Relations.AllowAll = false
	options.Relations.Definitions[storage.RelationName{Namespace: "app", Name: "projects"}] = relationdef.Relation{
		Computed: map[string]relationdef.Computed{
			"_role":   {Type: storage.TypeInteger, Expression: "2"},
			"canEdit": {Type: storage.TypeBool, Expression: `computed._role >= 2`},
		}, Visibility: `computed._role >= 1`,
	}
	service, err := New(ctx, backend, options)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	request := snapshotTestRequest(false, false)
	request.Root.Computed = []string{"_role"}
	if response, err := service.(*server).GetProjectionSnapshot(ctx, request); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("private snapshot accepted: %v %v", response, err)
	}
	select {
	case <-backend.opened:
		t.Fatal("private snapshot issued upstream query")
	default:
	}
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *pb.ClientMessage, 10), sent: make(chan *pb.ServerMessage, 100)}
	done := make(chan error, 1)
	go func() { done <- service.(*server).Open(stream) }()
	defer func() { cancel(); <-done }()
	stream.received <- &pb.ClientMessage{SubscriptionId: "private", RequestId: "private", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: request.Root}}}
waitForRejection:
	for {
		select {
		case message := <-stream.sent:
			if failure := message.GetError(); failure != nil {
				if message.SubscriptionId != "private" || failure.Code != "INVALID_PROJECTION" {
					t.Fatalf("unexpected failure: %v", message)
				}
				break waitForRejection
			}
		case <-ctx.Done():
			t.Fatal("private selection did not fail")
		}
	}
	select {
	case <-backend.opened:
		t.Fatal("private stream issued upstream query")
	default:
	}
	root := snapshotTestRequest(false, false).Root
	root.Computed = []string{"canEdit"}
	stream.received <- &pb.ClientMessage{SubscriptionId: "public", RequestId: "public", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
	initial := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
	if len(initial.Computed) != 1 || len(initial.Computed[0].GetUpsert().Fields) != 1 || !initial.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatalf("public result leaked private helper or failed: %v", initial)
	}
}

func TestComputedFloatsPreserveCanonicalWireValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	row := func(input string) q.Row {
		t.Helper()
		value, err := storage.ParseScalar(storage.TypeFloat, []byte(input))
		if err != nil {
			t.Fatal(err)
		}
		r := policyRow("projects", "p1", "project")
		r.Record["score"] = value
		return r
	}
	initialRow := row("Infinity")
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		return policySnapshot(10, initialRow)
	})
	options := computedOptions()
	options.Relations.Definitions[storage.RelationName{Namespace: "app", Name: "projects"}] = relationdef.Relation{Computed: map[string]relationdef.Computed{
		"direct": {Type: storage.TypeFloat, Expression: `item.score`},
		"copy":   {Type: storage.TypeFloat, Expression: `computed.direct`},
	}}
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	defer func() { cancel(); <-done }()
	root := snapshotTestRequest(false, false).Root
	root.Computed = []string{"copy"}
	stream.received <- &pb.ClientMessage{SubscriptionId: "p", RequestId: "subscribe", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
	initial := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
	check := func(batch *pb.ModelBatch, want string) {
		t.Helper()
		if len(batch.Computed) != 1 || batch.Computed[0].GetUpsert().GetFields()["copy"].GetFloatValue() != want {
			t.Fatalf("computed float should be %q: %v", want, batch)
		}
	}
	check(initial, "Infinity")
	request := receivePolicyRequest(t, ctx, backend, "projects")
	for i, input := range []string{"-Infinity", "NaN", "-0", "1.5"} {
		r := row(input)
		position := uint64(20 + i*10)
		backend.commit(t, position, q.SubscriptionEffect{SubscriptionID: request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: r}}}})
		batch := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_COMMIT, strconv.FormatUint(position, 10))
		check(batch, string(r.Record["score"].Data))
	}
}

func TestComputedFactChangesPublishWithoutChangingEntity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(_ context.Context, r q.QuerySubscribeRequest) q.QuerySnapshot {
		if r.Query.Relation.Name == "grants" {
			return policySnapshot(10, policyRow("grants", "g1", "grant"))
		}
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	stream, done := openPolicyStreamOptions(t, ctx, backend, computedOptions())
	defer func() { cancel(); <-done }()
	root := snapshotTestRequest(false, false).Root
	root.Computed = []string{"canEdit"}
	stream.received <- &pb.ClientMessage{SubscriptionId: "p", RequestId: "subscribe", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
	initial := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
	if len(initial.Computed) != 1 || !initial.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatalf("initial computation: %v", initial)
	}
	if len(initial.Entities) != 1 || initial.Entities[0].GetUpsert().Values["canEdit"] != nil {
		t.Fatalf("computed value leaked into entity: %v", initial)
	}
	grant := receivePolicyRequest(t, ctx, backend, "grants")
	row := policyRow("grants", "g1", "grant")
	backend.commit(t, 20, q.SubscriptionEffect{SubscriptionID: grant.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Removed, Row: q.Row{Key: row.Key}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{row.Key}}}})
	updated := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_COMMIT, "20")
	if len(updated.Entities) != 0 || len(updated.Computed) != 1 || updated.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatalf("computed-only update: %v", updated)
	}
}

func TestOneShotSnapshotIncludesComputedValues(t *testing.T) {
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	options := computedOptions()
	options.Relations.AllowAll = false // Omitted Visibility permits this configured relation.
	options.Relations.Definitions[storage.RelationName{Namespace: "app", Name: "projects"}] = relationdef.Relation{Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: `true`}}}
	service, err := New(t.Context(), backend, options)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	request := snapshotTestRequest(false, false)
	request.Root.GetList().Query.Columns = nil // A computed-only row is still available.
	request.Root.Computed = []string{"canEdit"}
	response, err := service.(*server).GetProjectionSnapshot(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	rows := response.Data.Fields["projects"].GetListValue().Values
	if len(rows) != 1 || len(rows[0].GetStructValue().Fields) != 1 || !rows[0].GetStructValue().Fields["canEdit"].GetBoolValue() {
		t.Fatalf("snapshot: %v", response)
	}
}

func TestPendingComputedValueWithholdsNewRowUntilCurrentEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		if request.Query.Relation.Name == "grants" {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return policySnapshot(30, policyRow("grants", "g1", "grant"))
		}
		return policySnapshot(10, policyRow("projects", "p1", "local"))
	})
	options := computedOptions()
	name := storage.RelationName{Namespace: "app", Name: "projects"}
	definition := options.Relations.Definitions[name]
	definition.Computed["canEdit"] = relationdef.Computed{Type: storage.TypeBool, Expression: `item.title == "local" || facts.grant.exists(g,true)`}
	options.Relations.Definitions[name] = definition
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	defer func() { cancel(); <-done }()
	root := snapshotTestRequest(false, false).Root
	root.Computed = []string{"canEdit"}
	stream.received <- &pb.ClientMessage{SubscriptionId: "p", RequestId: "subscribe", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
	request := receivePolicyRequest(t, ctx, backend, "projects")
	initial := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
	if len(initial.Computed) != 1 || !initial.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatal(initial)
	}
	// Consume Ready before looking for any accidental publication.
	select {
	case <-stream.sent:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	backend.commit(t, 20, q.SubscriptionEffect{SubscriptionID: request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("projects", "p1", "requires-fact")}}}})
	receivePolicyRequest(t, ctx, backend, "grants")
	select {
	case message := <-stream.sent:
		if batch := message.GetModelBatch(); batch != nil && (len(batch.Entities) > 0 || len(batch.Computed) > 0) {
			t.Fatalf("published partial row: %v", message)
		}
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	hydrated := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_HYDRATION, "30")
	if len(hydrated.Entities) != 1 || hydrated.Entities[0].GetUpsert().Values["title"].GetTextValue() != "requires-fact" {
		t.Fatalf("lost withheld row: %v", hydrated)
	}
}

func TestDefinitionInvalidationUpdatesSelectedValueOnIdleProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	projects := storage.RelationName{Namespace: "app", Name: "projects"}
	store, err := relationdef.NewStore(relationdef.Definitions{projects: {Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: `true`}}}}, relationdef.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	options := computedOptions()
	options.Relations.Definitions = nil
	options.Relations.Store = store
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	defer func() { cancel(); <-done }()
	root := snapshotTestRequest(false, false).Root
	root.Computed = []string{"canEdit"}
	stream.received <- &pb.ClientMessage{SubscriptionId: "p", RequestId: "subscribe", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
	initial := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
	if len(initial.Computed) != 1 || !initial.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatal(initial)
	}
	if err := store.Replace(relationdef.Definitions{projects: {Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: `false`}}}}); err != nil {
		t.Fatal(err)
	}
	updated := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_HYDRATION, "")
	if len(updated.Entities) != 0 || len(updated.Computed) != 1 || updated.Computed[0].GetUpsert().Fields["canEdit"].GetBoolValue() {
		t.Fatalf("definition-only update: %v", updated)
	}
}

func TestComputedCollisionFailsOnlyItsProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend := newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		if request.Query.Relation.Name == "notes" {
			return policySnapshot(10, policyRow("notes", "n1", "note"))
		}
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	options := computedOptions()
	options.Relations.Definitions[storage.RelationName{Namespace: "app", Name: "projects"}] = relationdef.Relation{Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: `true`}}}
	stream, done := openPolicyStreamOptions(t, ctx, backend, options)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream did not stop")
		}
	})
	subscribe := func(id, table string, computed bool) q.QuerySubscribeRequest {
		t.Helper()
		root := snapshotTestRequest(false, false).Root
		root.GetList().Query.Table = table
		if computed {
			root.Computed = []string{"canEdit"}
			root.AllFields = true
			root.GetList().Query.Columns = nil
		}
		stream.received <- &pb.ClientMessage{SubscriptionId: id, RequestId: id, Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root}}}
		request := receivePolicyRequest(t, ctx, backend, table)
		receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
		select {
		case message := <-stream.sent:
			if message.GetReady() == nil || message.SubscriptionId != id {
				t.Fatalf("expected Ready for %s: %v", id, message)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return request
	}
	project := subscribe("project", "projects", true)
	note := subscribe("note", "notes", false)
	bad := policyRow("projects", "p1", "must not publish")
	bad.Record["canEdit"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new stored column")}
	backend.commit(t, 20,
		q.SubscriptionEffect{SubscriptionID: note.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("notes", "n1", "healthy update")}}}},
		q.SubscriptionEffect{SubscriptionID: project.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: bad}}}},
	)
	failed, healthy := false, false
	for !failed || !healthy {
		select {
		case err := <-done:
			t.Fatalf("collision terminated the connection: %v", err)
		case message := <-stream.sent:
			if failure := message.GetError(); failure != nil {
				if message.SubscriptionId != "project" || !strings.Contains(failure.Message, "collides") {
					t.Fatalf("wrong failure: %v", message)
				}
				failed = true
			}
			if batch := message.GetModelBatch(); batch != nil {
				for _, entity := range batch.Entities {
					if title := entity.GetUpsert().GetValues()["title"].GetTextValue(); title == "must not publish" {
						t.Fatalf("published invalid projection: %v", message)
					} else if title == "healthy update" {
						if batch.Kind != pb.ModelBatch_KIND_COMMIT || batch.Checkpoint != "20" || !batch.Atomic {
							t.Fatalf("healthy update lost its source transaction: %v", message)
						}
						healthy = true
					}
				}
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	backend.commit(t, 25, q.SubscriptionEffect{SubscriptionID: note.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("notes", "n1", "still live")}}}})
	batch := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_COMMIT, "25")
	if len(batch.Entities) != 1 || batch.Entities[0].GetUpsert().Values["title"].GetTextValue() != "still live" {
		t.Fatalf("healthy projection did not survive: %v", batch)
	}
}

func TestInitialComputedCollisionRespectsVisibility(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(map[bool]string{false: "hidden", true: "visible"}[visible], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
				row := policyRow("projects", "p1", "project")
				row.Record["canEdit"] = storage.Value{State: storage.ValueNull}
				return policySnapshot(10, row)
			})
			options := computedOptions()
			options.Relations.AllowAll = false
			options.Relations.Definitions[storage.RelationName{Namespace: "app", Name: "projects"}] = relationdef.Relation{
				Computed:   map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: `true`}},
				Visibility: map[bool]string{false: `false`, true: `true`}[visible],
			}
			service, err := New(ctx, backend, options)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			request := snapshotTestRequest(false, false)
			request.Root.AllFields = true
			request.Root.Computed = []string{"canEdit"}
			request.Root.GetList().Query.Columns = nil
			response, err := service.(*server).GetProjectionSnapshot(ctx, request)
			if visible {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("expected output validation error: %v", err)
				}
			} else if err != nil || len(response.Data.Fields["projects"].GetListValue().Values) != 0 {
				t.Fatalf("hidden row failed output validation: %v %v", response, err)
			}
		})
	}
}
