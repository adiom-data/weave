package projectionserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/postgres"
	querygrpc "github.com/adiom-data/weave/internal/querysubscription/grpc"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// Count actual datastore operations, rather than just checking that the hint
// survived decoding. The optional gate deterministically races a target update
// against an older joined snapshot without stalling the coordinator's CDC loop.
type referenceCountingSource struct {
	datastore.Storage
	mu                                              sync.Mutex
	joins, batches, taskReads, linkReads, taskPlans int
	gate, joined                                    chan struct{}
	once                                            sync.Once
	hydrationGate, hydrating                        chan struct{}
	hydrationOnce                                   sync.Once
}

func (s *referenceCountingSource) PrepareQuery(ctx context.Context, spec storage.QuerySpec) (datastore.PreparedQuery, error) {
	if spec.Relation.Name == "tasks" {
		s.mu.Lock()
		s.taskPlans++
		s.mu.Unlock()
	}
	return s.Storage.PrepareQuery(ctx, spec)
}
func (s *referenceCountingSource) PrepareReference(ctx context.Context, base datastore.PreparedQuery, shape storage.ReferenceFetch) (datastore.PreparedReference, error) {
	return s.Storage.(datastore.ReferencePreparer).PrepareReference(ctx, base, shape)
}
func (s *referenceCountingSource) RequestProgress(ctx context.Context, fence storage.Fence) error {
	return s.Storage.(datastore.ProgressRequester).RequestProgress(ctx, fence)
}
func (s *referenceCountingSource) BeginSnapshot(ctx context.Context, plan datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	inner, err := s.Storage.BeginSnapshot(ctx, plan)
	if err != nil {
		return nil, err
	}
	return &referenceCountingSnapshot{SnapshotSession: inner, owner: s}, nil
}
func (s *referenceCountingSource) counts() (int, int, int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.joins, s.batches, s.taskReads, s.linkReads, s.taskPlans
}

type referenceCountingSnapshot struct {
	datastore.SnapshotSession
	owner *referenceCountingSource
}

func (s *referenceCountingSnapshot) FetchEntity(ctx context.Context, key storage.EntityKey) (datastore.AuthoritativeRecord, error) {
	if key.Relation.Name == "links" && s.owner.hydrationGate != nil {
		s.owner.hydrationOnce.Do(func() { close(s.owner.hydrating) })
		select {
		case <-s.owner.hydrationGate:
		case <-ctx.Done():
			return datastore.AuthoritativeRecord{}, ctx.Err()
		}
	}
	if key.Relation.Name == "tasks" {
		s.owner.mu.Lock()
		s.owner.taskReads++
		s.owner.mu.Unlock()
	}
	return s.SnapshotSession.FetchEntity(ctx, key)
}

func (s *referenceCountingSnapshot) LoadWindow(ctx context.Context, plan datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.AuthoritativeRecord, error) {
	s.owner.mu.Lock()
	if plan.Relation().Name == "tasks" {
		s.owner.taskReads++
	}
	if plan.Relation().Name == "links" {
		s.owner.linkReads++
	}
	s.owner.mu.Unlock()
	return s.SnapshotSession.LoadWindow(ctx, plan, request)
}
func (s *referenceCountingSnapshot) LoadWindowWithReference(ctx context.Context, plan datastore.PreparedQuery, request datastore.WindowRequest, fetch datastore.ReferenceFetch) ([]datastore.ReferenceWindowRow, error) {
	s.owner.mu.Lock()
	s.owner.joins++
	s.owner.mu.Unlock()
	rows, err := s.SnapshotSession.(datastore.ReferenceWindowReader).LoadWindowWithReference(ctx, plan, request, fetch)
	if err == nil && s.owner.gate != nil {
		s.owner.once.Do(func() {
			close(s.owner.joined)
			select {
			case <-s.owner.gate:
			case <-ctx.Done():
			}
		})
	}
	return rows, err
}
func (s *referenceCountingSnapshot) LoadReferences(ctx context.Context, fetch datastore.ReferenceFetch, parents []storage.Record) ([]datastore.AuthoritativeRecord, error) {
	s.owner.mu.Lock()
	s.owner.batches++
	s.owner.mu.Unlock()
	return s.SnapshotSession.(datastore.ReferenceWindowReader).LoadReferences(ctx, fetch, parents)
}

func newReferenceIntegration(t testing.TB, remote bool, gate bool) (context.Context, *pgxpool.Pool, string, *referenceCountingSource, *server, q.Client) {
	t.Helper()
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for reference-fetch integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("weave_ref_%x", nonce)
	publication, slot := schema+"_pub", schema+"_slot"
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %[1]s;
		CREATE TABLE %[1]s.projects(id bigint PRIMARY KEY, title text NOT NULL);
		CREATE TABLE %[1]s.tasks(id bigint PRIMARY KEY, title text NOT NULL);
		CREATE TABLE %[1]s.links(id bigint PRIMARY KEY, project_id bigint NOT NULL, task_id bigint);
		INSERT INTO %[1]s.projects VALUES (1,'project');
		INSERT INTO %[1]s.tasks VALUES (1,'one'),(2,'two'),(3,'three');
		INSERT INTO %[1]s.links VALUES (1,1,1),(2,1,999),(3,1,2),(4,1,3);
		CREATE PUBLICATION %[2]s FOR TABLES IN SCHEMA %[1]s;`, schema, publication))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, sql := range []string{"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='" + slot + "' AND NOT active", "DROP PUBLICATION " + publication, "DROP SCHEMA " + schema + " CASCADE"} {
			if _, err := pool.Exec(cleanup, sql); err != nil {
				t.Error(err)
			}
		}
	})
	opts := postgres.DefaultOptions()
	opts.Publication, opts.Slot = publication, slot
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := postgres.NewStorage(ctx, url, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	source := &referenceCountingSource{Storage: store}
	if gate {
		source.gate, source.joined = make(chan struct{}), make(chan struct{})
	}
	runCtx, stop := context.WithCancel(ctx)
	coordinator, err := engine.StartCoordinator(runCtx, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(); _ = coordinator.Wait(context.Background()) })
	var client q.Client = querylocal.New(coordinator)
	if remote {
		listener := bufconn.Listen(1 << 20)
		transport := grpc.NewServer()
		querygrpc.NewServer(client).Register(transport)
		go func() { _ = transport.Serve(listener) }()
		t.Cleanup(transport.Stop)
		connection, err := grpc.NewClient("passthrough:///reference-query", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		client = querygrpc.NewClient(connection)
	}
	service, err := New(ctx, client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled {
			t.Error(err)
		}
	})
	return ctx, pool, schema, source, service.(*server), client
}

func referenceProjection(schema string, hinted bool) *pb.ProjectionNode {
	query := func(table string, fields ...string) *pb.Query {
		q := &pb.Query{Schema: schema, Table: table}
		for _, field := range fields {
			q.Columns = append(q.Columns, &pb.FieldPath{Segments: []string{field}})
		}
		return q
	}
	binding := func(field, parent string) *pb.ProjectionBinding {
		return &pb.ProjectionBinding{Column: &pb.FieldPath{Segments: []string{field}}, Operator: pb.Comparison_EQUAL, Value: &pb.ProjectionBinding_ParentColumn{ParentColumn: &pb.FieldPath{Segments: []string{parent}}}}
	}
	list := &pb.ProjectionList{Query: query("links", "id", "task_id"), Window: &pb.InitialWindow{Count: 2, From: pb.InitialWindow_START}, Bindings: []*pb.ProjectionBinding{binding("project_id", "id")}}
	if hinted {
		list.FetchReference = "task"
	}
	task := &pb.ProjectionNode{NodeId: "task", Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{
		Query: query("tasks", "id", "title"), Bindings: []*pb.ProjectionBinding{binding("id", "task_id")},
	}}}
	links := &pb.ProjectionNode{NodeId: "links", Selection: &pb.ProjectionNode_List{List: list}, Children: []*pb.ProjectionNode{task}}
	return &pb.ProjectionNode{NodeId: "project", Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{
		Query: query("projects", "id", "title"),
	}}, Children: []*pb.ProjectionNode{links}}
}

func TestReferenceSnapshotsAttachToSourceRowsIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, _, _, queries := newReferenceIntegration(t, remote, false)
			if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".links VALUES (5,1,1),(6,1,NULL)"); err != nil {
				t.Fatal(err)
			}
			feed, err := queries.OpenFeed(ctx, q.FeedOptions{BufferLimit: 128})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(context.Background())
			shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}}}
			request := q.QuerySubscribeRequest{ID: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "links"}}, Window: q.InitialWindow{Count: 10}, Reference: &shape}
			var rows map[string]q.QueryRow
			if err := feed.SubscribeQuery(ctx, request, func(_ q.QuerySubscription, snapshot q.QuerySnapshot) error {
				if !snapshot.OwnedReferences || len(snapshot.Rows) != 6 {
					return errors.New("missing source rows or reference ownership")
				}
				rows = make(map[string]q.QueryRow)
				for _, row := range snapshot.Rows {
					rows[string(row.Record["id"].Data)] = row
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for id, title := range map[string]string{"1": "one", "3": "two", "4": "three", "5": "one"} {
				image := rows[id].Expanded
				if image == nil || image.Row == nil || string(image.Row.Record["title"].Data) != title || image.Checkpoint == nil {
					t.Fatalf("source %s has the wrong attached image: %+v", id, image)
				}
			}
			if rows["2"].Expanded == nil || rows["2"].Expanded.Row != nil {
				t.Fatal("dangling reference lost its loaded-absence snapshot")
			}
			if rows["6"].Expanded != nil {
				t.Fatal("null binding unexpectedly received an expansion")
			}
			rows["1"].Expanded.Row.Record["title"].Data[0] = '!'
			if string(rows["5"].Expanded.Row.Record["title"].Data) != "one" {
				t.Fatal("repeated target images share mutable payloads")
			}
			// Reusing a hinted handle must use the client's detached image, not
			// the snapshot object just mutated by the caller.
			bound, _, _, err := storage.BindReference(shape, rows["5"].Record)
			if err != nil {
				t.Fatal(err)
			}
			if err := feed.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "task", Query: bound, FetchFrom: &q.ReferenceSource{SubscriptionID: "links", ParentKey: rows["5"].Key}}, func(_ q.EntitySubscription, image q.EntitySnapshot) error {
				if image.Row == nil || string(image.Row.Record["title"].Data) != "one" {
					return errors.New("snapshot mutation corrupted the reusable image")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func referenceOpen(t testing.TB, ctx context.Context, service *server, root *pb.ProjectionNode) (*authorizationCommitStream, chan error) {
	t.Helper()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *pb.ClientMessage, 4), sent: make(chan *pb.ServerMessage, 128)}
	done := make(chan error, 1)
	go func() { done <- service.ServeStream(stream) }()
	stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: "open", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: root, WaitForSync: true}}}
	return stream, done
}

func referenceUntil(t *testing.T, ctx context.Context, stream *authorizationCommitStream, accept func(*pb.ServerMessage) bool) {
	t.Helper()
	for {
		select {
		case message := <-stream.sent:
			if message.GetError() != nil {
				t.Fatalf("projection error: %v", message.GetError())
			}
			if accept(message) {
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for reference projection")
		}
	}
}

func TestReferencePendingTOASTBindingHydratesIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, source, service, _ := newReferenceIntegration(t, remote, false)
			// The extra equality guard is out-of-line text, not an identity
			// column. Moving an uncached link into the list leaves its unchanged
			// guard unresolved until the ordinary entity hydration completes.
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				ALTER TABLE %[1]s.links ADD COLUMN guard text COLLATE "C";
				ALTER TABLE %[1]s.tasks ADD COLUMN guard text COLLATE "C";
				UPDATE %[1]s.tasks SET guard=(SELECT string_agg(md5(g::text),'') FROM generate_series(1,100) g) WHERE id=1;
				INSERT INTO %[1]s.links(id,project_id,task_id,guard) SELECT 10,2,1,guard FROM %[1]s.tasks WHERE id=1;`, schema))
			if err != nil {
				t.Fatal(err)
			}
			source.hydrationGate, source.hydrating = make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(source.hydrationGate) }) }
			feed, err := service.queries.OpenFeed(ctx, q.FeedOptions{BufferLimit: 128})
			if err != nil {
				release()
				t.Fatal(err)
			}
			defer func() { release(); _ = feed.Close(context.Background()) }()
			shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}, {Source: "guard", Target: "guard"}}}
			query := storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "links"}, Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{
				{Comparison: &storage.Comparison{Field: "id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("10")}}},
				{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("1")}}},
			}}}}
			if err := feed.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "links", Query: query, Window: q.InitialWindow{Count: 10}, Reference: &shape}, func(_ q.QuerySubscription, snapshot q.QuerySnapshot) error {
				if len(snapshot.Rows) != 0 {
					t.Fatal("initial link was in the subscribed range")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "UPDATE "+schema+".links SET project_id=1 WHERE id=10"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-source.hydrating:
			case <-ctx.Done():
				t.Fatal("expected an unresolved TOAST binding")
			}
			var key storage.EntityKey
			for key.Value == "" {
				event, err := feed.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Failure != nil {
					t.Fatal(event.Failure.Cause)
				}
				if event.Delivery != nil {
					for _, effect := range event.Delivery.Effects {
						for _, change := range effect.Batch.Changes {
							if change.Pending {
								key = change.Key
							}
						}
					}
				}
			}
			release()
			var parent storage.Record
			for parent == nil {
				event, err := feed.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Failure != nil {
					t.Fatal(event.Failure.Cause)
				}
				if event.Delivery != nil {
					for _, effect := range event.Delivery.Effects {
						for _, change := range effect.Batch.Changes {
							if change.Key == key && !change.Pending {
								parent = change.Record
							}
						}
					}
				}
			}
			bound, _, matchable, err := storage.BindReference(shape, parent)
			if err != nil || !matchable {
				t.Fatalf("hydrated binding: %v", err)
			}
			if err := feed.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "task", Query: bound, FetchFrom: &q.ReferenceSource{SubscriptionID: "links", ParentKey: key}}, func(_ q.EntitySubscription, snapshot q.EntitySnapshot) error {
				if snapshot.Row == nil || string(snapshot.Row.Record["title"].Data) != "one" {
					return errors.New("hydration did not attach the child")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReferenceOwnerTransferKeepsTargetInOriginalCommitIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, _, _, queries := newReferenceIntegration(t, remote, false)
			// Inspect parent-owned routing at the query boundary. Projection
			// dependencies consume it and publish only explicitly opened handles.
			feed, err := queries.OpenFeed(ctx, q.FeedOptions{BufferLimit: 128})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(context.Background())
			request := q.QuerySubscribeRequest{ID: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "links"}}, Window: q.InitialWindow{Count: 10}, Reference: &storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: schema, Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}}}}
			if err := feed.SubscribeQuery(ctx, request, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "BEGIN; DELETE FROM "+schema+".links WHERE id=1; INSERT INTO "+schema+".links VALUES(5,1,1); UPDATE "+schema+".tasks SET title='transferred' WHERE id=1; COMMIT"); err != nil {
				t.Fatal(err)
			}
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
				for _, effect := range event.Delivery.Effects {
					var newOwner storage.EntityKey
					for _, change := range effect.Batch.Changes {
						if string(change.Record["id"].Data) == "5" {
							newOwner = change.Key
						}
					}
					if newOwner.Value == "" {
						continue
					}
					if !event.Delivery.Delivery.Atomic || event.Delivery.Delivery.Kind != q.DeliveryCommit {
						t.Fatal("owner transfer lost commit metadata")
					}
					for _, entity := range event.Delivery.Entities {
						if entity.Target.SubscriptionID != effect.SubscriptionID || entity.Target.Reference == nil {
							continue
						}
						for _, owner := range entity.Target.Reference.Parents {
							if owner == newOwner {
								for _, change := range entity.Delta.Changes {
									if string(change.Record["title"].Data) == "transferred" {
										return
									}
								}
							}
						}
					}
					t.Fatalf("target update was detached from the association transfer transaction: owner=%+v entities=%+v", newOwner, event.Delivery.Entities)
				}
			}
		})
	}
}

func TestReferenceProjectionRebindsOwnedInterestIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, _, service, _ := newReferenceIntegration(t, remote, false)
			streamCtx, closeStream := context.WithCancel(ctx)
			stream, done := referenceOpen(t, streamCtx, service, referenceProjection(schema, true))
			defer func() { closeStream(); <-done }()
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool { return message.GetReady() != nil })
			if _, err := pool.Exec(ctx, "BEGIN; UPDATE "+schema+".links SET task_id=3 WHERE id=1; UPDATE "+schema+".tasks SET title='rebound' WHERE id=3; COMMIT"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				for _, entity := range message.GetModelBatch().GetEntities() {
					if entity.GetUpsert().GetValues()["title"].GetTextValue() == "rebound" {
						return true
					}
				}
				return false
			})
			if _, err := pool.Exec(ctx, "BEGIN; UPDATE "+schema+".tasks SET title='stale old target' WHERE id=1; UPDATE "+schema+".tasks SET title='latest new target' WHERE id=3; COMMIT"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				found := false
				for _, entity := range message.GetModelBatch().GetEntities() {
					title := entity.GetUpsert().GetValues()["title"].GetTextValue()
					if title == "stale old target" {
						t.Fatal("retired reference leaked an old target update")
					}
					found = found || title == "latest new target"
				}
				return found
			})
		})
	}
}

func TestReferenceProjectionEndToEndIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			ctx, pool, schema, source, service, _ := newReferenceIntegration(t, remote, false)
			streamCtx, closeStream := context.WithCancel(ctx)
			stream, done := referenceOpen(t, streamCtx, service, referenceProjection(schema, true))
			defer func() { closeStream(); <-done }()
			queryID := ""
			taskID := ""
			initialTask := false
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, query := range batch.Queries {
						if query.NodeId == "links" && query.GetReset_() != nil {
							queryID = query.QuerySubscriptionId
							if len(query.GetReset_().EntityIds) != 2 {
								t.Fatal("dangling target shrank the association window")
							}
						}
					}
					for _, entity := range batch.Entities {
						if entity.GetUpsert().GetValues()["title"].GetTextValue() == "one" {
							initialTask = true
							taskID = entity.EntityId
						}
					}
				}
				return message.GetReady() != nil
			})
			joins, batches, taskReads, _, taskPlans := source.counts()
			if queryID == "" || !initialTask || joins != 1 || batches != 0 || taskReads != 0 || taskPlans != 0 {
				t.Fatalf("initial path: query=%q task=%v operations=%d/%d/%d/%d", queryID, initialTask, joins, batches, taskReads, taskPlans)
			}
			stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: "extend", Operation: &pb.ClientMessage_UpdateProjectionWindow{UpdateProjectionWindow: &pb.UpdateProjectionWindow{InstanceId: queryID, Updates: []*pb.BoundaryUpdate{{Side: pb.BoundaryUpdate_RIGHT, Operation: pb.BoundaryUpdate_EXTEND, Count: 2}}}}}
			seenTwo, seenThree, windowAck := false, false, false
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, entity := range batch.Entities {
						switch entity.GetUpsert().GetValues()["title"].GetTextValue() {
						case "two":
							seenTwo = true
						case "three":
							seenThree = true
						}
					}
					windowAck = windowAck || message.RequestId == "extend"
				}
				return windowAck && seenTwo && seenThree
			})
			joins, _, taskReads, _, taskPlans = source.counts()
			if joins != 2 || taskReads != 0 || taskPlans != 0 {
				t.Fatalf("extension did not reuse reference loading: joins=%d task reads=%d plans=%d", joins, taskReads, taskPlans)
			}
			if _, err := pool.Exec(ctx, "UPDATE "+schema+".tasks SET title='updated' WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				batch := message.GetModelBatch()
				if batch == nil {
					return false
				}
				for _, entity := range batch.Entities {
					if entity.GetUpsert().GetValues()["title"].GetTextValue() == "updated" {
						if !batch.Atomic || batch.Kind != pb.ModelBatch_COMMIT {
							t.Fatal("target update lost normal commit semantics")
						}
						return true
					}
				}
				return false
			})
			if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".tasks VALUES (999,'formerly missing')"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, entity := range batch.Entities {
						if entity.GetUpsert().GetValues()["title"].GetTextValue() == "formerly missing" {
							return true
						}
					}
				}
				return false
			})
			// New associations arriving through CDC still use normal dynamic
			// expansion; joined loading is not a second reactive join mechanism.
			if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".tasks VALUES (4,'new task'); INSERT INTO "+schema+".links VALUES (5,1,4)"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, entity := range batch.Entities {
						if entity.GetUpsert().GetValues()["title"].GetTextValue() == "new task" {
							return true
						}
					}
				}
				return false
			})
			if _, err := pool.Exec(ctx, "DELETE FROM "+schema+".tasks WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, entity := range batch.Entities {
						if entity.EntityId == taskID && entity.GetReleased() != nil {
							return true
						}
					}
				}
				return false
			})
			if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".tasks VALUES (1,'restored')"); err != nil {
				t.Fatal(err)
			}
			referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
				if batch := message.GetModelBatch(); batch != nil {
					for _, entity := range batch.Entities {
						if entity.GetUpsert().GetValues()["title"].GetTextValue() == "restored" {
							return true
						}
					}
				}
				return false
			})
			beforeJoins, beforeBatches, _, _, _ := source.counts()
			for _, update := range []struct {
				token     string
				operation pb.BoundaryUpdate_Operation
				count     uint32
			}{
				{"contract", pb.BoundaryUpdate_CONTRACT, 2}, {"reextend", pb.BoundaryUpdate_EXTEND, 1},
			} {
				stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: update.token, Operation: &pb.ClientMessage_UpdateProjectionWindow{UpdateProjectionWindow: &pb.UpdateProjectionWindow{InstanceId: queryID, Updates: []*pb.BoundaryUpdate{{Side: pb.BoundaryUpdate_RIGHT, Operation: update.operation, Count: update.count}}}}}
				referenceUntil(t, ctx, stream, func(message *pb.ServerMessage) bool {
					return message.RequestId == update.token && message.GetModelBatch() != nil
				})
			}
			afterJoins, afterBatches, _, _, _ := source.counts()
			if beforeJoins != afterJoins || beforeBatches != afterBatches {
				t.Fatal("contraction or covered re-extension unnecessarily fetched references")
			}
			// A plain projection reuses association coverage populated by the
			// hinted one. It must not create another joined-query cache.
			beforeJoins, _, _, beforeLinks, _ := source.counts()
			plain := referenceProjection(schema, false)
			plain.Children[0].Children = nil
			if _, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: plain, WaitForSync: true}); err != nil {
				t.Fatal(err)
			}
			afterJoins, _, _, afterLinks, _ := source.counts()
			if beforeJoins != afterJoins || beforeLinks != afterLinks {
				t.Fatal("ordinary and hinted subscriptions did not share base coverage")
			}
		})
	}
}

func TestReferenceProjectionWarmBaseAndConcurrentTargetIntegration(t *testing.T) {
	for _, mode := range []string{"warm-base", "concurrent-target", "concurrent-delete", "concurrent-key-move"} {
		t.Run(mode, func(t *testing.T) {
			ctx, pool, schema, source, service, _ := newReferenceIntegration(t, true, mode != "warm-base")
			if mode == "warm-base" {
				plain := referenceProjection(schema, false)
				plain.Children[0].Children = nil
				if _, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: plain, WaitForSync: true}); err != nil {
					t.Fatal(err)
				}
			}
			type result struct {
				response *pb.ProjectionSnapshotResponse
				err      error
			}
			loaded := make(chan result, 1)
			go func() {
				response, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: referenceProjection(schema, true), WaitForSync: true})
				loaded <- result{response, err}
			}()
			if mode != "warm-base" {
				select {
				case <-source.joined:
				case outcome := <-loaded:
					t.Fatalf("projection ended before joined fetch: %v", outcome.err)
				case <-ctx.Done():
					t.Fatal("joined fetch was not reached")
				}
				sql := "UPDATE " + schema + ".tasks SET title='newest' WHERE id=1"
				if mode == "concurrent-delete" {
					sql = "DELETE FROM " + schema + ".tasks WHERE id=1"
				}
				if mode == "concurrent-key-move" {
					sql = "UPDATE " + schema + ".tasks SET id=10 WHERE id=1"
				}
				if _, err := pool.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
				close(source.gate)
			}
			var outcome result
			select {
			case outcome = <-loaded:
			case <-ctx.Done():
				t.Fatal("projection did not complete")
			}
			if outcome.err != nil {
				t.Fatal(outcome.err)
			}
			want := "one"
			if mode == "concurrent-target" {
				want = "newest"
			}
			found := false
			absent := mode == "concurrent-delete" || mode == "concurrent-key-move"
			taskPresent := false
			for _, entity := range snapshotObjects(outcome.response.Data) {
				title := entity.Fields["title"].GetStringValue()
				if title == want {
					found = true
				}
				if title == "one" || title == "newest" {
					taskPresent = true
				}
				if mode != "warm-base" && title == "one" {
					t.Fatal("older joined snapshot overwrote a newer update")
				}
			}
			if absent {
				found = !taskPresent
			}
			joins, batches, taskReads, _, taskPlans := source.counts()
			if !found || batches != 1 || taskReads != 0 || taskPlans != 0 {
				t.Fatalf("%s: found=%v joins=%d batches=%d reads=%d plans=%d", mode, found, joins, batches, taskReads, taskPlans)
			}
			if mode == "warm-base" && joins != 0 {
				t.Fatal("warm base was unnecessarily reread as a join")
			}
		})
	}
}

func TestReferenceProjectionCancellationDoesNotDamageLaterLoadsIntegration(t *testing.T) {
	ctx, _, schema, source, service, _ := newReferenceIntegration(t, true, true)
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := service.GetProjectionSnapshot(requestCtx, &pb.ProjectionSnapshotRequest{Root: referenceProjection(schema, true), WaitForSync: true})
		done <- err
	}()
	select {
	case <-source.joined:
	case err := <-done:
		t.Fatalf("load ended before fetch: %v", err)
	case <-ctx.Done():
		t.Fatal("load did not reach join")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled projection unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("cancellation did not unblock joined load")
	}
	if _, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: referenceProjection(schema, true), WaitForSync: true}); err != nil {
		t.Fatalf("canceled join damaged later subscriptions: %v", err)
	}
}

func TestReferenceProjectionGuardedSelectorsShareEntityNotMembershipIntegration(t *testing.T) {
	ctx, pool, schema, source, service, _ := newReferenceIntegration(t, true, false)
	_, err := pool.Exec(ctx, "ALTER TABLE "+schema+".tasks ADD organization_id bigint NOT NULL DEFAULT 1; ALTER TABLE "+schema+".links ADD organization_id bigint NOT NULL DEFAULT 1; INSERT INTO "+schema+".links (id,project_id,task_id,organization_id) VALUES (6,1,1,2)")
	if err != nil {
		t.Fatal(err)
	}
	plain := referenceProjection(schema, false)
	plain.Children[0].GetList().Window.Count = 10
	plain.Children[0].Children = nil
	if _, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: plain, WaitForSync: true}); err != nil {
		t.Fatal(err)
	}
	hinted := referenceProjection(schema, true)
	hinted.Children[0].GetList().Window.Count = 10
	entity := hinted.Children[0].Children[0].GetEntity()
	entity.Bindings = append(entity.Bindings, &pb.ProjectionBinding{Column: &pb.FieldPath{Segments: []string{"organization_id"}}, Operator: pb.Comparison_EQUAL, Value: &pb.ProjectionBinding_ParentColumn{ParentColumn: &pb.FieldPath{Segments: []string{"organization_id"}}}})
	response, err := service.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: hinted, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	taskOccurrences := 0
	for _, object := range snapshotObjects(response.Data) {
		if object.Fields["task"].GetStructValue() != nil {
			taskOccurrences++
		}
	}
	joins, batches, reads, _, plans := source.counts()
	// The dangling guarded lookup (task 999) conservatively reads once: an
	// empty guarded join is not proof that the whole entity is absent. Existing
	// targets, including the mismatching organization guard, reuse full rows.
	if taskOccurrences != 3 || joins != 0 || batches != 1 || reads != 1 || plans != 0 {
		t.Fatalf("guarded selectors: occurrences=%d joins=%d batches=%d reads=%d plans=%d", taskOccurrences, joins, batches, reads, plans)
	}
}
