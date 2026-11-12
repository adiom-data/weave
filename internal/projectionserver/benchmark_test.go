package projectionserver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/protobuf/proto"
)

func (*projectionBenchmarkFeed) WaitForFence(context.Context, string) (*q.Position, error) {
	return nil, fmt.Errorf("unexpected fence wait")
}

func benchmarkPath(name string) *reactivev1.FieldPath {
	return &reactivev1.FieldPath{Segments: []string{name}}
}

// One project -> N association rows -> N task entity subscriptions. This is
// intentionally N+1, exercising the actual projection expansion scheduler.
func benchmarkProjectionRequest(count int) *reactivev1.ProjectionSnapshotRequest {
	query := func(table string) *reactivev1.Query {
		return &reactivev1.Query{Schema: "app", Table: table, Columns: []*reactivev1.FieldPath{benchmarkPath("id"), benchmarkPath("title")}}
	}
	parent := func(field, parent string) *reactivev1.ProjectionBinding {
		return &reactivev1.ProjectionBinding{Column: benchmarkPath(field), Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.ProjectionBinding_ParentColumn{ParentColumn: benchmarkPath(parent)}}
	}
	return &reactivev1.ProjectionSnapshotRequest{Parameters: map[string]*reactivev1.Value{"project": {Kind: &reactivev1.Value_TextValue{TextValue: "p1"}}}, Root: &reactivev1.ProjectionNode{NodeId: "project", Selection: &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{Query: query("projects"), Bindings: []*reactivev1.ProjectionBinding{{Column: benchmarkPath("id"), Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.ProjectionBinding_Parameter{Parameter: "project"}}}}}, Children: []*reactivev1.ProjectionNode{{NodeId: "links", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{Query: query("project_tasks"), Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_FROM_START, Count: uint32(count)}, Bindings: []*reactivev1.ProjectionBinding{parent("project_id", "id")}}}, Children: []*reactivev1.ProjectionNode{{NodeId: "task", Selection: &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{Query: query("tasks"), Bindings: []*reactivev1.ProjectionBinding{parent("id", "task_id")}}}}}}}}}
}

func benchmarkProjectionEvents(count int) []proj.ProjectionEvent {
	open := proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, Node: "tasks", InstanceID: "tasks", Bounds: snapshotTestBounds(), Checkpoint: &q.Position{SourceID: "test", Value: 10}, Fence: &q.Position{SourceID: "test", Value: 10}}
	events := []proj.ProjectionEvent{open}
	for i := 0; i < count; i++ {
		row := policyRow("tasks", fmt.Sprintf("t%03d", i), "Task")
		events = append(events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: "tasks", InstanceID: "tasks", Kind: q.Added, Key: row.Key, Record: row.Record, Cursor: row.Cursor, Initial: true, Version: q.RecordVersion{Revision: 1}})
	}
	return events
}

func BenchmarkProjectionStages(b *testing.B) {
	for _, count := range []int{10, 100} {
		b.Run(fmt.Sprintf("rows=%d", count), func(b *testing.B) {
			for _, stage := range []string{"DecodeRequest", "VisibilityInitialAllowAll", "VisibilityUpdateAllowAll", "NormalizeInitial", "EncodeSnapshot", "UpdateSelected", "UpdateUnselected"} {
				b.Run(stage, func(b *testing.B) {
					ctx := context.Background()
					request := benchmarkProjectionRequest(count)
					events := benchmarkProjectionEvents(count)
					selected := map[string]*fieldSelection{"tasks": selectColumns([]storage.FieldPath{storage.MustFieldPath("title")})}
					controller := accesscontrol.NewAllowAll()
					prepared, err := controller.Prepare(ctx, accesscontrol.PrepareRequest{ID: "bench"})
					if err != nil {
						b.Fatal(err)
					}
					defer prepared.Close()
					visibility := newProjectionVisibility()
					if _, err := visibility.filterInitial(ctx, controller, prepared, events); err != nil {
						b.Fatal(err)
					}
					model := newConnectionModel()
					initial, err := model.applyProjection("bench", proj.ProjectionBatch{Events: events}, selected)
					if err != nil {
						b.Fatal(err)
					}
					update := events[1]
					update.Initial = false
					update.Kind = q.Updated
					records := []storage.Record{modelRecord(update.Key.Value, "1", "Task A"), modelRecord(update.Key.Value, "2", "Task B")}
					if stage == "UpdateUnselected" {
						records[0]["title"] = records[1]["title"]
					}
					var iteration uint64
					if stage == "UpdateUnselected" {
						iteration = 1
						update.Version.Revision = 2
						update.Record = records[1]
						if _, err := model.applyProjection("bench", proj.ProjectionBatch{Events: []proj.ProjectionEvent{update}}, selected); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					for b.Loop() {
						switch stage {
						case "DecodeRequest":
							if _, _, err := projectionNode(request.Root, request.Parameters, 100, 8, 64); err != nil {
								b.Fatal(err)
							}
						case "VisibilityInitialAllowAll":
							if _, err := newProjectionVisibility().filterInitial(ctx, controller, prepared, events); err != nil {
								b.Fatal(err)
							}
						case "VisibilityUpdateAllowAll":
							iteration++
							update.Version.Revision = iteration + 1
							update.Record = records[iteration%2]
							batch, err := visibility.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(iteration + 10), Events: []proj.ProjectionEvent{update}})
							if err != nil || len(batch.Events) != 1 {
								b.Fatalf("visibility update: %v events=%d", err, len(batch.Events))
							}
						case "NormalizeInitial":
							batch, err := newConnectionModel().applyProjection("bench", proj.ProjectionBatch{Events: events}, selected)
							if err != nil || len(batch.Entities) != count {
								b.Fatalf("normalize: %v", err)
							}
						case "EncodeSnapshot":
							encoded, err := proto.Marshal(initial)
							if err != nil || len(encoded) == 0 {
								b.Fatalf("encode: %v", err)
							}
						case "UpdateSelected", "UpdateUnselected":
							iteration++
							update.Version.Revision = iteration + 1
							update.Record = records[iteration%2]
							batch, err := model.applyProjection("bench", proj.ProjectionBatch{Events: []proj.ProjectionEvent{update}}, selected)
							if err != nil {
								b.Fatal(err)
							}
							want := 1
							if stage == "UpdateUnselected" && iteration > 1 {
								want = 0
							}
							if len(batch.Entities) != want {
								b.Fatalf("updates=%d want=%d", len(batch.Entities), want)
							}
						}
					}
					if stage == "EncodeSnapshot" {
						b.ReportMetric(float64(proto.Size(initial)), "wire-bytes/op")
					}
				})
			}
		})
	}
}

// The synthetic query source has zero DB/network delay, but supplies real
// subscription installers and ordered progress. Cold/warm load counts are
// asserted so a benchmark cannot accidentally measure the wrong cache path.
type projectionBenchmarkSource struct {
	count int
	loads atomic.Uint64
}
type projectionBenchmarkFeed struct {
	source   *projectionBenchmarkSource
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	revision uint64
	events   chan q.Event
}
type projectionBenchmarkHandle string

func (h projectionBenchmarkHandle) ID() string                { return string(h) }
func (projectionBenchmarkHandle) Close(context.Context) error { return nil }
func (projectionBenchmarkHandle) UpdateWindow(context.Context, []q.BoundaryChange, string) error {
	return nil
}
func (s *projectionBenchmarkSource) OpenFeed(context.Context, q.FeedOptions) (q.Feed, error) {
	return &projectionBenchmarkFeed{source: s, done: make(chan struct{}), events: make(chan q.Event, 4096)}, nil
}
func (f *projectionBenchmarkFeed) Next(ctx context.Context) (q.Event, error) {
	select {
	case event := <-f.events:
		return event, nil
	case <-ctx.Done():
		return q.Event{}, ctx.Err()
	case <-f.done:
		return q.Event{}, q.ErrClosed
	}
}
func (f *projectionBenchmarkFeed) Close(context.Context) error {
	f.once.Do(func() { close(f.done) })
	return nil
}
func (f *projectionBenchmarkFeed) RequestProgress(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	previous := f.revision
	f.revision++
	event := q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: f.revision, PreviousRevision: previous}}
	select {
	case f.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func benchmarkLiteral(p *storage.Predicate, field storage.FieldPath) string {
	if p == nil {
		return ""
	}
	if p.Comparison != nil && p.Comparison.Field == field {
		return string(p.Comparison.Value.Data)
	}
	if p.Junction != nil {
		for _, child := range p.Junction.Operands {
			if value := benchmarkLiteral(child, field); value != "" {
				return value
			}
		}
	}
	return ""
}
func (f *projectionBenchmarkFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	f.source.loads.Add(1)
	image := policySnapshot(10)
	switch request.Query.Relation.Name {
	case "projects":
		id := benchmarkLiteral(request.Query.Filter, "id")
		image.Rows = []q.QueryRow{{Row: policyRow("projects", id, "Project")}}
	case "project_tasks":
		project := benchmarkLiteral(request.Query.Filter, "project_id")
		for i := 0; i < f.source.count; i++ {
			id := fmt.Sprintf("%s:t%03d", project, i)
			row := policyRow("project_tasks", "link:"+id, "Association")
			row.Record["project_id"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(project)}
			row.Record["task_id"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}
			image.Rows = append(image.Rows, q.QueryRow{Row: row})
		}
	case "tasks":
		id := benchmarkLiteral(request.Query.Filter, "id")
		image.Rows = []q.QueryRow{{Row: policyRow("tasks", id, "Task")}}
	case "grants":
		project := benchmarkLiteral(request.Query.Filter, "project_id")
		row := policyRow("grants", "grant:"+project, "Grant")
		row.Record["user_id"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("u1")}
		image.Rows = []q.QueryRow{{Row: row}}
	default:
		return fmt.Errorf("unexpected benchmark relation %s", request.Query.Relation.Name)
	}
	f.mu.Lock()
	image.Revision = f.revision
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return install(projectionBenchmarkHandle(request.ID), image)
}
func (f *projectionBenchmarkFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	return f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: request.ID, Query: request.Query}, func(handle q.QuerySubscription, image q.QuerySnapshot) error {
		if len(image.Rows) != 1 {
			return fmt.Errorf("entity rows=%d", len(image.Rows))
		}
		return install(handle, q.EntitySnapshot{Row: &image.Rows[0].Row, Checkpoint: image.Checkpoint, Fence: image.Fence, Revision: image.Revision})
	})
}

func benchmarkProjectionOptions(facts bool) Options {
	options := DefaultOptions()
	options.Dependencies.IdleTTL = time.Hour
	if !facts {
		return options
	}
	projects := storage.RelationName{Namespace: "app", Name: "projects"}
	links := storage.RelationName{Namespace: "app", Name: "project_tasks"}
	tasks := storage.RelationName{Namespace: "app", Name: "tasks"}
	options.ClaimsProvider = func(context.Context) (accesscontrol.Claims, error) {
		return accesscontrol.Claims{"user_id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("u1")}}, nil
	}
	options.Relations = &RelationOptions{Limits: relationdef.DefaultLimits(), MaxFactQueries: 1000, MaxFactRows: 100, FactLoadConcurrency: 8, Definitions: relationdef.Definitions{
		projects: {Facts: []relationdef.Fact{testPolicyFact("grant", storage.RelationName{Namespace: "app", Name: "grants"}, true)}, Computed: map[string]relationdef.Computed{"_visible": {Type: storage.TypeBool, Expression: `facts.grant.exists(g,g.item.user_id == claims.user_id)`, Propagate: []relationdef.Propagation{{To: links, When: `parent.id == item.project_id`}}}}, Visibility: `computed._visible`},
		links:    {Computed: map[string]relationdef.Computed{"_visible": {Type: storage.TypeBool, Expression: `has(inherited._visible) && inherited._visible`, Propagate: []relationdef.Propagation{{To: tasks, When: `parent.task_id == item.id`}}}}, Visibility: `computed._visible`},
		tasks:    {Visibility: `has(inherited._visible) && inherited._visible`},
	}}
	return options
}

// Compare first-image and synchronized capture with identical warm dependencies.
// Synchronization must not materialize a second complete image.
func BenchmarkProjectionSnapshotSynchronization(b *testing.B) {
	for _, synchronize := range []bool{false, true} {
		b.Run(fmt.Sprintf("wait=%t", synchronize), func(b *testing.B) {
			ctx := context.Background()
			source := &projectionBenchmarkSource{count: 100}
			public, err := New(ctx, source, benchmarkProjectionOptions(false))
			if err != nil {
				b.Fatal(err)
			}
			defer public.Close()
			service := public.(*server)
			request := benchmarkProjectionRequest(100)
			request.WaitForSync = synchronize
			run := func() {
				response, err := service.GetProjectionSnapshot(ctx, request)
				if err != nil {
					b.Fatal(err)
				}
				if response.Extensions.Synchronized != synchronize || snapshotObjectCount(response.Data) != 201 || synchronize && response.Extensions.Checkpoint != "10" || !synchronize && response.Extensions.Checkpoint != "" {
					b.Fatal("unexpected snapshot image or cut")
				}
			}
			run()
			before := source.loads.Load()
			b.ReportAllocs()
			for b.Loop() {
				run()
			}
			if loads := source.loads.Load() - before; loads != uint64(b.N) {
				b.Fatalf("upstream loads=%d want=%d", loads, b.N)
			}
		})
	}
}

func BenchmarkProjectionInitialization(b *testing.B) {
	for _, count := range []int{10, 100} {
		for _, mode := range []string{"allow-all", "facts", "item-only", "item-propagation", "propagation-only"} {
			for _, warm := range []bool{false, true} {
				b.Run(fmt.Sprintf("tasks=%d/policy=%s/warm=%t", count, mode, warm), func(b *testing.B) {
					ctx := context.Background()
					source := &projectionBenchmarkSource{count: count}
					options := benchmarkProjectionOptions(mode != "allow-all")
					if mode != "allow-all" && mode != "facts" {
						for relation, definition := range options.Relations.Definitions {
							definition.Facts = nil
							if mode == "item-only" {
								definition.Computed = nil
								definition.Visibility = `item.id != ""`
							} else if relation.Name == "projects" {
								when := `item.id == "p1"`
								if mode == "propagation-only" {
									when = `true`
								}
								value := definition.Computed["_visible"]
								value.Expression = when
								definition.Computed["_visible"] = value
							}
							options.Relations.Definitions[relation] = definition
						}
					}
					request := benchmarkProjectionRequest(count)
					var service *server
					create := func() *server {
						b.Helper()
						public, err := New(ctx, source, options)
						if err != nil {
							b.Fatal(err)
						}
						return public.(*server)
					}
					run := func() {
						b.Helper()
						response, err := service.GetProjectionSnapshot(ctx, request)
						if err != nil {
							b.Fatal(err)
						}
						if got := snapshotObjectCount(response.Data); got != 1+2*count {
							b.Fatalf("objects=%d want=%d", got, 1+2*count)
						}
					}
					if warm {
						service = create()
						defer service.Close()
						run()
					}
					before := source.loads.Load()
					var operations uint64
					b.ReportAllocs()
					for b.Loop() {
						if !warm {
							b.StopTimer()
							service = create()
							b.StartTimer()
						}
						run()
						operations++
						if !warm {
							b.StopTimer()
							if err := service.Close(); err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
					}
					loads := source.loads.Load() - before
					expected := uint64(count + 2)
					if mode == "facts" {
						expected++
					}
					if warm {
						expected = 1
					}
					if loads != operations*expected {
						b.Fatalf("upstream loads=%d want=%d", loads, operations*expected)
					}
					b.ReportMetric(float64(loads)/float64(operations), "upstream-loads/op")
				})
			}
		}
	}
}
