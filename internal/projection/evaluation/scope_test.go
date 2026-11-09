package evaluation

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/dependency"
	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

func value(text string) storage.Value {
	return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(text)}
}

var tasks = storage.RelationName{Namespace: "demo", Name: "tasks"}

func TestLinkedAuthorizationPathDoesNotBypassDeniedParent(t *testing.T) {
	facts, _ := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	s := scope(t, facts)
	projects := storage.RelationName{Namespace: "demo", Name: "projects"}
	evaluator, err := relationdef.Compile(relationdef.Definitions{projects: {Visibility: `false`}, tasks: {Visibility: `true`}}, relationdef.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	root := relationdef.NewProjectionPath(nil, relationdef.ProjectionStep{Node: "project", Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: "demo", Name: "projects"}, Value: "p1"}, Complete: true})
	candidate := input("u1").Candidate
	candidate.Context.Projection = relationdef.NewProjectionPath(root, relationdef.ProjectionStep{Node: "task", Key: candidate.Key, Record: candidate.Record, Complete: true})
	view, err := facts.OpenView(at(10))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	decision, err := s.EvaluatePath(context.Background(), evaluator, input("u1").Claims, candidate, view)
	if err != nil || !decision.Resolved || decision.Visible {
		t.Fatalf("denied ancestor was bypassed: %+v %v", decision, err)
	}
	if len(s.PolicyStamps()) != 1 {
		t.Fatal("path evaluation continued beyond the denied parent")
	}
}

func TestLazyFactsExpandPrivateProjectionAndBindImmediateParent(t *testing.T) {
	f, feed := newFacts(t, Options{10, 10, 2})
	s := scope(t, f)
	projects := storage.RelationName{Namespace: "demo", Name: "projects"}
	e, err := relationdef.Compile(relationdef.Definitions{tasks: {Facts: []relationdef.Fact{{ID: "access", Projection: &plan.Node{
		Name: "links", Query: storage.QuerySpec{Relation: relation}, Bindings: []plan.Binding{{Field: "task_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}},
		Children: []*plan.Node{{Name: "project", Entity: true, Query: storage.QuerySpec{Relation: projects}, Bindings: []plan.Binding{{Field: "id", Operator: storage.ComparisonEqual, Parent: "project_id"}}}},
	}}}, Visibility: `facts.access.exists(link, link.children.project != null && link.children.project.item.owner == claims.user_id)`}}, relationdef.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	in := input("u1")
	in.Candidate.Record["id"] = value("t1")
	check := func(want bool) {
		t.Helper()
		got, err := s.Evaluate(context.Background(), e, in, at(10))
		if err != nil || got.Visible != want || (want && !got.Resolved) {
			t.Fatalf("decision %+v: %v", got, err)
		}
	}
	check(false)
	linkRequest := receive(t, feed)
	if string(linkRequest.request.Query.Filter.Comparison.Value.Data) != "t1" {
		t.Fatal("root parameter binding lost")
	}
	link := row("l1", "link")
	link.Record["project_id"] = value("p1")
	install(t, feed, linkRequest, snapshot(10, 1, link))
	consume(t, f, feed, progress(10, 1, 0))
	check(false)
	projectRequest := receive(t, feed)
	if projectRequest.request.Query.Relation != projects || string(projectRequest.request.Query.Filter.Comparison.Value.Data) != "p1" {
		t.Fatal("child did not bind immediate parent")
	}
	project := row("p1", "project")
	project.Key.Relation.Name = "projects"
	project.Record["owner"] = value("u1")
	install(t, feed, projectRequest, snapshot(10, 2, project))
	consume(t, f, feed, progress(10, 2, 1))
	check(true)
	if len(s.DependencyIDs()) != 2 {
		t.Fatal("nested dependencies were not retained")
	}
}

func evaluator(t *testing.T) *relationdef.Evaluator {
	t.Helper()
	e, err := relationdef.Compile(relationdef.Definitions{tasks: {
		Facts: []relationdef.Fact{{ID: "grant", Projection: &plan.Node{Name: "grant", Query: storage.QuerySpec{Relation: relation}, Bindings: []plan.Binding{
			{Field: "project_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "project_id")},
			{Field: "user_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("claims", "user_id")},
		}}}}, Visibility: "(item.public == \"yes\") || (facts.grant.exists(g,true))",
	}}, relationdef.Limits{MaxDepth: 30, MaxNodes: 1000, MaxEvaluationSteps: 100000})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func input(user string) relationdef.Input {
	return relationdef.Input{Claims: relationdef.Claims{"user_id": value(user)}, Candidate: relationdef.Candidate{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: tasks.Namespace, Name: tasks.Name}, Value: "t1"}, Record: storage.Record{"project_id": value("p1")}, Complete: true}}
}
func scope(t *testing.T, facts *Facts) *Scope {
	t.Helper()
	s, err := NewScope(facts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestScopesShareFactsButPruneTheirOwnDependencies(t *testing.T) {
	f, feed := newFacts(t, Options{10, 10, 2})
	first, second := scope(t, f), scope(t, f)
	e := evaluator(t)
	for _, s := range []*Scope{first, second} {
		got, err := s.Evaluate(context.Background(), e, input("u1"), at(10))
		if err != nil || got.Resolved {
			t.Fatalf("%+v %v", got, err)
		}
	}
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1, row("g1", "grant")))
	consume(t, f, feed, progress(10, 1, 0))
	for _, s := range []*Scope{first, second} {
		got, err := s.Evaluate(context.Background(), e, input("u1"), at(10))
		if err != nil || !got.Visible {
			t.Fatalf("%+v %v", got, err)
		}
	}
	ids1, ids2 := first.DependencyIDs(), second.DependencyIDs()
	if len(ids1) != 1 || len(ids2) != 1 || ids1[0] != ids2[0] {
		t.Fatalf("selectors not shared: %v %v", ids1, ids2)
	}
	public := input("u1")
	public.Candidate.Record["public"] = value("yes")
	got, err := first.Evaluate(context.Background(), e, public, at(10))
	if err != nil || !got.Visible || len(first.DependencyIDs()) != 0 || feed.closed.Load() != 0 {
		t.Fatalf("bad partial prune: %+v %v", got, err)
	}
	got, err = second.Evaluate(context.Background(), e, public, at(10))
	if err != nil || !got.Visible || feed.closed.Load() != 1 {
		t.Fatalf("bad final prune: %+v %v closes=%d", got, err, feed.closed.Load())
	}
	got, err = first.Evaluate(context.Background(), e, input("u1"), at(10))
	if err != nil || got.Resolved {
		t.Fatalf("cannot rediscover pruned grant: %+v %v", got, err)
	}
	newRequest := receive(t, feed)
	if newRequest.request.ID == i.request.ID {
		t.Fatal("reused subscription ID")
	}
}

func TestFailedRequirementSkipsPrunesAndRestoresAllowFacts(t *testing.T) {
	f, feed := newFacts(t, Options{10, 10, 1})
	s := scope(t, f)
	e, err := relationdef.Compile(relationdef.Definitions{tasks: {

		Facts: []relationdef.Fact{{ID: "grant", Projection: &plan.Node{Name: "grants", Query: storage.QuerySpec{Relation: relation}}}}, Visibility: "(item.enabled == \"yes\") && (facts.grant.exists(g, true))",
	}}, relationdef.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	in := input("u1")
	check := func(enabled bool, want relationdef.Decision) {
		t.Helper()
		in.Candidate.Record["enabled"] = value(map[bool]string{true: "yes", false: "no"}[enabled])
		got, err := s.Evaluate(t.Context(), e, in, at(10))
		if err != nil || got != want {
			t.Fatalf("enabled=%t: got %+v, %v; want %+v", enabled, got, err, want)
		}
	}
	check(false, relationdef.Decision{Resolved: true})
	if len(s.DependencyIDs()) != 0 {
		t.Fatal("failed requirement discovered allow dependencies")
	}
	select {
	case request := <-feed.requests:
		t.Fatalf("failed requirement started allow expansion: %s", request.request.ID)
	default:
	}
	check(true, relationdef.Decision{})
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1, row("g1", "grant")))
	consume(t, f, feed, progress(10, 1, 0))
	check(true, relationdef.Decision{Visible: true, Resolved: true})
	check(false, relationdef.Decision{Resolved: true})
	if len(s.DependencyIDs()) != 0 || feed.closed.Load() != 1 {
		t.Fatal("failed requirement retained obsolete allow dependencies")
	}
	check(true, relationdef.Decision{})
	if next := receive(t, feed); next.request.ID == i.request.ID {
		t.Fatal("re-enabled item reused a closed allow subscription")
	}
}

func TestDifferentSubjectsBindDifferentSelectors(t *testing.T) {
	f, feed := newFacts(t, Options{10, 10, 2})
	e := evaluator(t)
	for _, user := range []string{"u1", "u2"} {
		s := scope(t, f)
		if _, err := s.Evaluate(context.Background(), e, input(user), at(10)); err != nil {
			t.Fatal(err)
		}
	}
	a, b := receive(t, feed), receive(t, feed)
	if a.request.ID == b.request.ID {
		t.Fatal("different user facts shared an ID")
	}
	for _, i := range []installation{a, b} {
		pred := i.request.Query.Filter.Junction.Operands[1].Comparison
		if string(pred.Value.Data) != "u1" && string(pred.Value.Data) != "u2" {
			t.Fatal(pred)
		}
	}
}

func TestAtomicGrantReplacementDoesNotFlicker(t *testing.T) {
	f, feed := newFacts(t, Options{10, 10, 1})
	s := scope(t, f)
	e := evaluator(t)
	if _, err := s.Evaluate(context.Background(), e, input("u1"), at(10)); err != nil {
		t.Fatal(err)
	}
	i := receive(t, feed)
	old, newRow := row("g1", "old"), row("g2", "new")
	install(t, feed, i, snapshot(10, 1, old))
	consume(t, f, feed, progress(10, 1, 0))
	consume(t, f, feed, commit(11, 2, 1, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{
		Changes:    []q.Change{{Kind: q.Removed, Row: q.Row{Key: old.Key}}, {Kind: q.Added, Row: newRow}},
		Membership: q.MembershipDelta{Removed: []storage.EntityKey{old.Key}, Insertions: []q.Insertion{{Key: newRow.Key, FinalIndex: 0}}},
	}}))
	got, err := s.Evaluate(context.Background(), e, input("u1"), at(11))
	if err != nil || !got.Visible || !got.Resolved {
		t.Fatalf("grant replacement flickered: %+v %v", got, err)
	}
	ids := f.DrainChanges()
	if len(ids) != 1 || ids[0] != i.request.ID {
		t.Fatal(ids)
	}
}

type testClient struct{ feed q.Feed }

func (c testClient) OpenFeed(context.Context, q.FeedOptions) (q.Feed, error) { return c.feed, nil }

func TestSeparateFactConsumersShareProjectionDependencyCache(t *testing.T) {
	feed := newTestFeed()
	cache, err := dependency.New(context.Background(), testClient{feed}, dependency.Options{UpstreamBufferLimit: 100, MaxSubscriptions: 100, IdleTTL: 0, SweepInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	aFeed, err := cache.OpenFactFeed(context.Background(), q.FeedOptions{BufferLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	bFeed, err := cache.OpenFactFeed(context.Background(), q.FeedOptions{BufferLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer aFeed.Close(context.Background())
	defer bFeed.Close(context.Background())
	a, err := NewObservedFacts(context.Background(), aFeed, Options{10, 10, 1})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewObservedFacts(context.Background(), bFeed, Options{10, 10, 1})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	aLease, err := a.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	bLease, err := b.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1, row("g1", "grant")))
	for _, f := range []*Facts{a, b} {
		select {
		case <-f.Changed():
		case <-time.After(3 * time.Second):
			t.Fatal("fact consumer did not install")
		}
	}
	feed.events <- progress(10, 1, 0)
	for _, consumer := range []struct {
		facts *Facts
		feed  q.Feed
	}{{a, aFeed}, {b, bFeed}} {
		event, err := consumer.feed.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.facts.Observe(event); err != nil {
			t.Fatal(err)
		}
	}
	for _, lease := range []*Lease{aLease, bLease} {
		got, err := lease.Read(at(10))
		if err != nil || !got.Complete || len(got.Rows) != 1 {
			t.Fatalf("%+v %v", got, err)
		}
	}
	select {
	case extra := <-feed.requests:
		t.Fatalf("duplicate physical subscription: %s", extra.request.ID)
	default:
	}
}
