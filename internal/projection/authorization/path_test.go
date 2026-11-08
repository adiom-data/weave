package authorization

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func pathCandidate(path *accesscontrol.ProjectionPath) accesscontrol.Candidate {
	step := path.Step()
	return accesscontrol.Candidate{Key: step.Key, Record: step.Record, Complete: step.Complete, Context: accesscontrol.Scope{Projection: path}}
}

func TestAuthorizationGraphSharesOnlyConcreteParentsInOneView(t *testing.T) {
	ctx := context.Background()
	facts, _ := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	projects := storage.RelationName{Namespace: "demo", Name: "projects"}
	evaluator, err := policy.Compile(policy.Map{
		projects: {Allow: []policy.Rule{{ID: "owner", When: `item.owner == claims.user_id`}}, Propagate: []policy.Edge{{ID: "task", To: tasks, When: `parent.id == item.project_id`}}},
		tasks:    {Require: []policy.Rule{{ID: "not-blocked", When: `item.blocked != "yes"`}}},
	}, policy.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	root := accesscontrol.NewProjectionPath(nil, accesscontrol.ProjectionStep{Node: "project", Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: "demo", Name: "projects"}, Value: "p1"}, Record: storage.Record{"id": value("p1"), "owner": value("u1")}, Complete: true})
	child := func(parent *accesscontrol.ProjectionPath, id string) *accesscontrol.ProjectionPath {
		key := input("u1").Candidate.Key
		key.Value = id
		return accesscontrol.NewProjectionPath(parent, accesscontrol.ProjectionStep{Node: "task", Key: key, Record: storage.Record{"id": value(id), "project_id": value("p1"), "blocked": value("no")}, Complete: true})
	}
	view, _ := facts.OpenView(at(10))
	defer view.Close()
	check := func(s *Scope, claims accesscontrol.Claims, candidate accesscontrol.Candidate, view *View, visible bool) {
		t.Helper()
		decision, err := s.EvaluatePath(ctx, evaluator, claims, candidate, view)
		if err != nil || !decision.Resolved || decision.Visible != visible {
			t.Fatalf("decision=%+v err=%v want visible=%t", decision, err, visible)
		}
	}
	a, b := child(root, "t1"), child(root, "t2")
	sa, sb := scope(t, facts), scope(t, facts)
	check(sa, input("u1").Claims, pathCandidate(a), view, true)
	rootResult := view.paths[0].nodes[root]
	check(sb, input("u1").Claims, pathCandidate(b), view, true)
	graph := view.paths[0]
	if len(graph.nodes) != 3 || graph.nodes[a].parent != rootResult || graph.nodes[b].parent != rootResult {
		t.Fatal("siblings did not reuse the one established parent result")
	}
	if len(sa.PolicyStamps()) != 2 || len(sb.PolicyStamps()) != 2 {
		t.Fatal("descendants lost ancestor policy dependencies")
	}
	// The same entity via a distinct concrete occurrence must not merge paths.
	diamond := child(root, "t1")
	check(scope(t, facts), input("u1").Claims, pathCandidate(diamond), view, true)
	if graph.nodes[diamond] == graph.nodes[a] || graph.nodes[diamond].parent != rootResult {
		t.Fatal("diamond occurrences were merged")
	}
	// An explicit replacement image may not poison or consume a cached leaf.
	override := pathCandidate(a)
	override.Record = storage.CloneRecord(override.Record)
	override.Record["blocked"] = value("yes")
	check(sa, input("u1").Claims, override, view, false)
	check(sa, input("u1").Claims, pathCandidate(a), view, true)
	// No grants cross subjects, including through an already cached ancestor.
	check(scope(t, facts), input("u2").Claims, pathCandidate(a), view, false)
	if len(view.paths) != 2 || view.paths[1].nodes[root].decision.Visible {
		t.Fatal("claims reused another claims's parent authorization")
	}
	fresh, _ := facts.OpenView(at(10))
	defer fresh.Close()
	check(sa, input("u1").Claims, pathCandidate(a), fresh, true)
	if fresh.paths[0].nodes[root] == rootResult {
		t.Fatal("parent result escaped its evaluation view")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sa.EvaluatePath(ctx, evaluator, input("u1").Claims, pathCandidate(a), view); err == nil {
		t.Fatal("closed view reused cached authorization")
	}
}

func TestAuthorizationGraphRetainsInheritedFactsAndRevokesDescendants(t *testing.T) {
	ctx := context.Background()
	feed := newTestFeed()
	defer feed.Close(ctx)
	projects := storage.RelationName{Namespace: "demo", Name: "projects"}
	evaluator, err := policy.Compile(policy.Map{
		projects: {Facts: []policy.Fact{{ID: "grant", Projection: &plan.Node{Name: "grants", Query: storage.QuerySpec{Relation: relation}}}}, Allow: []policy.Rule{{ID: "grant", When: `facts.grant.exists(g,true)`}}, Propagate: []policy.Edge{{ID: "task", To: tasks, When: `parent.id == item.project_id`}}},
		tasks:    {},
	}, policy.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(ctx, feed, evaluator, input("u1").Claims, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Observe(progress(10, 1, 0)); err != nil {
		t.Fatal(err)
	}
	root := accesscontrol.NewProjectionPath(nil, accesscontrol.ProjectionStep{Node: "project", Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: "demo", Name: "projects"}, Value: "p1"}, Record: storage.Record{"id": value("p1")}, Complete: true})
	child := func(id string) accesscontrol.Candidate {
		key := input("u1").Candidate.Key
		key.Value = id
		return pathCandidate(accesscontrol.NewProjectionPath(root, accesscontrol.ProjectionStep{Node: "task", Key: key, Record: storage.Record{"project_id": value("p1")}, Complete: true}))
	}
	a, b := child("t1"), child("t2")
	oa, ob := Occurrence{Scope: "a", Key: a.Key}, Occurrence{Scope: "b", Key: b.Key}
	view, _ := session.OpenView(at(10))
	check := func(o Occurrence, c accesscontrol.Candidate, view *View, want policy.Decision) {
		t.Helper()
		got, err := session.Evaluate(ctx, o, c, view)
		if err != nil || got != want {
			t.Fatalf("got=%+v err=%v want=%+v", got, err, want)
		}
	}
	check(oa, a, view, policy.Decision{})
	i := receive(t, feed)
	grant := row("grant", "yes")
	install(t, feed, i, snapshot(10, 1, grant))
	if _, err := session.Observe(progress(10, 2, 1)); err != nil {
		t.Fatal(err)
	}
	// Same-view pending evidence is immutable, including reused ancestors.
	check(ob, b, view, policy.Decision{})
	if len(view.paths[0].nodes) != 3 || len(view.paths[0].resolverScope.leases) != 1 {
		t.Fatal("shared parent was not memoized or duplicated its selector")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, _ := session.OpenView(at(10))
	check(oa, a, fresh, policy.Decision{Visible: true, Resolved: true})
	check(ob, b, fresh, policy.Decision{Visible: true, Resolved: true})
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	// Closing the view (and another occurrence) must not release b's inherited
	// grant subscription. It still needs a revocation notification.
	if err := session.Forget(ctx, oa); err != nil {
		t.Fatal(err)
	}
	if len(session.scopes[ob].dependencies) != 1 || feed.closed.Load() != 0 {
		t.Fatal("descendant lost ownership of its inherited fact")
	}
	targets, err := session.Observe(commit(11, 3, 2, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Removed, Row: q.Row{Key: grant.Key}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{grant.Key}}}}))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, target := range targets {
		found = found || target == ob
	}
	if !found {
		t.Fatal("ancestor grant change did not invalidate descendant")
	}
	revoked, _ := session.OpenView(at(11))
	check(ob, b, revoked, policy.Decision{Resolved: true})
	if err := revoked.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Forget(ctx, ob); err != nil {
		t.Fatal(err)
	}
	if feed.closed.Load() != 1 {
		t.Fatal("last graph/occurrence owner did not release selector")
	}
}

func TestRestrictedPropagationRetainsPrunesAndRestoresRequiredFacts(t *testing.T) {
	ctx := t.Context()
	facts, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	s := scope(t, facts)
	projects := storage.RelationName{Namespace: "demo", Name: "projects"}
	evaluator, err := policy.Compile(policy.Map{
		projects: {Allow: []policy.Rule{{ID: "root", When: "true"}}, Propagate: []policy.Edge{{ID: "task", To: tasks, When: `parent.id == item.project_id`}}},
		tasks: {
			Facts:   []policy.Fact{{ID: "grant", Projection: &plan.Node{Name: "grants", Query: storage.QuerySpec{Relation: relation}}}},
			Require: []policy.Rule{{ID: "restricted", When: `item.restricted != true || facts.grant.exists(g, true)`}},
		},
	}, policy.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	root := accesscontrol.NewProjectionPath(nil, accesscontrol.ProjectionStep{Node: "project", Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: "demo", Name: "projects"}, Value: "p1"}, Record: storage.Record{"id": value("p1")}, Complete: true})
	check := func(restricted bool, position uint64, want policy.Decision) {
		t.Helper()
		v := "false"
		if restricted {
			v = "true"
		}
		candidate := input("u1").Candidate
		candidate.Record["restricted"] = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte(v)}
		candidate.Context.Projection = accesscontrol.NewProjectionPath(root, accesscontrol.ProjectionStep{Node: "task", Key: candidate.Key, Record: candidate.Record, Complete: true})
		view, err := facts.OpenView(at(position))
		if err != nil {
			t.Fatal(err)
		}
		defer view.Close()
		got, err := s.EvaluatePath(ctx, evaluator, input("u1").Claims, candidate, view)
		if err != nil || got != want {
			t.Fatalf("restricted=%t at %d: got %+v, %v; want %+v", restricted, position, got, err, want)
		}
	}
	check(false, 10, policy.Decision{Visible: true, Resolved: true})
	if len(s.DependencyIDs()) != 0 {
		t.Fatal("unrestricted propagation expanded private facts")
	}
	check(true, 10, policy.Decision{})
	i := receive(t, feed)
	grant := row("g1", "grant")
	install(t, feed, i, snapshot(10, 1, grant))
	consume(t, facts, feed, progress(10, 1, 0))
	check(true, 10, policy.Decision{Visible: true, Resolved: true})
	consume(t, facts, feed, commit(11, 2, 1, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{
		Changes:    []q.Change{{Kind: q.Removed, Row: q.Row{Key: grant.Key}}},
		Membership: q.MembershipDelta{Removed: []storage.EntityKey{grant.Key}},
	}}))
	check(true, 11, policy.Decision{Resolved: true})
	consume(t, facts, feed, commit(12, 3, 2, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{
		Changes:    []q.Change{{Kind: q.Added, Row: grant}},
		Membership: q.MembershipDelta{Insertions: []q.Insertion{{Key: grant.Key, FinalIndex: 0}}},
	}}))
	check(true, 12, policy.Decision{Visible: true, Resolved: true})
	check(false, 12, policy.Decision{Visible: true, Resolved: true})
	if len(s.DependencyIDs()) != 0 || feed.closed.Load() != 1 {
		t.Fatal("unrestricted item retained obsolete explicit-grant facts")
	}
	check(true, 12, policy.Decision{})
	if next := receive(t, feed); next.request.ID == i.request.ID {
		t.Fatal("restricted item reused a closed fact subscription")
	}
}
