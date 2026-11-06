package authorization

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
)

func TestUnchangedProgressDoesNotInvalidateAuthorization(t *testing.T) {
	feed := newTestFeed()
	defer feed.Close(context.Background())
	evaluator, err := policy.Compile(policy.Map{}, policy.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(context.Background(), feed, evaluator, nil, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	changed := s.Changes()
	for revision := uint64(1); revision <= 1000; revision++ {
		if _, err := s.Observe(progress(10, revision, revision-1)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-changed:
		t.Fatal("progress without new evidence woke authorization waiters")
	default:
	}
	if s.version != 0 {
		t.Fatal("progress invalidated an in-flight evaluation")
	}
}

func TestNewOccurrenceRetainsReadinessMemoizedBeforeInstallation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed := newTestFeed()
	defer feed.Close(ctx)
	evaluator, err := policy.Compile(policy.Map{relation: {
		Facts: []policy.Fact{{ID: "grant", Projection: &plan.Node{Name: "grant", Query: storage.QuerySpec{Relation: relation}}}},
		Allow: []policy.Rule{{ID: "grant", When: `facts.grant.exists(g,true)`}},
	}}, policy.Limits{MaxNodes: 100, MaxDepth: 10, MaxEvaluationSteps: 100})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(ctx, feed, evaluator, accesscontrol.Claims{}, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Observe(progress(10, 1, 0)); err != nil {
		t.Fatal(err)
	}
	view, err := s.OpenView(at(10))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	first := row("one", "first")
	a := Occurrence{Scope: "a", Key: first.Key}
	if decision, err := s.Evaluate(ctx, a, accesscontrol.Candidate{Key: first.Key, Record: first.Record, Complete: true}, view); err != nil || decision.Resolved {
		t.Fatalf("expected unresolved evidence: %+v %v", decision, err)
	}
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1, row("grant", "allow")))
	select {
	case <-s.FactChanges():
	case <-ctx.Done():
		t.Fatal("no installation notification")
	}
	s.Refresh()
	epoch, _ := s.FactChangesAfter(0)
	// This occurrence did not exist when Refresh consumed the installation.
	// It shares the view's immutable unknown evidence, not a mutable latest read.
	second := row("two", "second")
	b := Occurrence{Scope: "b", Key: second.Key}
	if decision, err := s.Evaluate(ctx, b, accesscontrol.Candidate{Key: second.Key, Record: second.Record, Complete: true}, view); err != nil || decision.Resolved {
		t.Fatalf("memoized evidence changed: %+v %v", decision, err)
	}
	_, targets := s.FactChangesAfter(epoch)
	found := false
	for _, target := range targets {
		if target == b {
			found = true
		}
	}
	if !found {
		t.Fatal("new occurrence lost the readiness edge")
	}
	fresh, err := s.OpenView(at(10))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if decision, err := s.Evaluate(ctx, b, accesscontrol.Candidate{Key: second.Key, Record: second.Record, Complete: true}, fresh); err != nil || !decision.Resolved || !decision.Visible {
		t.Fatalf("fresh view did not resolve: %+v %v", decision, err)
	}
}
