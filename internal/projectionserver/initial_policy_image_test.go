package projectionserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type countedPolicy struct {
	accesscontrol.Prepared
	candidates int
}

func (p *countedPolicy) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	p.candidates += len(candidates)
	return p.Prepared.Evaluate(ctx, view, candidates)
}

func TestInitialPolicyImageRetriesOnlyChangedRowsWhileFactsLoad(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "projection", true: "direct"}[direct], func(t *testing.T) { testInitialPolicyImage(t, direct) })
	}
}

func testInitialPolicyImage(t *testing.T, direct bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	backend := newPolicyBackend(func(ctx context.Context, _ q.QuerySubscribeRequest) q.QuerySnapshot {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return policySnapshot(20, policyRow("grants", "grant", "allow"))
	})
	feed, err := backend.OpenFeed(ctx, q.FeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(ctx)
	definitions := policy.Map{{Namespace: "app", Name: "projects"}: {
		Facts: []policy.Fact{testPolicyFact("grant", storage.RelationName{Namespace: "app", Name: "grants"}, false)},
		Allow: []policy.Rule{{ID: "grant", When: `facts.grant.exists(g,true)`}},
	}}
	evaluator, err := policy.Compile(definitions, policyOptions().Policies.Limits)
	if err != nil {
		t.Fatal(err)
	}
	access, err := newPolicyAccess(ctx, feed, evaluator, accesscontrol.Claims{}, authorization.Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer access.Close()
	prepared, err := access.Prepare(ctx, accesscontrol.PrepareRequest{ID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	counter := &countedPolicy{Prepared: prepared}
	s := &server{access: access, policyAccess: access}
	image := newInitialPolicyImage()
	defer image.release(ctx, prepared)
	previous := uint64(0)
	progress := func(point uint64) {
		t.Helper()
		event := q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: point}, Revision: previous + 1, PreviousRevision: previous}}
		if _, err := access.session.Observe(event); err != nil {
			t.Fatal(err)
		}
		previous++
	}
	progress(10)
	batch := proj.ProjectionBatch{Delivery: q.DeliveryMetadata{Kind: q.DeliveryHydration, Checkpoint: &q.Position{SourceID: "test", Value: 10}}, Events: []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "list", Node: "projects"}}}
	for n := 0; n < 100; n++ {
		row := policyRow("projects", fmt.Sprint(n), "old")
		batch.Events = append(batch.Events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: "list", Node: "projects", Key: row.Key, Record: row.Record, Kind: q.Added})
	}
	var query *initialQueryImage
	if direct {
		snapshot := policySnapshot(10)
		for _, event := range batch.Events[1:] {
			snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: q.Row{Key: event.Key, Record: event.Record}})
		}
		query, err = newInitialQueryImage("query", snapshot)
		if err != nil {
			t.Fatal(err)
		}
		query.observe(&q.Position{SourceID: "test", Value: 10}, previous)
		defer prepared.Release(ctx, query.visibility.id, nil)
	}
	apply := func(batch proj.ProjectionBatch) (bool, error) {
		if direct {
			return query.authorize(ctx, access.session, counter)
		}
		return image.apply(ctx, s, counter, batch)
	}
	if resolved, err := apply(batch); err != nil || resolved {
		t.Fatalf("initial facts should be pending: %v %v", resolved, err)
	}
	if counter.candidates != 100 {
		t.Fatalf("evaluated %d initial rows", counter.candidates)
	}
	receivePolicyRequest(t, ctx, backend, "grants")
	var untouched *projectionVisibilityRow
	if !direct {
		untouched = image.visibility.instances["list"].rows[batch.Events[2].Key]
	}
	for range 3 {
		progress(10)
		if direct {
			query.observe(&q.Position{SourceID: "test", Value: 10}, previous)
		}
		before := counter.candidates
		if resolved, err := apply(proj.ProjectionBatch{Delivery: batch.Delivery}); err != nil || resolved {
			t.Fatalf("still loading: %v %v", resolved, err)
		}
		if counter.candidates != before {
			t.Fatal("unchanged progress reevaluated pending IO")
		}
	}
	_, epochTargets := access.session.FactChangesAfter(0)
	if len(epochTargets) != 0 {
		t.Fatal("unresolved decisions are not new fact evidence")
	}
	for point := uint64(11); point <= 19; point++ {
		// Ordinary ordered source traffic must not retry all pending rows.
		metadata := q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: point}, Revision: previous + 1, PreviousRevision: previous}
		if _, err := access.session.Observe(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: metadata}}); err != nil {
			t.Fatal(err)
		}
		previous++
		row := policyRow("projects", "0", fmt.Sprint(point))
		batch.Delivery.Checkpoint.Value = point
		batch.Events = []proj.ProjectionEvent{{Type: proj.ProjectionRowChanged, InstanceID: "list", Node: "projects", Key: row.Key, Record: row.Record, Kind: q.Updated}}
		if direct {
			if err := query.apply(q.DeliveryEnvelope{Delivery: metadata, Effects: []q.SubscriptionEffect{{SubscriptionID: "query", Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: row}}}}}}); err != nil {
				t.Fatal(err)
			}
		}
		before := counter.candidates
		if resolved, err := apply(batch); err != nil || resolved {
			t.Fatalf("pending update: %v %v", resolved, err)
		}
		if counter.candidates-before != 1 {
			t.Fatalf("one hot row reevaluated %d candidates", counter.candidates-before)
		}
	}
	if !direct && untouched != image.visibility.instances["list"].rows[policyRow("projects", "1", "").Key] {
		t.Fatal("unchanged row was replaced")
	}
	close(release)
	select {
	case <-access.session.FactChanges():
	case <-ctx.Done():
		t.Fatal("fact snapshot was not installed")
	}
	access.session.Refresh()
	epoch, targets := access.session.FactChangesAfter(0)
	if epoch == 0 || len(targets) != 100 {
		t.Fatalf("missing dependent notifications: epoch %d, targets %d", epoch, len(targets))
	}
	if _, repeated := access.session.FactChangesAfter(epoch); len(repeated) != 0 {
		t.Fatal("fact notifications repeated at the same epoch")
	}
	// The loaded snapshot is too new for position 19. It must stay unknown
	// until ordered source progress covers 20, then all rows resolve once.
	batch.Events = nil
	if resolved, err := apply(batch); err != nil || resolved {
		t.Fatalf("used facts before their baseline: %v %v", resolved, err)
	}
	progress(19)
	if direct {
		query.observe(&q.Position{SourceID: "test", Value: 19}, previous)
	}
	before := counter.candidates
	if resolved, err := apply(batch); err != nil || resolved {
		t.Fatalf("baseline still ahead: %v %v", resolved, err)
	}
	if counter.candidates != before {
		t.Fatal("uncovered snapshot retried on repeated progress")
	}
	// A commit can cross the baseline too: readiness must not depend on a
	// special progress message arriving later.
	metadata := q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 20}, Revision: previous + 1, PreviousRevision: previous}
	if _, err := access.session.Observe(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: metadata}}); err != nil {
		t.Fatal(err)
	}
	if direct {
		if err := query.apply(q.DeliveryEnvelope{Delivery: metadata}); err != nil {
			t.Fatal(err)
		}
	}
	batch.Delivery.Checkpoint = &q.Position{SourceID: "test", Value: 20}
	if resolved, err := apply(batch); err != nil || !resolved {
		t.Fatalf("covered facts did not resolve: %v %v", resolved, err)
	}
	if direct {
		snapshot := query.visibleSnapshot()
		if len(snapshot.Rows) != 100 || string(snapshot.Rows[0].Record["title"].Data) != "19" {
			t.Fatal("final query image lost current rows")
		}
		if query.visibility.rows[snapshot.Rows[0].Key].version != query.versions[snapshot.Rows[0].Key] {
			t.Fatal("authorization changed row version")
		}
		return
	}
	events := image.borrowEvents()
	if len(events) != 101 || string(events[1].Record["title"].Data) != "19" {
		t.Fatalf("final image lost current rows: %d events", len(events))
	}
}
