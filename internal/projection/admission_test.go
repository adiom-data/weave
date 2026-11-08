package projection

import (
	"context"
	"strconv"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type admissionFenceFeed struct{ boundarySession }

func TestAdmissionReadinessDoesNotCaptureAnUnusedImage(t *testing.T) {
	for _, synchronize := range []bool{false, true} {
		t.Run(strconv.FormatBool(synchronize), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			p, err := BeginProjection(ctx, &admissionTreeFeed{}, &ProjectionNode{Name: "items", BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, nil
			}}, StartOptions{QueueLimit: 1, CurrentImageAdmission: true, WaitForSync: synchronize})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			err = p.WaitReady(ctx)
			if err != nil || len(p.readyEvents) != 0 {
				t.Fatalf("admission readiness retained a discarded image: err=%v", err)
			}
			if _, _, err := p.TakeInitial(ctx); err == nil {
				t.Fatal("ordinary initial-image ownership bypassed admission")
			}
			if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: 1}}); err != nil {
				t.Fatal(err)
			}
			wait, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
				if len(batch.Events) != 2 || string(batch.Events[1].Record["id"].Data) != "one" {
					t.Fatal("admission did not capture the authoritative image")
				}
				return true, nil
			})
			if err != nil || !wait.Accepted {
				t.Fatalf("admission: %+v %v", wait, err)
			}
			if _, _, err := p.TakeInitial(ctx); err == nil {
				t.Fatal("admission left a second initial-image handoff")
			}
		})
	}
}

func (*admissionFenceFeed) SubscribeEntity(_ context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	return install(boundaryEntitySubscription{Subscription: &boundarySubscription{id: request.ID}}, q.EntitySnapshot{Checkpoint: &q.Position{SourceID: "test", Value: 10}, Fence: &q.Position{SourceID: "test", Value: 20}})
}

func TestCurrentImageAdmissionCompactsBeforeCutAndPreservesAfterCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := BeginProjection(ctx, &admissionFenceFeed{}, &ProjectionNode{Name: "item", Entity: true, BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil }}, StartOptions{QueueLimit: 1, CurrentImageAdmission: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(func(ProjectionBatch) error { return nil }); err == nil {
		t.Fatal("released output before admission")
	}
	if err := p.QueueWindowUpdate(ctx, p.rootID, nil, "early"); err == nil {
		t.Fatal("accepted window request before admission")
	}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	apply := func(point uint64, related bool) (ProjectionBatch, bool, error) {
		envelope := q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: point}, Revision: point}}
		if related {
			envelope.Effects = []q.SubscriptionEffect{{SubscriptionID: p.rootID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(strconv.FormatUint(point, 10))}}}}}}}}
		}
		return p.ApplyDelivery(envelope)
	}
	for point := uint64(21); point <= 40; point++ {
		if _, out, err := apply(point, point%2 == 0); err != nil || out {
			t.Fatalf("pre-cut output: %v %v", out, err)
		}
		// Dynamic publications are also superseded by the eventual full image.
		if err := p.publishSubtrees([]string{p.rootID}, point); err != nil {
			t.Fatal(err)
		}
		if len(p.pendingOutput) != 0 {
			t.Fatal("retained superseded pre-cut output")
		}
	}
	wait, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
		if len(batch.Events) != 2 || string(batch.Events[1].Record["title"].Data) != "40" {
			t.Fatalf("stale initial materialization: %+v", batch)
		}
		return false, nil
	})
	if err != nil || wait.Accepted || !p.awaitingAdmission {
		t.Fatalf("rejected cut changed publication state: %+v %v", wait, err)
	}
	wait, err = p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
		if len(batch.Events) != 0 || batch.Delivery.Checkpoint.Value != 40 {
			t.Fatalf("unchanged materialization was recopied: %+v", batch)
		}
		return true, nil
	})
	if err != nil || !wait.Accepted {
		t.Fatalf("admission failed: %+v %v", wait, err)
	}
	// The gap after acceptance but before public installation still needs
	// catch-up. It must not be compacted into an already accepted image.
	if _, out, err := apply(41, true); err != nil || out {
		t.Fatalf("post-cut staging: %v %v", out, err)
	}
	var caught []ProjectionBatch
	if err := p.releaseOutput(func(batch ProjectionBatch) error { caught = append(caught, batch); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(caught) != 1 || caught[0].Delivery.Revision != 41 {
		t.Fatalf("lost post-cut transaction: %+v", caught)
	}
	if batch, out, err := apply(42, true); err != nil || !out || batch.Delivery.Revision != 42 {
		t.Fatalf("live output broken: %+v %v %v", batch, out, err)
	}
}

type admissionTreeFeed struct{ boundarySession }

func (*admissionTreeFeed) SubscribeQuery(_ context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: request.Query.Relation.Name}, Value: "one"}
	return install(&boundarySubscription{id: request.ID}, q.QuerySnapshot{Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}}}}, Bounds: q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}}, Checkpoint: &q.Position{SourceID: "test", Value: 10}})
}

func TestCurrentImageAdmissionKeepsUnpublishedDescendantsCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	node := func(name string, children ...*ProjectionNode) *ProjectionNode {
		return &ProjectionNode{Name: name, Children: children, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: name}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "binding", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: parent["binding"].Data}}}}, nil
		}}
	}
	p, err := BeginProjection(ctx, &admissionTreeFeed{}, node("roots", node("children", node("leaves"))), StartOptions{QueueLimit: 1, CurrentImageAdmission: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	// Capture before replacing either subtree, so the next admission must
	// describe retirement and installation rather than another full image.
	if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: 10}}); err != nil {
		t.Fatal(err)
	}
	image := &deltaImage{}
	if _, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
		image.apply(t, batch)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	update := func(id string, point uint64) {
		t.Helper()
		p.mu.Lock()
		key := p.instances[id].order[0]
		p.mu.Unlock()
		_, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: point}, Revision: point}, Effects: []q.SubscriptionEffect{{SubscriptionID: id, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}, "binding": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(strconv.FormatUint(point, 10))}}}}}}}}})
		if err != nil || out {
			t.Fatalf("pre-admission update failed: %v %v", out, err)
		}
		if err := p.expansions.waitIdle(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Replacing the root creates a complete but unpublished subtree. Updating
	// its child must expand its replacement leaf even without a queued token.
	update(p.rootID, 21)
	p.mu.Lock()
	root := p.instances[p.rootID]
	childID := root.members[root.order[0]].children[0]
	childPublished := p.instances[childID].published
	p.mu.Unlock()
	if childPublished {
		t.Fatal("test child was already published")
	}
	update(childID, 22)
	wait, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
		image.apply(t, batch)
		events, err := p.currentEvents(p.rootID)
		if err != nil {
			return false, err
		}
		if len(events) != 6 || len(image.rows) != 3 {
			t.Fatalf("missing descendant or retained retired instance: %+v", image)
		}
		for _, event := range events {
			if event.Type == ProjectionRowChanged {
				if got, exists := image.rows[event.InstanceID][event.Key]; !exists || got.Version != event.Version {
					t.Fatalf("delta lost current descendant: %+v", event)
				}
			}
		}
		return true, nil
	})
	if err != nil || !wait.Accepted {
		t.Fatalf("admission failed: %+v %v", wait, err)
	}
	if len(p.pendingOutput) != 0 {
		t.Fatal("retained pre-admission publication tokens")
	}
}

func TestAdmissionRequiresSourceProgressPastNewestSnapshotBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := BeginProjection(ctx, &admissionFenceFeed{}, &ProjectionNode{Name: "item", Entity: true, BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil }}, StartOptions{QueueLimit: 10, CurrentImageAdmission: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	called := false
	accept := func(ProjectionBatch) (bool, error) { called = true; return true, nil }
	for _, kind := range []q.DeliveryKind{q.DeliveryWindow, q.DeliveryHydration} {
		if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: kind, Checkpoint: &q.Position{SourceID: "test", Value: 30}, Revision: 2}}); err != nil {
			t.Fatal(err)
		}
		wait, err := p.TryAdmitInitial(accept)
		if err != nil || wait.Accepted || len(wait.Progress) == 0 || called {
			t.Fatalf("local delivery bypassed freshness fence: %+v %v", wait, err)
		}
	}
	if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 20}, Revision: 3}}); err != nil {
		t.Fatal(err)
	}
	wait, err := p.TryAdmitInitial(accept)
	if err != nil || !wait.Accepted || !called {
		t.Fatalf("covered fence was not admitted: %+v %v", wait, err)
	}
}
