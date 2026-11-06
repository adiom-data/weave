package projection

import (
	"context"
	"sync"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type childBindingFeed struct {
	boundarySession
	mu      sync.Mutex
	loads   map[string]int
	started chan struct{}
	release chan struct{}
}

func (f *childBindingFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	name := request.Query.Relation.Name
	f.mu.Lock()
	f.loads[name]++
	f.mu.Unlock()
	if name == "new" {
		select {
		case f.started <- struct{}{}:
		default:
		}
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	record := memberTarget("old")
	record["id"] = storage.Value{State: storage.ValuePresent, Type: storage.TypeText, Data: []byte("one")}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: name}, Value: "one"}
	return install(&boundarySubscription{id: request.ID}, q.QuerySnapshot{Checkpoint: &q.Position{SourceID: "test", Value: 10}, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}}}})
}

func bindingProjection(t *testing.T) (*Projection, *childBindingFeed, storage.EntityKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	f := &childBindingFeed{loads: map[string]int{}, started: make(chan struct{}, 10), release: make(chan struct{})}
	stable := &ProjectionNode{Name: "stable", BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "stable"}}, nil
	}}
	bound := &ProjectionNode{Name: "bound", BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, nil
	}}
	root := &ProjectionNode{Name: "parents", Children: []*ProjectionNode{stable, bound}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, nil
	}}
	p, _, err := startTestProjection(ctx, f, root, StartOptions{QueueLimit: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, f, p.instances[p.rootID].order[0]
}

func updateBoundParent(t *testing.T, p *Projection, key storage.EntityKey, target string, revision uint64, remove bool) ProjectionBatch {
	t.Helper()
	record := memberTarget(target)
	record["id"] = storage.Value{State: storage.ValuePresent, Type: storage.TypeText, Data: []byte("one")}
	record["title"] = storage.Value{State: storage.ValuePresent, Type: storage.TypeText, Data: []byte("changed")}
	kind := q.Updated
	if remove {
		kind = q.Removed
	}
	batch, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: revision, Checkpoint: &q.Position{SourceID: "test", Value: revision}, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: p.rootID, Batch: q.Batch{Changes: []q.Change{{Kind: kind, Row: q.Row{Key: key, Record: record}}}}}}})
	if err != nil || !out {
		t.Fatalf("apply parent: %v %v", out, err)
	}
	return batch
}

func TestChildRebindingPublishesDetachAndReplacementTogether(t *testing.T) {
	p, f, key := bindingProjection(t)
	p.mu.Lock()
	old := append([]string(nil), p.instances[p.rootID].members[key].children...)
	p.mu.Unlock()
	batch := updateBoundParent(t, p, key, "new", 11, false)
	if len(batch.Events) != 1 || batch.Events[0].Type != ProjectionRowChanged {
		t.Fatalf("parent update cleared children: %+v", batch.Events)
	}
	select {
	case <-f.started:
	case <-p.ctx.Done():
		t.Fatal("replacement did not start")
	}
	// A further parent field change must not restart this in-flight binding.
	batch = updateBoundParent(t, p, key, "new", 12, false)
	if len(batch.Events) != 1 {
		t.Fatal("same selector changed topology")
	}
	close(f.release)
	if err := p.expansions.waitIdle(p.ctx); err != nil {
		t.Fatal(err)
	}
	publication, err := p.Next(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	publication = completeTestPublication(t, p, publication)
	closed, opened := 0, 0
	for _, event := range publication.Events {
		switch event.Type {
		case ProjectionInstanceClosed:
			closed++
			if event.InstanceID != old[1] {
				t.Fatal("unchanged sibling was closed")
			}
		case ProjectionInstanceOpened:
			opened++
		}
	}
	if closed != 1 || opened != 1 {
		t.Fatalf("replacement was not one structural switch: %+v", publication.Events)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loads["stable"] != 1 || f.loads["old"] != 1 || f.loads["new"] != 1 {
		t.Fatalf("redundant child subscriptions: %v", f.loads)
	}
}

func TestUnchangedChildBindingsPreserveWindowAndInstance(t *testing.T) {
	p, f, key := bindingProjection(t)
	p.mu.Lock()
	ids := append([]string(nil), p.instances[p.rootID].members[key].children...)
	p.mu.Unlock()
	bounds := q.Bounds{Left: q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("left")}, Right: q.Boundary{Kind: q.BoundaryEnd}}
	if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryWindow, Revision: 11}, Effects: []q.SubscriptionEffect{{SubscriptionID: ids[0], Batch: q.Batch{Window: &q.WindowTransition{Token: "resize", Bounds: bounds}}}}}); err != nil {
		t.Fatal(err)
	}
	batch := updateBoundParent(t, p, key, "old", 12, false)
	if len(batch.Events) != 1 || len(batch.Membership) != 0 {
		t.Fatalf("unrelated parent update modified its children: %+v", batch)
	}
	p.mu.Lock()
	member := p.instances[p.rootID].members[key]
	window := p.instances[ids[0]].initial.Bounds
	retained := len(member.children) == 2 && member.children[0] == ids[0] && member.children[1] == ids[1]
	p.mu.Unlock()
	if !retained || string(window.Left.Cursor) != "left" || window.Right.Kind != q.BoundaryEnd {
		t.Fatal("parent update lost child identity or resized window")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loads["old"] != 1 || f.loads["stable"] != 1 {
		t.Fatalf("unchanged children were reloaded: %v", f.loads)
	}
}

func TestParentDeletionFlushesDeferredChildDetachImmediately(t *testing.T) {
	p, f, key := bindingProjection(t)
	updateBoundParent(t, p, key, "new", 11, false)
	select {
	case <-f.started:
	case <-p.ctx.Done():
		t.Fatal("replacement did not start")
	}
	batch := updateBoundParent(t, p, key, "new", 12, true)
	closed := 0
	for _, event := range batch.Events {
		if event.Type == ProjectionInstanceClosed {
			closed++
		}
	}
	if closed != 2 {
		t.Fatalf("parent deletion did not retire both public children: %+v", batch.Events)
	}
	close(f.release)
	if err := p.expansions.waitIdle(p.ctx); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.instances) != 1 {
		t.Fatal("obsolete load attached after parent deletion")
	}
}

func TestDisplayedPredecessorReceivesDeletionWhileReplacementLoads(t *testing.T) {
	p, f, key := bindingProjection(t)
	p.mu.Lock()
	oldID := p.instances[p.rootID].members[key].children[1]
	oldKey := p.instances[oldID].order[0]
	p.mu.Unlock()
	updateBoundParent(t, p, key, "new", 11, false)
	select {
	case <-f.started:
	case <-p.ctx.Done():
		t.Fatal("replacement did not start")
	}
	batch, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 12, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: oldID, Batch: q.Batch{
		Changes:    []q.Change{{Kind: q.Removed, Row: q.Row{Key: oldKey}}},
		Membership: q.MembershipDelta{Removed: []storage.EntityKey{oldKey}},
	}}}})
	if err != nil || !out || len(batch.Events) != 1 || batch.Events[0].InstanceID != oldID || batch.Events[0].Kind != q.Removed {
		t.Fatalf("deletion was hidden behind replacement loading: %+v %v", batch, err)
	}
	close(f.release)
	if err := p.expansions.waitIdle(p.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSiblingPublicationDoesNotDetachAnotherPendingReplacement(t *testing.T) {
	a, b := &ProjectionNode{Name: "a"}, &ProjectionNode{Name: "b"}
	key := storage.EntityKey{Value: "parent"}
	member := newProjectionMember(nil, q.RecordVersion{}, nil, false)
	member.children = []string{"new-a"}
	member.replacementRoots = map[*ProjectionNode][]string{
		a: {"old-a"},
		b: {"old-b"},
	}
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Name: "parent"}, members: map[storage.EntityKey]*projectionMember{key: member}}
	child := &projectionInstance{id: "new-a", node: a, parentID: parent.id, parentKey: key}
	p := &Projection{ctx: context.Background(), instances: map[string]*projectionInstance{parent.id: parent, child.id: child}}
	p.instances["old-a"] = &projectionInstance{id: "old-a", node: a, published: true}
	p.instances["old-b"] = &projectionInstance{id: "old-b", node: b, published: true}
	batch, err := p.completePublication(projectionPublication{InstanceIDs: []string{child.id}})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 2 || batch.Events[0].InstanceID != "old-a" || batch.Events[1].InstanceID != "new-a" {
		t.Fatalf("sibling publication detached another slot: %+v", batch.Events)
	}
	if len(member.replacementRoots[b]) != 1 || member.replacementRoots[a] != nil {
		t.Fatal("publication consumed another slot's replacement ownership")
	}
}

func TestRetiringUnpublishedLoadsDoesNotAccumulateReplacements(t *testing.T) {
	child := &ProjectionNode{Name: "child", BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, nil
	}}
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Name: "parent", Children: []*ProjectionNode{child}}}
	p := &Projection{ctx: context.Background(), instances: make(map[string]*projectionInstance), outputReleased: true}
	for range 1000 {
		previous := newProjectionMember(memberTarget("old"), q.RecordVersion{}, nil, false)
		previous.children = []string{"private"}
		next := newProjectionMember(memberTarget("new"), q.RecordVersion{}, nil, false)
		p.instances["private"] = &projectionInstance{id: "private", node: child}
		if events, _, err := p.reconcileChildren(parent, previous, next); err != nil || len(events) != 0 || len(next.replacementRoots) != 0 {
			t.Fatal("retained an unpublished predecessor", err)
		}
	}
}
