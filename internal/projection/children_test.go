package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type failingChildBindingFeed struct {
	*childBindingFeed
	failure error
}

func (f *failingChildBindingFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if request.Query.Relation.Name != "new" {
		return f.childBindingFeed.SubscribeQuery(ctx, request, install)
	}
	f.started <- struct{}{}
	select {
	case <-f.release:
		return f.failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestChildLoadFailureFollowsExpansionGeneration(t *testing.T) {
	for _, action := range []string{"rebind", "delete", "unchanged", "current"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			failure := errors.New("child load failed")
			feed := &failingChildBindingFeed{childBindingFeed: &childBindingFeed{loads: map[string]int{}, started: make(chan struct{}, 1), release: make(chan struct{})}, failure: failure}
			child := &ProjectionNode{Name: "child", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
			}}
			root := &ProjectionNode{Name: "parents", Children: []*ProjectionNode{child}, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
			}}
			p, _, err := startTestProjection(ctx, feed, root, StartOptions{QueueLimit: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			key := p.graph.instances[p.graph.rootID].order[0]
			updateBoundParent(t, p, key, "new", 11, false)
			select {
			case <-feed.started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			switch action {
			case "rebind":
				updateBoundParent(t, p, key, "old", 12, false)
				publication, err := p.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				completeTestPublication(t, p, publication)
			case "delete":
				updateBoundParent(t, p, key, "new", 12, true)
			case "unchanged":
				updateBoundParent(t, p, key, "new", 12, false)
			}
			close(feed.release)
			if err := p.expansions.waitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			if action == "current" || action == "unchanged" {
				if !errors.Is(p.Err(), failure) {
					t.Fatalf("current load failure lost: %v", p.Err())
				}
			} else if err := p.Err(); err != nil {
				t.Fatalf("obsolete load terminated current projection: %v", err)
			}
		})
	}
}

func TestChildBindingsAreSharedBetweenExpansionAndParentUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed := &childBindingFeed{loads: map[string]int{}, started: make(chan struct{}, 1), release: make(chan struct{})}
	var calls atomic.Int32
	child := &ProjectionNode{Name: "child", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		calls.Add(1)
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}
	root := &ProjectionNode{Name: "parents", Children: []*ProjectionNode{child}, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
	}}
	p, _, err := startTestProjection(ctx, feed, root, StartOptions{QueueLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	key := p.graph.instances[p.graph.rootID].order[0]
	if got := calls.Load(); got != 1 {
		t.Fatalf("initial binding calls = %d, want 1", got)
	}
	for revision := uint64(11); revision <= 12; revision++ {
		updateBoundParent(t, p, key, "old", revision, false)
		if got := calls.Load(); got != int32(revision-9) {
			t.Fatalf("binding calls = %d, want one per parent image", got)
		}
	}
	updateBoundParent(t, p, key, "new", 13, false)
	select {
	case <-feed.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(feed.release)
	if err := p.expansions.waitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("expansion rebound a cached selector: %d calls, want 4", got)
	}
}

func TestChildBindingsBorrowOwnedImageAndBindOnceConcurrently(t *testing.T) {
	record := memberTarget("old")
	record["payload"] = storage.Value{State: storage.ValuePresent, Type: storage.TypeBytes, Data: make([]byte, 1<<20)}
	member := newProjectionMember(record, q.RecordVersion{}, nil, false)
	// The input ownership copy is still required; only repeated binding copies
	// are removed. External mutations cannot change the retained parent image.
	record["target"].Data[0] = 'X'
	var calls atomic.Int32
	child := &ProjectionNode{BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		calls.Add(1)
		if &parent["payload"].Data[0] != &member.record["payload"].Data[0] {
			t.Error("binding copied the immutable parent payload")
		}
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}
	node := &ProjectionNode{Children: []*ProjectionNode{child}}
	p := &Projection{}
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			queries, err := p.childBindings(node, member)
			if err != nil || queries[child].spec.Relation.Name != "old" {
				t.Errorf("binding changed the owned image: %v %v", queries, err)
			}
		})
	}
	workers.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("same image bound %d times, want 1", got)
	}
}

func TestInlineExpansionIgnoresRetiredLoadFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &failingChildBindingFeed{childBindingFeed: &childBindingFeed{started: make(chan struct{}, 1), release: make(chan struct{})}, failure: errors.New("obsolete load failed")}
	child := &ProjectionNode{Name: "child", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "new"}}, true, nil
	}}
	key := storage.EntityKey{Value: "parent"}
	member := newProjectionMember(nil, q.RecordVersion{}, nil, false)
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Children: []*ProjectionNode{child}}, members: map[storage.EntityKey]*projectionMember{key: member}}
	p := &Projection{ctx: ctx, queries: feed, limits: DefaultLimits(), graph: projectionGraph{instances: map[string]*projectionInstance{parent.id: parent}}}
	done := make(chan error, 1)
	go func() {
		_, err := p.loadChildren(projectionExpansion{parent: parent, key: key, member: member})
		done <- err
	}()
	select {
	case <-feed.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.processMu.Lock()
	p.mu.Lock()
	delete(parent.members, key)
	p.mu.Unlock()
	p.processMu.Unlock()
	close(feed.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retired inline expansion failed initialization: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestQueuedFailureForRetiredChildDoesNotCloseReplacement(t *testing.T) {
	p, feed, key := bindingProjection(t)
	p.mu.Lock()
	old := p.graph.instances[p.graph.rootID].members[key].children[1]
	p.mu.Unlock()
	updateBoundParent(t, p, key, "new", 11, false)
	select {
	case <-feed.started:
	case <-p.ctx.Done():
		t.Fatal(p.ctx.Err())
	}
	close(feed.release)
	if err := p.expansions.waitIdle(p.ctx); err != nil {
		t.Fatal(err)
	}
	publication, err := p.Next(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	completeTestPublication(t, p, publication)
	p.mu.Lock()
	retired := p.graph.instances[old] == nil
	p.mu.Unlock()
	if !retired {
		t.Fatal("replacement did not retire its predecessor")
	}
	if _, _, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: old, Cause: errors.New("queued before unsubscribe")}}); err != nil || p.Err() != nil {
		t.Fatalf("retired failure closed replacement: %v %v", err, p.Err())
	}
	currentFailure := errors.New("active root failed")
	if _, _, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: p.graph.rootID, Cause: currentFailure}}); !errors.Is(err, currentFailure) || !errors.Is(p.Err(), currentFailure) {
		t.Fatalf("current failure was ignored: %v %v", err, p.Err())
	}
}

func BenchmarkReconcileChildBindings(b *testing.B) {
	for _, size := range []int{0, 1 << 20} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			record := memberTarget("old")
			record["payload"] = storage.Value{State: storage.ValuePresent, Type: storage.TypeBytes, Data: make([]byte, size)}
			parent := &projectionInstance{node: &ProjectionNode{}}
			for range 8 {
				parent.node.Children = append(parent.node.Children, &ProjectionNode{BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
					return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
				}})
			}
			p := &Projection{}
			previous := &projectionMember{record: record}
			if _, err := p.childBindings(parent.node, previous); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				// The immutable image is already owned by the member. Measure
				// binding/reconciliation only, not the required input-ownership copy.
				next := &projectionMember{record: record}
				if _, _, err := p.reconcileChildren(parent, previous, next); err != nil {
					b.Fatal(err)
				}
				previous = next
			}
		})
	}
}

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
	stable := &ProjectionNode{Name: "stable", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "stable"}}, true, nil
	}}
	bound := &ProjectionNode{Name: "bound", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}
	root := &ProjectionNode{Name: "parents", Children: []*ProjectionNode{stable, bound}, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
	}}
	p, _, err := startTestProjection(ctx, f, root, StartOptions{QueueLimit: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, f, p.graph.instances[p.graph.rootID].order[0]
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
	batch, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: revision, Checkpoint: &q.Position{SourceID: "test", Value: revision}, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: p.graph.rootID, Batch: q.Batch{Changes: []q.Change{{Kind: kind, Row: q.Row{Key: key, Record: record}}}}}}})
	if err != nil || !out {
		t.Fatalf("apply parent: %v %v", out, err)
	}
	return batch
}

func TestChildRebindingPublishesDetachAndReplacementTogether(t *testing.T) {
	p, f, key := bindingProjection(t)
	p.mu.Lock()
	old := append([]string(nil), p.graph.instances[p.graph.rootID].members[key].children...)
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
	ids := append([]string(nil), p.graph.instances[p.graph.rootID].members[key].children...)
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
	member := p.graph.instances[p.graph.rootID].members[key]
	window := p.graph.instances[ids[0]].initial.Bounds
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
	if len(p.graph.instances) != 1 {
		t.Fatal("obsolete load attached after parent deletion")
	}
}

func TestDisplayedPredecessorReceivesDeletionWhileReplacementLoads(t *testing.T) {
	p, f, key := bindingProjection(t)
	p.mu.Lock()
	oldID := p.graph.instances[p.graph.rootID].members[key].children[1]
	oldKey := p.graph.instances[oldID].order[0]
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
	p := &Projection{ctx: context.Background(), graph: projectionGraph{instances: map[string]*projectionInstance{parent.id: parent, child.id: child}}}
	p.graph.instances["old-a"] = &projectionInstance{id: "old-a", node: a, published: true}
	p.graph.instances["old-b"] = &projectionInstance{id: "old-b", node: b, published: true}
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
	child := &ProjectionNode{Name: "child", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Name: "parent", Children: []*ProjectionNode{child}}}
	p := &Projection{ctx: context.Background(), graph: projectionGraph{instances: make(map[string]*projectionInstance)}, publication: publicationLifecycle{outputReleased: true}}
	for range 1000 {
		previous := newProjectionMember(memberTarget("old"), q.RecordVersion{}, nil, false)
		previous.children = []string{"private"}
		next := newProjectionMember(memberTarget("new"), q.RecordVersion{}, nil, false)
		p.graph.instances["private"] = &projectionInstance{id: "private", node: child}
		if events, _, err := p.reconcileChildren(parent, previous, next); err != nil || len(events) != 0 || len(next.replacementRoots) != 0 {
			t.Fatal("retained an unpublished predecessor", err)
		}
	}
}
