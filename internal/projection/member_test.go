package projection

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestMemberExpansionStateFollowsRowLifetime(t *testing.T) {
	key := storage.EntityKey{Value: "parent"}
	first := newProjectionMember(nil, q.RecordVersion{}, []byte("old"), false)
	child := &projectionInstance{id: "empty-child", node: &ProjectionNode{Name: "child"}}
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Entity: true, Children: []*ProjectionNode{child.node}}, members: map[storage.EntityKey]*projectionMember{key: first}, published: true}
	p := &Projection{ctx: context.Background(), graph: projectionGraph{instances: map[string]*projectionInstance{parent.id: parent, child.id: child}}}
	oldWork := projectionExpansion{parent: parent, key: key, member: first}
	if !p.graph.expansionCurrentLocked(oldWork) {
		t.Fatal("new member was not expandable")
	}
	if !p.graph.attachExpansionLocked(oldWork, []string{child.id}) {
		t.Fatal("empty child query did not complete its parent expansion")
	}
	if !first.expanded || p.graph.expansionCurrentLocked(oldWork) {
		t.Fatal("empty completed expansion was treated as missing")
	}
	_, missing := p.publicationState([]string{parent.id})
	if len(missing) != 0 {
		t.Fatal("publication tried to reopen an empty completed expansion")
	}
	apply := func(kind q.ChangeKind, pending bool) []projectionExpansion {
		t.Helper()
		_, work, err := p.processBatch(parent, q.DeliveryMetadata{Revision: 2}, q.Batch{Changes: []q.Change{{Kind: kind, Row: q.Row{Key: key, Cursor: []byte("new"), Pending: pending}}}})
		if err != nil {
			t.Fatal(err)
		}
		return work
	}
	apply(q.Updated, true)
	unresolved := parent.members[key]
	if unresolved == first || !unresolved.pending || unresolved.expanded || string(unresolved.cursor) != "new" || p.graph.expansionCurrentLocked(oldWork) {
		t.Fatal("update did not replace the member's complete state")
	}
	if work := apply(q.Updated, false); len(work) != 1 || work[0].member != parent.members[key] || !p.graph.expansionCurrentLocked(work[0]) {
		t.Fatal("resolved row did not get fresh expansion ownership")
	}
	previous := parent.members[key]
	stale := projectionExpansion{parent: parent, key: key, member: previous}
	apply(q.Removed, false)
	if len(parent.members) != 0 || p.graph.expansionCurrentLocked(stale) {
		t.Fatal("removal retained member state or expansion ownership")
	}
	work := apply(q.Added, false)
	if len(work) != 1 || work[0].member == previous || p.graph.expansionCurrentLocked(stale) {
		t.Fatal("readding the same key revived stale expansion ownership")
	}
}

type memberReloadFeed struct {
	boundarySession
	key     storage.EntityKey
	old     chan string
	new     chan string
	release chan struct{}
}

func (f *memberReloadFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	snapshot := q.QuerySnapshot{Checkpoint: &q.Position{SourceID: "test", Value: 10}}
	switch request.Query.Relation.Name {
	case "parents":
		snapshot.Rows = []q.QueryRow{{Row: q.Row{Key: f.key, Record: memberTarget("old")}}}
	case "old":
		f.old <- request.ID
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	case "new":
		f.new <- request.ID
	default:
		panic("unexpected test relation")
	}
	return install(&boundarySubscription{id: request.ID}, snapshot)
}

func memberTarget(target string) storage.Record {
	return storage.Record{"target": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(target)}}
}

func TestInFlightExpansionCannotAttachToReaddedMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "parents"}, Value: "same-key"}
	feed := &memberReloadFeed{key: key, old: make(chan string, 1), new: make(chan string, 1), release: make(chan struct{})}
	root := &ProjectionNode{Name: "parents", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
	}, Children: []*ProjectionNode{{Name: "children", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}}}
	p, err := BeginProjection(ctx, feed, root, StartOptions{QueueLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	receive := func(ch <-chan string) string {
		t.Helper()
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return ""
		}
	}
	oldID := receive(feed.old)
	p.mu.Lock()
	var parent *projectionInstance
	for _, instance := range p.graph.instances {
		if instance.node == root {
			parent = instance
		}
	}
	p.mu.Unlock()
	if parent == nil {
		t.Fatal("missing parent")
	}
	for i, kind := range []q.ChangeKind{q.Removed, q.Added} {
		membership := q.MembershipDelta{Removed: []storage.EntityKey{key}}
		if kind == q.Added {
			membership = q.MembershipDelta{Insertions: []q.Insertion{{Key: key, FinalIndex: 0}}}
		}
		_, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: uint64(i + 11), Checkpoint: &q.Position{SourceID: "test", Value: uint64(i + 11)}}, Effects: []q.SubscriptionEffect{{SubscriptionID: parent.id, Batch: q.Batch{Changes: []q.Change{{Kind: kind, Row: q.Row{Key: key, Record: memberTarget("new")}}}, Membership: membership}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	newID := receive(feed.new)
	close(feed.release)
	if image, _, err := p.TakeInitial(ctx); err != nil || len(image) != 3 {
		t.Fatalf("replacement initial image: events=%d err=%v", len(image), err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	member := parent.members[key]
	if member == nil || !member.expanded || len(member.children) != 1 || member.children[0] != newID || p.graph.instances[oldID] != nil || p.graph.instances[newID] == nil || len(p.graph.instances) != 2 {
		t.Fatal("stale child attached or survived remove/readd")
	}
	if !p.graph.instances[newID].published {
		t.Fatal("replacement expansion was not part of the initial image")
	}
}
