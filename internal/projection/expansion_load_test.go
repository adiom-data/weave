package projection

import (
	"context"
	"errors"
	"reflect"
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestChildRecoveryWindowFollowsReconciledOwnership(t *testing.T) {
	for _, rebind := range []bool{false, true} {
		name := "equivalent parent update"
		if rebind {
			name = "changed selector"
		}
		t.Run(name, func(t *testing.T) {
			child := recoveryNode("child")
			child.BuildQuery = func(parent storage.Record) (storage.QuerySpec, bool, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
			}
			node := recoveryNode("parent", child)
			key := storage.EntityKey{Value: "parent"}
			before := newProjectionMember(memberTarget("old"), q.RecordVersion{}, nil, false)
			parent := &projectionInstance{id: "parent", node: node, published: true, members: map[storage.EntityKey]*projectionMember{key: before}}
			spec, matchable, err := child.BuildQuery(before.record)
			if err != nil {
				t.Fatal(err)
			}
			saved := q.Bounds{Left: q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("m")}, Right: q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("t")}}
			old := &projectionInstance{id: "old-child", node: child, spec: spec, failure: errors.New("failed"), initial: projectionInitialState{Bounds: saved}}
			before.replacementRoots = map[*ProjectionNode][]string{child: {old.id}}
			feed := &recoveryFeed{requests: make(chan q.QuerySubscribeRequest, 1)}
			p := &Projection{ctx: t.Context(), queries: feed, limits: DefaultLimits(), graph: projectionGraph{instances: map[string]*projectionInstance{parent.id: parent, old.id: old}}}
			expansion := projectionExpansion{parent: parent, key: key, member: before}
			// The new immutable row image is not itself the selector lifetime.
			// Admission must use the ownership restored by reconciliation, not
			// treat a different member pointer as the absence of saved bounds.
			target := "old"
			if rebind {
				target = "new"
			}
			next := newProjectionMember(memberTarget(target), q.RecordVersion{Revision: 1}, nil, false)
			p.processMu.Lock()
			p.mu.Lock()
			parent.members[key] = next
			p.mu.Unlock()
			_, same, err := p.reconcileChildren(parent, before, next)
			p.processMu.Unlock()
			wantSame := !rebind
			if err != nil || same != wantSame {
				t.Fatalf("reconcile ownership: same=%t err=%v", same, err)
			}
			if rebind {
				if _, err := p.loadNode(child, boundQuery{spec: spec, matchable: matchable}, nil, &expansion); !errors.Is(err, q.ErrClosed) {
					t.Fatalf("obsolete recovery was admitted: %v", err)
				}
				if len(feed.requests) != 0 {
					t.Fatal("obsolete selector reached the query feed")
				}
				expansion.member = next
				spec, matchable, err = child.BuildQuery(next.record)
				if err != nil {
					t.Fatal(err)
				}
			}
			instance, err := p.loadNode(child, boundQuery{spec: spec, matchable: matchable}, nil, &expansion)
			if err != nil {
				t.Fatal(err)
			}
			defer p.closeInstance(instance.id)
			request := <-feed.requests
			if rebind {
				if request.Window.Bounds != nil || request.Window.Count != child.Window.Count {
					t.Fatalf("new selector reused old cursors: %+v", request.Window)
				}
			} else if request.Window.Count != 0 || !reflect.DeepEqual(request.Window.Bounds, &saved) || !reflect.DeepEqual(instance.initial.Bounds, saved) {
				t.Fatalf("equivalent parent update lost the recovery window: %+v", request.Window)
			}
		})
	}
}

func TestExpansionCompletionUsesCurrentCoverageBeforeTransferringOwnership(t *testing.T) {
	key := storage.EntityKey{Value: "row"}
	first, second := &ProjectionNode{Name: "first"}, &ProjectionNode{Name: "second"}
	before := newProjectionMember(nil, q.RecordVersion{}, nil, false)
	before.children = []string{"old-second"}
	current := newProjectionMember(nil, q.RecordVersion{}, nil, false)
	current.expansionToken = before.expansionToken
	parent := &projectionInstance{id: "parent", node: &ProjectionNode{Children: []*ProjectionNode{first, second}}, members: map[storage.EntityKey]*projectionMember{key: current}}
	expansion := projectionExpansion{parent: parent, key: key, member: before}
	opened := &projectionInstance{id: "new-first", node: first, opening: &expansion}
	p := &Projection{ctx: context.Background(), graph: projectionGraph{instances: map[string]*projectionInstance{parent.id: parent, "old-second": {id: "old-second", node: second}, opened.id: opened}}}
	if p.graph.attachExpansionLocked(expansion, []string{opened.id}) {
		t.Fatal("an obsolete row image hid the current sibling coverage gap")
	}
	if current.expanded || len(current.children) != 0 || opened.opening == nil {
		t.Fatal("an incomplete pass transferred private interests")
	}
	replacement := &projectionInstance{id: "new-second", node: second, opening: &expansion, failure: errors.New("replacement remains errored")}
	p.graph.instances[replacement.id] = replacement
	current.replacementRoots = map[*ProjectionNode][]string{second: {"old-second"}}
	if p.graph.failedReplacementLocked(second, &expansion) != nil {
		t.Fatal("healthy predecessor unexpectedly counted as a failed replacement")
	}
	p.graph.instances["old-second"].failure = errors.New("old source failed")
	if p.graph.failedReplacementLocked(second, &expansion) != p.graph.instances["old-second"] {
		t.Fatal("replacement lookup used the old row image instead of current ownership")
	}
	if !p.graph.attachExpansionLocked(expansion, []string{opened.id, replacement.id}) {
		t.Fatal("complete child slots, including explicit failure placeholders, were not accepted")
	}
	if !current.expanded || len(current.children) != 2 || opened.opening != nil || replacement.opening != nil || len(before.children) != 1 {
		t.Fatal("completion did not transfer all interests into the current row")
	}
}
