package projection

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type openingInterestFixture struct {
	projection *Projection
	feed       *partialExpansionFeed
	parentKey  storage.EntityKey
	interestID string
	invalid    q.SubscriptionEffect
}

// One child has installed (including its descendants), but its sibling still
// loads. The selected occurrence has a normal plan binding that fails if its
// scalar target becomes an object. It must matter only while that path is live.
func openingBoundInterest(t *testing.T, nested bool) openingInterestFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	feed := &partialExpansionFeed{childBindingFeed: &childBindingFeed{loads: map[string]int{}}, installed: make(chan string, 1), blocked: make(chan struct{}), resume: make(chan struct{})}
	root := &ProjectionNode{Name: "parents", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
	}}
	for _, suffix := range []string{"one", "two"} {
		child := &ProjectionNode{Name: suffix, BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data) + "_" + suffix}}, true, nil
		}}
		if suffix == "one" {
			leaf := &plan.Node{Name: "leaf", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "leaf"}}, Bindings: []plan.Binding{{Field: "owner_id", Operator: storage.ComparisonEqual, Parent: "target"}}}
			if nested {
				leaf.Children = []*plan.Node{{Name: "descendant", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "descendant"}}, Bindings: leaf.Bindings}}
			}
			child.Children = []*ProjectionNode{FromPlan(leaf, nil, nil)}
		}
		root.Children = append(root.Children, child)
	}
	p, _, err := startTestProjection(ctx, feed, root, StartOptions{QueueLimit: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	parentKey := p.graph.instances[p.graph.rootID].order[0]
	updateBoundParent(t, p, parentKey, "new", 11, false)
	select {
	case <-feed.blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var id string
	select {
	case id = <-feed.installed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.mu.Lock()
	instance := p.graph.instances[id]
	if nested {
		id = instance.members[instance.order[0]].children[0]
		instance = p.graph.instances[id]
	}
	key := instance.order[0]
	record := storage.CloneRecord(instance.members[key].record)
	p.mu.Unlock()
	record["target"] = storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{}}
	return openingInterestFixture{projection: p, feed: feed, parentKey: parentKey, interestID: id, invalid: q.SubscriptionEffect{
		SubscriptionID: id, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}},
	}}
}

func interestCommit(revision uint64) q.DeliveryMetadata {
	return q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: revision}, Revision: revision, Atomic: true}
}

func (f openingInterestFixture) parentChange(action string) q.SubscriptionEffect {
	p := f.projection
	p.mu.Lock()
	record := storage.CloneRecord(p.graph.instances[p.graph.rootID].members[f.parentKey].record)
	p.mu.Unlock()
	batch := q.Batch{}
	kind := q.Updated
	if action == "delete" {
		kind, record = q.Removed, nil
		batch.Membership.Removed = []storage.EntityKey{f.parentKey}
	} else {
		record["target"] = memberTarget("old")["target"]
	}
	batch.Changes = []q.Change{{Kind: kind, Row: q.Row{Key: f.parentKey, Record: record}}}
	return q.SubscriptionEffect{SubscriptionID: p.graph.rootID, Batch: batch}
}

func TestInterestDataAndFailuresUseTheSameOwnership(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, action := range []string{"delete", "rebind", "unchanged", "current"} {
			t.Run(map[bool]string{false: "child/", true: "descendant/"}[nested]+action, func(t *testing.T) {
				f := openingBoundInterest(t, nested)
				p := f.projection
				if action == "delete" || action == "rebind" {
					if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: interestCommit(12), Effects: []q.SubscriptionEffect{f.parentChange(action)}}); err != nil {
						t.Fatal(err)
					}
				} else if action == "unchanged" {
					updateBoundParent(t, p, f.parentKey, "new", 12, false)
				}
				metadata := interestCommit(13)
				batch, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: metadata, Effects: []q.SubscriptionEffect{f.invalid}})
				if action == "current" || action == "unchanged" {
					if err == nil || !strings.Contains(err.Error(), "query literal requires a present scalar value") {
						t.Fatalf("current child binding failure was hidden: %v", err)
					}
					return
				}
				if err != nil || !out || p.Err() != nil || len(batch.Events) != 0 || len(batch.Membership) != 0 || batch.Window != nil {
					t.Fatalf("obsolete payload was evaluated or published: %+v out=%v err=%v terminal=%v", batch, out, err, p.Err())
				}
				p.mu.Lock()
				position, revision, retired := p.latestCheckpoints["test"], p.latestRevision, p.graph.instances[f.interestID] == nil
				p.mu.Unlock()
				if !retired || revision != metadata.Revision || batch.Delivery.Revision != metadata.Revision || batch.Delivery.Kind != metadata.Kind || !q.SamePosition(position, metadata.Checkpoint) || !q.SamePosition(batch.Delivery.Checkpoint, metadata.Checkpoint) || !batch.Delivery.Atomic {
					t.Fatalf("obsolete-interest cleanup lost transaction progress: retired=%v revision=%d position=%v delivery=%+v", retired, revision, position, batch.Delivery)
				}
				close(f.feed.resume)
				if err := p.expansions.waitIdle(p.ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestParentTopologySettlesBeforeChildEffectsInSameTransaction(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, action := range []string{"delete", "rebind"} {
			for _, childFirst := range []bool{false, true} {
				name := map[bool]string{false: "child/", true: "descendant/"}[nested] + action + map[bool]string{false: "/parent-first", true: "/child-first"}[childFirst]
				t.Run(name, func(t *testing.T) {
					f := openingBoundInterest(t, nested)
					effects := []q.SubscriptionEffect{f.parentChange(action), f.invalid}
					if childFirst {
						effects[0], effects[1] = effects[1], effects[0]
					}
					batch, out, err := f.projection.ApplyDelivery(q.DeliveryEnvelope{Delivery: interestCommit(12), Effects: effects})
					if err != nil || !out || !batch.Delivery.Atomic {
						t.Fatalf("transaction depended on upstream subscription order: %+v %v", batch, err)
					}
					parentChanged := false
					for _, event := range batch.Events {
						if event.Type == ProjectionRowChanged && event.InstanceID == f.projection.graph.rootID {
							parentChanged = true
						}
						if event.InstanceID == f.interestID {
							t.Fatal("obsolete child payload escaped into transaction", event)
						}
					}
					if !parentChanged {
						t.Fatal("discarded a live parent effect with its obsolete child")
					}
				})
			}
		}
	}
}

func TestObsoleteInstalledInterestPreservesNoncommitMetadata(t *testing.T) {
	for _, window := range []bool{false, true} {
		t.Run(map[bool]string{false: "hydration", true: "window"}[window], func(t *testing.T) {
			f := openingBoundInterest(t, true)
			p := f.projection
			if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: interestCommit(12), Effects: []q.SubscriptionEffect{f.parentChange("delete")}}); err != nil {
				t.Fatal(err)
			}
			effect := f.invalid
			metadata := q.DeliveryMetadata{Kind: q.DeliveryHydration, Revision: 13, Checkpoint: &q.Position{SourceID: "test", Value: 30}}
			if window {
				metadata.Kind, metadata.Checkpoint = q.DeliveryWindow, nil
				effect.Batch.Window = &q.WindowTransition{Token: "resize", Bounds: q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}}}
			}
			batch, out, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: metadata, Effects: []q.SubscriptionEffect{effect}})
			if err != nil || !out || batch.Window != nil || len(batch.Events) != 0 || batch.Delivery.Revision != metadata.Revision || batch.Delivery.Kind != metadata.Kind || !q.SamePosition(batch.Delivery.Checkpoint, metadata.Checkpoint) {
				t.Fatalf("discarding obsolete data lost delivery metadata: %+v out=%v err=%v", batch, out, err)
			}
			if window && (batch.WindowFailure == nil || batch.WindowFailure.Token != "resize" || batch.WindowFailure.InstanceID != f.interestID) {
				t.Fatal("lost scoped window failure acknowledgment", batch)
			}
			p.mu.Lock()
			position := p.latestCheckpoints["test"]
			p.mu.Unlock()
			if position == nil || position.Value != 12 {
				t.Fatal("noncommit delivery fabricated source progress", position)
			}
		})
	}
}
