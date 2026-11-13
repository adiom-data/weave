package projection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type partialExpansionFeed struct {
	*childBindingFeed
	installed chan string
	blocked   chan struct{}
	resume    chan struct{}
	nested    bool
}

type reconcilingExpansionFeed struct {
	*childBindingFeed
	loading, resume, installing chan struct{}
}

func (f *reconcilingExpansionFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if request.Query.Relation.Name == "old" {
		close(f.loading)
		select {
		case <-f.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
		return f.childBindingFeed.SubscribeQuery(ctx, request, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
			close(f.installing)
			return install(handle, snapshot)
		})
	}
	return f.childBindingFeed.SubscribeQuery(ctx, request, install)
}

func TestInstallingChildSeesCompleteParentReconciliation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed := &reconcilingExpansionFeed{childBindingFeed: &childBindingFeed{loads: map[string]int{}}, loading: make(chan struct{}), resume: make(chan struct{}), installing: make(chan struct{})}
	binding, resumeBinding := make(chan struct{}), make(chan struct{})
	var once sync.Once
	child := &ProjectionNode{Name: "child", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		if string(parent["title"].Data) == "changed" {
			once.Do(func() { close(binding) })
			select {
			case <-resumeBinding:
			case <-ctx.Done():
				return storage.QuerySpec{}, true, ctx.Err()
			}
		}
		return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data)}}, true, nil
	}}
	root := &ProjectionNode{Name: "parents", Children: []*ProjectionNode{child}, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
	}}
	p, err := BeginProjection(ctx, feed, root, StartOptions{QueueLimit: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	select {
	case <-feed.loading:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.mu.Lock()
	var id string
	var key storage.EntityKey
	for _, instance := range p.graph.instances {
		if instance.node == root {
			id, key = instance.id, instance.order[0]
		}
	}
	p.mu.Unlock()
	changed := make(chan error, 1)
	go func() {
		record := memberTarget("old")
		record["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("changed")}
		_, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 11, Checkpoint: &q.Position{SourceID: "test", Value: 11}, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: id, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}}}}})
		changed <- err
	}()
	select {
	case <-binding:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The current row has been replaced, but equivalent selector bindings have
	// not yet restored its shared expansion token. Installation must serialize
	// after that complete transition, not reject the temporary token mismatch.
	close(feed.resume)
	select {
	case <-feed.installing:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(resumeBinding)
	if err := <-changed; err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(ctx); err != nil {
		t.Fatalf("unchanged binding rejected an in-flight child: %v", err)
	}
}

func (f *partialExpansionFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	name := request.Query.Relation.Name
	if name == "new_two" {
		close(f.blocked)
		select {
		case <-f.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := f.childBindingFeed.SubscribeQuery(ctx, request, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
		if name == "new_one" {
			snapshot.Rows[0].Record["target"] = memberTarget("new")["target"]
		}
		return install(handle, snapshot)
	})
	if err == nil && (name == "new_one" && !f.nested || name == "new_leaf" && f.nested) {
		f.installed <- request.ID
	}
	return err
}

func TestInstalledInterestFailureFollowsOpeningAncestors(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, action := range []string{"delete", "rebind", "unchanged", "current"} {
			t.Run(map[bool]string{false: "child/", true: "descendant/"}[nested]+action, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				feed := &partialExpansionFeed{childBindingFeed: &childBindingFeed{loads: map[string]int{}}, installed: make(chan string, 1), blocked: make(chan struct{}), resume: make(chan struct{}), nested: nested}
				root := &ProjectionNode{Name: "parents", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
					return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
				}}
				for _, suffix := range []string{"one", "two"} {
					child := &ProjectionNode{Name: suffix, BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
						return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data) + "_" + suffix}}, true, nil
					}}
					if nested && suffix == "one" {
						child.Children = []*ProjectionNode{{Name: "leaf", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
							return storage.QuerySpec{Relation: storage.RelationName{Name: string(parent["target"].Data) + "_leaf"}}, true, nil
						}}}
					}
					root.Children = append(root.Children, child)
				}
				p, _, err := startTestProjection(ctx, feed, root, StartOptions{QueueLimit: 32})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				key := p.graph.instances[p.graph.rootID].order[0]
				updateBoundParent(t, p, key, "new", 11, false)
				select {
				case <-feed.blocked:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				id := <-feed.installed
				switch action {
				case "delete":
					updateBoundParent(t, p, key, "new", 12, true)
				case "rebind":
					updateBoundParent(t, p, key, "old", 12, false)
				case "unchanged":
					updateBoundParent(t, p, key, "new", 12, false)
				}
				failure := errors.New("installed child failed while sibling loads")
				_, _, err = p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: failure}})
				if action == "current" || action == "unchanged" {
					if !errors.Is(err, failure) || !errors.Is(p.Err(), failure) {
						t.Fatalf("current failure was ignored: %v %v", err, p.Err())
					}
				} else {
					if err != nil || p.Err() != nil {
						t.Fatalf("obsolete installed interest terminated projection: %v %v", err, p.Err())
					}
					close(feed.resume)
					if err := p.expansions.waitIdle(ctx); err != nil {
						t.Fatal(err)
					}
					p.mu.Lock()
					for _, instance := range p.graph.instances {
						if instance.opening != nil && !p.graph.instanceCurrentLocked(instance) {
							t.Errorf("obsolete interest remained after load completion: %s", instance.id)
						}
					}
					p.mu.Unlock()
				}
			})
		}
	}
}
