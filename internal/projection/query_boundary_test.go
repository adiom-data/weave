package projection

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type boundarySession struct {
	mu           sync.Mutex
	subscription *boundarySubscription
	entity       bool
	events       chan querysubscription.Event
}

func (*boundarySession) RequestProgress(context.Context) error {
	return nil
}

func (s *boundarySession) SubscribeQuery(_ context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	s.mu.Lock()
	if s.subscription != nil {
		s.mu.Unlock()
		return errors.New("unexpected second subscription")
	}
	if s.events == nil {
		s.events = make(chan querysubscription.Event, 1)
	}
	s.subscription = &boundarySubscription{id: request.ID}
	subscription := s.subscription
	s.mu.Unlock()
	return install(subscription, querysubscription.QuerySnapshot{
		Bounds: querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}},
	})
}

func (s *boundarySession) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	s.mu.Lock()
	s.entity = true
	s.mu.Unlock()
	return s.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{ID: request.ID, Query: request.Query, Window: querysubscription.InitialWindow{Count: 1}}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		return install(boundaryEntitySubscription{Subscription: subscription}, querysubscription.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision})
	})
}

func TestProjectionEntityNodeUsesEntitySubscriptionContract(t *testing.T) {
	session := &boundarySession{events: make(chan querysubscription.Event, 1)}
	projection, _, err := startTestProjection(context.Background(), session, &ProjectionNode{
		Name: "item", Entity: true,
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, nil
		},
	}, StartOptions{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	session.mu.Lock()
	entity := session.entity
	session.mu.Unlock()
	if !entity {
		t.Fatal("entity projection node used the windowed query interface")
	}
}

func TestEntityResetKeepsReplacementRowAndDoesNotExposeQueryMembership(t *testing.T) {
	oldKey := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "old"}
	newKey := storage.EntityKey{Relation: oldKey.Relation, Value: "new"}
	instance := &projectionInstance{
		id:      "entity",
		node:    &ProjectionNode{Name: "item", Entity: true},
		members: map[storage.EntityKey]*projectionMember{oldKey: {record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")}}}},
		order:   []storage.EntityKey{oldKey},
	}
	projection := &Projection{
		instances: map[string]*projectionInstance{instance.id: instance},
	}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}}
	result, _, err := projection.processBatch(instance, querysubscription.DeliveryMetadata{}, querysubscription.Batch{
		Clear: true,
		Changes: []querysubscription.Change{{
			Kind: querysubscription.Added,
			Row:  querysubscription.Row{Key: newKey, Record: record},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.order) != 1 || instance.order[0] != newKey {
		t.Fatalf("entity order after reset = %v, want [%v]", instance.order, newKey)
	}
	if _, exists := instance.members[oldKey]; exists {
		t.Fatal("reset retained the old entity")
	}
	if _, exists := instance.members[newKey]; !exists {
		t.Fatal("reset discarded the replacement entity")
	}
	if result.Membership != nil {
		t.Fatalf("entity reset exposed query membership: %+v", result.Membership)
	}
}

type boundaryEntitySubscription struct{ querysubscription.Subscription }

func (s *boundarySession) Next(ctx context.Context) (querysubscription.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-ctx.Done():
		return querysubscription.Event{}, ctx.Err()
	}
}

func (*boundarySession) Close(context.Context) error { return nil }

type boundarySubscription struct {
	id string
}

func (s *boundarySubscription) ID() string { return s.id }
func (*boundarySubscription) UpdateWindow(context.Context, []querysubscription.BoundaryChange, string) error {
	return nil
}
func (*boundarySubscription) Close(context.Context) error { return nil }

func TestProjectionUsesOnlyQuerySessionContract(t *testing.T) {
	session := &boundarySession{events: make(chan querysubscription.Event, 1)}
	projection, initial, err := startTestProjection(context.Background(), session, &ProjectionNode{
		Name: "items",
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, nil
		},
	}, StartOptions{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	if len(initial) != 1 || initial[0].Type != ProjectionInstanceOpened {
		t.Fatalf("initial projection = %+v", initial)
	}
	session.mu.Lock()
	subscribed := session.subscription != nil
	session.mu.Unlock()
	if !subscribed {
		t.Fatal("projection did not open its query subscription")
	}
}
