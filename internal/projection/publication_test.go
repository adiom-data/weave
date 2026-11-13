package projection

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestCurrentEventsCaptureOwnsRecordsAndCursors(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	record := storage.Record{"properties": {Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{"labels": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("label")}}}}}}
	before := storage.CloneRecord(record)
	instance := &projectionInstance{id: "items", node: &ProjectionNode{Name: "items"}, members: map[storage.EntityKey]*projectionMember{key: {record: record, version: querysubscription.RecordVersion{Revision: 1}, cursor: []byte("cursor")}}, order: []storage.EntityKey{key}}
	p := &Projection{graph: projectionGraph{instances: map[string]*projectionInstance{"items": instance}}}
	first, err := p.currentEvents("items")
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.currentEvents("items")
	if err != nil {
		t.Fatal(err)
	}
	first[1].Record["properties"].Object["labels"].List[0].Data[0] = 'X'
	first[1].Cursor[0] = 'X'
	if !reflect.DeepEqual(instance.members[key].record, before) || string(instance.members[key].cursor) != "cursor" {
		t.Fatal("captured event aliases projection state")
	}
	if !reflect.DeepEqual(second[1].Record, before) || string(second[1].Cursor) != "cursor" {
		t.Fatal("independent captures share mutable payloads")
	}
}

type publicationFeed struct {
	boundarySession
	childStarted chan struct{}
	childRelease chan struct{}
}

func (f *publicationFeed) SubscribeQuery(ctx context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	snapshot := querysubscription.QuerySnapshot{Revision: 1, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}}
	if request.Query.Relation.Name == "children" {
		close(f.childStarted)
		select {
		case <-f.childRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "parents"}, Value: "one"}
		snapshot.Rows = []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}, ChangeIndex: 7}}}
	}
	return install(&boundarySubscription{id: request.ID}, snapshot)
}

func TestPublicationCaptureSeparatesRecordVersionAndAuthorizationProgress(t *testing.T) {
	for _, synchronize := range []bool{false, true} {
		name := "first_image"
		if synchronize {
			name = "synchronized_image"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			feed := &publicationFeed{childStarted: make(chan struct{}), childRelease: make(chan struct{})}
			root := &ProjectionNode{Name: "parents", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, true, nil
			}, Children: []*ProjectionNode{{Name: "children", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: "children"}}, true, nil
			}}}}
			p, err := BeginProjection(ctx, feed, root, StartOptions{QueueLimit: 4, WaitForSync: synchronize})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			select {
			case <-feed.childStarted:
			case <-ctx.Done():
				t.Fatal("child load did not start")
			}
			commit := func(position uint64) {
				t.Helper()
				if _, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: position, Checkpoint: &querysubscription.Position{SourceID: "test", Value: position}}}); err != nil || outgoing {
					t.Fatalf("initialization commit: outgoing=%v err=%v", outgoing, err)
				}
			}
			commit(2) // A policy-only transaction while descendants still load.
			close(feed.childRelease)
			image, _, err := p.TakeInitial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range image {
				if event.Checkpoint == nil || event.Checkpoint.Value != 2 {
					t.Fatalf("image lost incorporated authorization progress: %+v", event)
				}
				if event.Type == ProjectionRowChanged && event.Version != (querysubscription.RecordVersion{Revision: 1, ChangeIndex: 7}) {
					t.Fatalf("progress changed the record's version: %+v", event.Version)
				}
			}
			commit(3) // After capture, the clock must instead be staged.
			var staged []ProjectionBatch
			if err := p.releaseOutput(func(batch ProjectionBatch) error { staged = append(staged, batch); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(staged) != 1 || staged[0].Delivery.Revision != 3 || len(staged[0].Events) != 0 {
				t.Fatalf("handoff did not separate pre/post capture commits: %+v", staged)
			}
		})
	}
}

func TestFenceOnlyPublicationPreservesSnapshotAuthorizationUntilCovered(t *testing.T) {
	for _, progress := range []uint64{0, 1, 9, 10, 11} {
		p := &Projection{latestCheckpoints: make(map[storage.SourceID]*querysubscription.Position)}
		if progress != 0 {
			p.latestCheckpoints["test"] = &querysubscription.Position{SourceID: "test", Value: progress}
		}
		instance := &projectionInstance{id: "entity", node: &ProjectionNode{Name: "entity", Entity: true}, fence: &querysubscription.Position{SourceID: "test", Value: 10}}
		p.graph.instances = map[string]*projectionInstance{"entity": instance}
		image, err := p.currentEvents("entity")
		if err != nil {
			t.Fatal(err)
		}
		if len(image) != 1 || image[0].Fence == nil || image[0].Fence.Value != 10 {
			t.Fatalf("progress %d lost snapshot fence: %+v", progress, image)
		}
		if progress < 10 {
			if image[0].Checkpoint != nil {
				t.Fatalf("progress %d selected a checkpoint before fence 10: %+v", progress, image[0].Checkpoint)
			}
		} else if image[0].Checkpoint == nil || image[0].Checkpoint.Value != progress {
			t.Fatalf("covered fence did not select progress %d: %+v", progress, image[0].Checkpoint)
		}
	}
}

type fencePublicationFeed struct{ boundarySession }

func (*fencePublicationFeed) SubscribeEntity(_ context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	return install(boundaryEntitySubscription{Subscription: &boundarySubscription{id: request.ID}}, querysubscription.EntitySnapshot{Fence: &querysubscription.Position{SourceID: "test", Value: 10}})
}

func TestSynchronizedSingleTableImageDoesNotTurnFenceIntoCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := BeginProjection(ctx, &fencePublicationFeed{}, &ProjectionNode{Name: "entity", Entity: true, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) { return storage.QuerySpec{}, true, nil }}, StartOptions{QueueLimit: 4, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	image, positions, err := p.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(image) != 1 || image[0].Checkpoint != nil || image[0].Fence == nil || image[0].Fence.Value != 10 || len(positions) != 0 {
		t.Fatalf("fence-only snapshot fabricated a committed checkpoint: image=%+v positions=%+v", image, positions)
	}
}
