package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestProjectionBoundsOutputBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &boundarySession{}
	p, err := BeginProjection(ctx, feed, &ProjectionNode{
		Name: "item", Entity: true,
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil },
	}, StartOptions{QueueLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	initial, _, err := p.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	apply := func(revision uint64) error {
		_, _, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{
			Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: revision, Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision}},
			Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: initial[0].InstanceID, Batch: querysubscription.Batch{
				Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: storage.Record{}}}},
			}}},
		})
		return err
	}
	if err := apply(1); err != nil {
		t.Fatal(err)
	}
	if len(p.pendingOutput) != 1 {
		t.Fatal("first output was not staged")
	}
	// Subtree publications share the same capacity as transaction batches.
	childID := "unpublished-child"
	p.mu.Lock()
	p.instances[childID] = &projectionInstance{id: childID, node: &ProjectionNode{Name: "child"}}
	p.mu.Unlock()
	if err := p.publishSubtrees([]string{childID}, 1); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("publication overflow = %v", err)
	}
	if len(p.pendingOutput) != 1 {
		t.Fatal("publication exceeded the shared staging bound")
	}
	if err := apply(2); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("transaction overflow = %v", err)
	}
	if !errors.Is(p.Err(), querysubscription.ErrBackpressure) {
		t.Fatalf("terminal failure = %v", p.Err())
	}
	if _, err := p.Next(ctx); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("consumer did not observe overflow: %v", err)
	}
	if err := p.releaseOutput(p.queue.Offer); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("release hid the terminal failure: %v", err)
	}
	if err := p.WaitReady(ctx); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("ready image hid the terminal failure: %v", err)
	}
	if len(p.pendingOutput) != 0 {
		t.Fatal("terminal projection retained staged output")
	}
}

func TestProjectionReleaseDrainsStagedBatchesBeforeEnablingLiveDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := BeginProjection(ctx, &boundarySession{}, &ProjectionNode{
		Name: "item", Entity: true,
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil },
	}, StartOptions{QueueLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	initial, _, err := p.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	apply := func(revision uint64) (ProjectionBatch, bool, error) {
		return p.ApplyDelivery(querysubscription.DeliveryEnvelope{
			Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: revision, Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision}},
			Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: initial[0].InstanceID, Batch: querysubscription.Batch{
				Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: storage.Record{}}}},
			}}},
		})
	}
	if _, outgoing, err := apply(1); err != nil || outgoing {
		t.Fatalf("pre-release update: outgoing=%v, err=%v", outgoing, err)
	}
	var consumed []uint64
	if err := p.releaseOutput(func(batch ProjectionBatch) error {
		consumed = append(consumed, batch.Delivery.Revision)
		if batch.Delivery.Revision == 1 {
			// Feed processing remains usable while the consumer handles the old
			// batch, but newer effects cannot bypass the release drain.
			if _, outgoing, err := apply(2); err != nil || outgoing {
				t.Fatalf("newer update escaped during release: outgoing=%v, err=%v", outgoing, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(consumed) != 2 || consumed[0] != 1 || consumed[1] != 2 {
		t.Fatalf("synchronous release order = %v", consumed)
	}
	if p.queue.Len() != 0 {
		t.Fatal("staged transactions were sent through the asynchronous output path")
	}
	if batch, outgoing, err := apply(3); err != nil || !outgoing || batch.Delivery.Revision != 3 {
		t.Fatalf("post-release update: batch=%+v, outgoing=%v, err=%v", batch, outgoing, err)
	}
	if err := p.releaseOutput(func(ProjectionBatch) error {
		t.Fatal("repeated release replayed staged output")
		return nil
	}); err != nil {
		t.Fatalf("repeated release failed: %v", err)
	}
}
