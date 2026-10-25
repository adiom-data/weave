package projection

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func outputTestProjection() (*Projection, storage.EntityKey) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "one"}
	instance := &projectionInstance{
		id: "projection:child", node: &ProjectionNode{Name: "tasks"},
		members: map[storage.EntityKey]*projectionMember{key: {record: outputTestRecord("snapshot")}},
		order:   []storage.EntityKey{key}, revision: 1,
	}
	return &Projection{ctx: context.Background(), prefix: "projection", instances: map[string]*projectionInstance{instance.id: instance}, outputReleased: true}, key
}

func outputTestRecord(title string) storage.Record {
	return storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
}

func outputTestPublication(revision uint64) ProjectionBatch {
	return ProjectionBatch{publication: &projectionPublication{InstanceIDs: []string{"projection:child"}, CausalRevision: revision}}
}

func collectTestOutput(t *testing.T, scheduler *OutputScheduler[string], revision uint64) []Output[string] {
	t.Helper()
	var ready []Output[string]
	if err := scheduler.Advance(revision, func(output Output[string]) error {
		ready = append(ready, output)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ready
}

func TestOutputSchedulerWaitsForParentAndCapturesLatestImage(t *testing.T) {
	p, key := outputTestProjection()
	var scheduler OutputScheduler[string]
	if output := scheduler.Submit(p, "projection", outputTestPublication(7)); output != nil {
		t.Fatal("subtree published before its parent transaction")
	}
	if output := collectTestOutput(t, &scheduler, 6); len(output) != 0 {
		t.Fatal("subtree published before causal revision")
	}
	// A newer image arrives after the publication was queued, but before its
	// causal transaction is consumed. It must replace the old snapshot.
	metadata := querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: 8}
	batch, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{Delivery: metadata,
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: "projection:child", Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: outputTestRecord("latest")}}},
		}}},
	})
	if err != nil || !outgoing || len(batch.Events) != 0 {
		t.Fatalf("unpublished update escaped: batch=%+v outgoing=%v err=%v", batch, outgoing, err)
	}
	ready := collectTestOutput(t, &scheduler, 7)
	if len(ready) != 1 || ready[0].Owner != "projection" || ready[0].Err != nil {
		t.Fatalf("ready output = %+v", ready)
	}
	image := ready[0].Batch
	if image.publication != nil || len(image.Events) != 2 || string(image.Events[1].Record["title"].Data) != "latest" || image.Events[1].Version.Revision != 8 {
		t.Fatalf("publication did not capture current image: %+v", image)
	}
	metadata.Revision = 9
	live, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{Delivery: metadata,
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: "projection:child", Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: outputTestRecord("live")}}},
		}}},
	})
	if err != nil || !outgoing || len(live.Events) != 1 {
		t.Fatalf("published subtree did not become live: %+v %v", live, err)
	}
}

func TestOutputSchedulerPreservesArrivalOrderAndDropsExactLifetime(t *testing.T) {
	old, _ := outputTestProjection()
	current, _ := outputTestProjection()
	other, _ := outputTestProjection()
	var scheduler OutputScheduler[string]
	scheduler.Submit(old, "same-id", outputTestPublication(2))
	scheduler.Submit(current, "same-id", outputTestPublication(2))
	scheduler.Submit(other, "other", outputTestPublication(2))
	scheduler.Drop(old)
	ready := collectTestOutput(t, &scheduler, 2)
	if len(ready) != 2 || ready[0].Owner != "same-id" || ready[1].Owner != "other" || old.instances["projection:child"].published {
		t.Fatalf("closed lifetime affected output: %+v", ready)
	}
	if len(scheduler.pending) != 0 {
		t.Fatal("scheduler retained consumed output")
	}
	// Backing storage must not keep projection graphs alive after draining.
	for _, item := range scheduler.pending[:cap(scheduler.pending)] {
		if item.projection != nil || item.batch.publication != nil {
			t.Fatal("drained backing array retained a projection")
		}
	}
}

func TestOutputSchedulerImmediateAndStagedOutputUseSamePath(t *testing.T) {
	p, _ := outputTestProjection()
	p.outputReleased = false
	p.ready = make(chan struct{})
	close(p.ready)
	metadata := querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: 7, Atomic: true}
	p.pendingOutput = []ProjectionBatch{outputTestPublication(7), {Delivery: metadata}}
	var scheduler OutputScheduler[string]
	var consumed []ProjectionBatch
	if err := scheduler.Release(p, "projection", func(output Output[string]) error {
		if output.Err != nil {
			return output.Err
		}
		consumed = append(consumed, output.Batch)
		return scheduler.Advance(output.Batch.Delivery.Revision, func(ready Output[string]) error {
			if ready.Err != nil {
				return ready.Err
			}
			consumed = append(consumed, ready.Batch)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if !p.outputReleased || len(consumed) != 2 || consumed[0].Delivery != metadata || len(consumed[1].Events) != 2 {
		t.Fatalf("initial handoff lost order or transaction metadata: %+v", consumed)
	}
	// Once progress is sufficient, submission resolves immediately. A repeated
	// request cannot open an already-published subtree again.
	output := scheduler.Submit(p, "projection", outputTestPublication(7))
	if output == nil || output.Err != nil || len(output.Batch.Events) != 0 || output.Batch.publication != nil {
		t.Fatalf("duplicate publication = %+v", output)
	}
}

func TestOutputSchedulerCompletionFailureDoesNotDiscardOtherOwners(t *testing.T) {
	failed, _ := outputTestProjection()
	failed.instances["projection:child"].node.Children = []*ProjectionNode{{Name: "grandchild"}}
	// A zero-capacity expansion backlog makes completion fail without I/O.
	failed.expansions = &expansionScheduler{queued: make(map[expansionKey]projectionExpansion)}
	healthy, _ := outputTestProjection()
	var scheduler OutputScheduler[string]
	scheduler.Submit(failed, "failed", outputTestPublication(2))
	scheduler.Submit(healthy, "healthy", outputTestPublication(2))
	ready := collectTestOutput(t, &scheduler, 2)
	if len(ready) != 2 || ready[0].Owner != "failed" || !errors.Is(ready[0].Err, errExpansionBacklogFull) || ready[1].Owner != "healthy" || ready[1].Err != nil || len(ready[1].Batch.Events) != 2 {
		t.Fatalf("completion failure escaped its owner: %+v", ready)
	}
}

func TestOutputSchedulerConsumesEachImageBeforeCompletingTheNext(t *testing.T) {
	var scheduler OutputScheduler[int]
	projections := make([]*Projection, 3)
	for index := range projections {
		projections[index], _ = outputTestProjection()
		scheduler.Submit(projections[index], index, outputTestPublication(2))
	}
	consumed := 0
	if err := scheduler.Advance(2, func(output Output[int]) error {
		if output.Err != nil {
			return output.Err
		}
		if output.Owner != consumed || len(output.Batch.Events) != 2 {
			t.Fatalf("unexpected output: %+v", output)
		}
		// Completing an image marks its instances published. No subsequent
		// image may have been captured while this one is being consumed.
		for _, p := range projections[consumed+1:] {
			if p.instances["projection:child"].published {
				t.Fatal("later image was completed before current output was consumed")
			}
		}
		consumed++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if consumed != len(projections) {
		t.Fatalf("consumed %d outputs, want %d", consumed, len(projections))
	}
}

func TestOutputSchedulerConsumerErrorLeavesUnvisitedImagesUnmaterialized(t *testing.T) {
	first, _ := outputTestProjection()
	next, _ := outputTestProjection()
	var scheduler OutputScheduler[string]
	scheduler.Submit(first, "first", outputTestPublication(2))
	scheduler.Submit(next, "next", outputTestPublication(2))
	failure := errors.New("output failed")
	if err := scheduler.Advance(2, func(output Output[string]) error {
		if output.Owner != "first" {
			t.Fatalf("unexpected output: %+v", output)
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatalf("consumer error = %v", err)
	}
	if next.instances["projection:child"].published || len(scheduler.pending) != 1 || scheduler.pending[0].projection != next {
		t.Fatal("consumer error completed or discarded unvisited work")
	}
	ready := collectTestOutput(t, &scheduler, 2)
	if len(ready) != 1 || ready[0].Owner != "next" || ready[0].Err != nil {
		t.Fatalf("unvisited work could not resume: %+v", ready)
	}
}

func TestOutputSchedulerDropDuringConsumptionDoesNotCompleteRetiredImages(t *testing.T) {
	retired, _ := outputTestProjection()
	later, _ := outputTestProjection()
	laterInstance := later.instances["projection:child"]
	laterInstance.id = "projection:later"
	retired.instances[laterInstance.id] = laterInstance
	healthy, _ := outputTestProjection()
	future, _ := outputTestProjection()
	var scheduler OutputScheduler[string]
	scheduler.Submit(future, "future", outputTestPublication(9))
	scheduler.Submit(retired, "retired", outputTestPublication(2))
	scheduler.Submit(retired, "retired", ProjectionBatch{publication: &projectionPublication{
		InstanceIDs: []string{laterInstance.id}, CausalRevision: 2,
	}})
	scheduler.Submit(healthy, "healthy", outputTestPublication(2))
	var owners []string
	if err := scheduler.Advance(2, func(output Output[string]) error {
		if output.Err != nil {
			return output.Err
		}
		owners = append(owners, output.Owner)
		if output.Owner == "retired" {
			scheduler.Drop(retired)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(owners) != 2 || owners[0] != "retired" || owners[1] != "healthy" || laterInstance.published {
		t.Fatalf("retired image completed or healthy image skipped: owners=%v published=%v", owners, laterInstance.published)
	}
	if len(scheduler.pending) != 1 || scheduler.pending[0].projection != future {
		t.Fatal("drop or compaction discarded ineligible work")
	}
	for _, item := range scheduler.pending[len(scheduler.pending):cap(scheduler.pending)] {
		if item.projection != nil || item.batch.publication != nil {
			t.Fatal("compaction retained consumed projection state")
		}
	}
	ready := collectTestOutput(t, &scheduler, 9)
	if len(ready) != 1 || ready[0].Owner != "future" || ready[0].Err != nil {
		t.Fatalf("future image lost: %+v", ready)
	}
}

func TestOutputSchedulerRejectsInvalidDrainWithoutAdvancingProgress(t *testing.T) {
	var scheduler OutputScheduler[string]
	if err := scheduler.Advance(9, nil); err == nil || scheduler.emittedRevision != 0 {
		t.Fatalf("nil consumer advanced progress: err=%v revision=%d", err, scheduler.emittedRevision)
	}
	p, _ := outputTestProjection()
	scheduler.Submit(p, "projection", outputTestPublication(2))
	if err := scheduler.Advance(2, func(Output[string]) error {
		if err := scheduler.Advance(9, func(Output[string]) error { return nil }); err == nil || scheduler.emittedRevision != 2 {
			t.Fatalf("reentrant drain advanced progress: err=%v revision=%d", err, scheduler.emittedRevision)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
