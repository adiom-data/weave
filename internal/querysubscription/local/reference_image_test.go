package local

import (
	"bytes"
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func referenceTestImage(revision uint64, payload string) q.EntitySnapshot {
	return q.EntitySnapshot{
		Revision:   revision,
		Checkpoint: &q.Position{SourceID: "test", Value: revision},
		Fence:      &q.Position{SourceID: "test", Value: revision},
		Row: &q.Row{Record: storage.Record{"payload": {
			Type: storage.TypeObject, State: storage.ValuePresent,
			Object: map[string]storage.Value{"nested": {Type: storage.TypeBytes, State: storage.ValuePresent, Data: []byte(payload)}},
		}}},
	}
}

func referencePayload(image q.EntitySnapshot) string {
	return string(image.Row.Record["payload"].Object["nested"].Data)
}

func TestOwnedReferenceInitialImageSurvivesLiveUpdates(t *testing.T) {
	r := &ownedReferences{}
	child := &ownedReferenceChild{owner: r, pins: 1}
	child.adoptInitial(referenceTestImage(1, "initial"))
	output := cloneEntityImage(child.initial)
	output.Row.Record["payload"].Object["nested"].Data[0] = '!'
	output.Checkpoint.Value = 99
	output.Fence.Value = 99
	if referencePayload(child.current) != "initial" || child.current.Checkpoint.Value != 1 || child.current.Fence.Value != 1 {
		t.Fatal("initial publication mutated the shared private image")
	}
	update := referenceTestImage(2, "updated")
	child.advance(q.DeliveryMetadata{Revision: 2, Checkpoint: update.Checkpoint, Fence: update.Fence}, q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: *update.Row}}})
	if referencePayload(child.initial) != "initial" || referencePayload(child.current) != "updated" {
		t.Fatal("live update overwrote the pinned initial image")
	}
	// Delivered values may be mutated by the recipient; retained images must
	// remain private, including recursively nested values and position pointers.
	update.Row.Record["payload"].Object["nested"].Data[0] = '!'
	output = cloneEntityImage(child.initial)
	output.Row.Record["payload"].Object["nested"].Data[0] = '!'
	output.Checkpoint.Value = 99
	output.Fence.Value = 99
	if referencePayload(child.initial) != "initial" || child.initial.Checkpoint.Value != 1 || child.initial.Fence.Value != 1 || referencePayload(child.current) != "updated" {
		t.Fatal("mutable delivery escaped into retained images")
	}
	child.advance(q.DeliveryMetadata{Revision: 3}, q.Batch{Clear: true})
	if child.current.Row != nil || referencePayload(child.initial) != "initial" {
		t.Fatal("clear changed the pinned initial image")
	}
}

func TestOwnedReferenceOlderInitialDoesNotReplaceCurrent(t *testing.T) {
	child := &ownedReferenceChild{pins: 1, current: referenceTestImage(5, "current")}
	child.adoptInitial(referenceTestImage(3, "initial"))
	if child.current.Revision != 5 || referencePayload(child.current) != "current" || child.initial.Revision != 3 {
		t.Fatal("older initialization overwrote newer live state")
	}
}

func TestOwnedReferenceUnpinnedImageOnlyUpdatesCurrent(t *testing.T) {
	child := &ownedReferenceChild{}
	child.adoptInitial(referenceTestImage(3, "current"))
	if child.initial.Row != nil || child.current.Revision != 3 || referencePayload(child.current) != "current" {
		t.Fatal("unpinned installation retained an initial image or lost current state")
	}
}

func BenchmarkOwnedReferenceInitialImages500(b *testing.B) {
	template := referenceTestImage(1, string(bytes.Repeat([]byte("x"), 256)))
	b.ReportAllocs()
	for b.Loop() {
		for range 500 {
			// A detached subscription image is handed to the owner; the
			// public result must still be independently mutable.
			child := ownedReferenceChild{pins: 1}
			child.adoptInitial(cloneEntityImage(template))
			_ = cloneEntityImage(child.initial)
		}
	}
}
