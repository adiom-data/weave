package querysubscription

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func testKey(value string) storage.EntityKey {
	return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: value}
}

func TestApplyMembershipRejectsInvalidDelta(t *testing.T) {
	a, b, c := testKey("a"), testKey("b"), testKey("c")
	if _, err := ApplyMembership([]storage.EntityKey{b}, MembershipDelta{Insertions: []Insertion{{Key: a, FinalIndex: 2}}}); err == nil {
		t.Fatal("out-of-bounds insertion was accepted")
	}
	if _, err := ApplyMembership([]storage.EntityKey{b}, MembershipDelta{Removed: []storage.EntityKey{a}}); err == nil {
		t.Fatal("absent removal was accepted")
	}
	if _, err := ApplyMembership([]storage.EntityKey{a}, MembershipDelta{Insertions: []Insertion{{Key: b, FinalIndex: 1}, {Key: c, FinalIndex: 0}}}); err == nil {
		t.Fatal("out-of-order insertions were accepted")
	}
	if _, err := ApplyMembership([]storage.EntityKey{a}, MembershipDelta{Insertions: []Insertion{{Key: b, FinalIndex: 0}, {Key: b, FinalIndex: 1}}}); err == nil {
		t.Fatal("duplicate insertion was accepted")
	}
}

func TestDeliveryMetadataValidation(t *testing.T) {
	valid := DeliveryMetadata{Kind: DeliveryCommit, Checkpoint: &Position{SourceID: "test", Value: 2}, Revision: 2, PreviousRevision: 1}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid metadata: %v", err)
	}
	invalid := []DeliveryMetadata{
		{Revision: 1},
		{Kind: DeliveryCommit},
		{Kind: DeliveryCommit, Checkpoint: &Position{SourceID: "test", Value: 2}, Revision: 2, PreviousRevision: 2},
	}
	for _, metadata := range invalid {
		if err := metadata.Validate(); err == nil {
			t.Fatalf("invalid metadata was accepted: %+v", metadata)
		}
	}
}

func TestApplyMembershipMergesLargeInsertionBatch(t *testing.T) {
	const existing = 1000
	before := make([]storage.EntityKey, existing)
	after := make([]storage.EntityKey, 0, existing*2)
	insertions := make([]Insertion, 0, existing)
	for index := 0; index < existing; index++ {
		old := testKey("old-" + strconv.Itoa(index))
		added := testKey("new-" + strconv.Itoa(index))
		before[index] = old
		after = append(after, added, old)
		insertions = append(insertions, Insertion{Key: added, FinalIndex: index * 2})
	}
	actual, err := ApplyMembership(before, MembershipDelta{Insertions: insertions})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, after) {
		t.Fatal("large insertion batch produced the wrong order")
	}
}
