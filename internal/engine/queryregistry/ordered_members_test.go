package queryregistry

import (
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func TestOrderedMembersBoundedIterationAndReorder(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "ordered", relation: relation}
	members := newOrderedMembers(plan)
	key := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }
	for _, value := range []string{"d", "a", "c", "b", "e"} {
		if _, err := members.upsert(key(value), datastore.EncodedSortKey(value)); err != nil {
			t.Fatal(err)
		}
	}
	bounded, err := members.within([]datastore.Bounds{{Left: orderedCursor("b"), Right: orderedCursor("d")}})
	if err != nil {
		t.Fatal(err)
	}
	assertEntityValues(t, bounded, "c", "d")

	if added, err := members.upsert(key("c"), datastore.EncodedSortKey("z")); err != nil || added {
		t.Fatalf("reorder = added %v, err %v", added, err)
	}
	assertEntityValues(t, members.all(), "a", "b", "d", "e", "c")
	reverse, err := members.sortKeysWithin(datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("z")}, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(reverse) != 3 || string(reverse[0].(datastore.EncodedSortKey)) != "z" || string(reverse[1].(datastore.EncodedSortKey)) != "e" || string(reverse[2].(datastore.EncodedSortKey)) != "d" {
		t.Fatalf("reverse keys = %v, want [z e d]", reverse)
	}
}

func TestOrderedMembersUnionDoesNotDuplicateOverlaps(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	members := newOrderedMembers(fakePlan{fingerprint: "ordered", relation: relation})
	for _, value := range []string{"a", "b", "c", "d"} {
		entity := storage.EntityKey{Relation: relation, Value: value}
		if _, err := members.upsert(entity, datastore.EncodedSortKey(value)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := members.within([]datastore.Bounds{
		{Left: orderedCursor("a"), Right: orderedCursor("c")},
		{Left: orderedCursor("b"), Right: orderedCursor("d")},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEntityValues(t, keys, "b", "c", "d")
}

func TestOrderedMembersPositionsAreWindowRelativeAfterReorder(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	members := newOrderedMembers(fakePlan{fingerprint: "ordered", relation: relation})
	key := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }
	for _, value := range []string{"d", "a", "e", "b", "c"} {
		if _, err := members.upsert(key(value), datastore.EncodedSortKey(value)); err != nil {
			t.Fatal(err)
		}
	}
	bounds := datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("e")}
	for value, expected := range map[string]int{"b": 0, "c": 1, "d": 2, "e": 3} {
		position, present, err := members.position(bounds, key(value))
		if err != nil || !present || position != expected {
			t.Fatalf("position(%q) = %d, %v, %v; want %d, true, nil", value, position, present, err, expected)
		}
	}
	if _, present, err := members.position(bounds, key("a")); err != nil || present {
		t.Fatalf("left boundary position = _, %v, %v; want absent", present, err)
	}
	if _, err := members.upsert(key("d"), datastore.EncodedSortKey("bb")); err != nil {
		t.Fatal(err)
	}
	position, present, err := members.position(bounds, key("d"))
	if err != nil || !present || position != 1 {
		t.Fatalf("reordered position = %d, %v, %v; want 1, true, nil", position, present, err)
	}
	if removed, err := members.remove(key("b")); err != nil || !removed {
		t.Fatalf("remove = %v, %v", removed, err)
	}
	position, present, err = members.position(bounds, key("e"))
	if err != nil || !present || position != 2 {
		t.Fatalf("position after removal = %d, %v, %v; want 2, true, nil", position, present, err)
	}
}

func TestOrderedMembersDistinguishesEntitiesWithEqualSortKeys(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	members := newOrderedMembers(fakePlan{fingerprint: "ordered", relation: relation})
	entity := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }

	for _, member := range []struct {
		entity string
		sort   string
	}{{"b", "same"}, {"later", "z"}, {"a", "same"}} {
		if added, err := members.upsert(entity(member.entity), datastore.EncodedSortKey(member.sort)); err != nil || !added {
			t.Fatalf("upsert(%q) = %v, %v; want true, nil", member.entity, added, err)
		}
	}
	assertEntityValues(t, members.all(), "a", "b", "later")

	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	for value, expected := range map[string]int{"a": 0, "b": 1, "later": 2} {
		position, present, err := members.position(all, entity(value))
		if err != nil || !present || position != expected {
			t.Fatalf("position(%q) = %d, %v, %v; want %d, true, nil", value, position, present, err, expected)
		}
	}

	throughTie := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: orderedCursor("same")}
	within, err := members.within([]datastore.Bounds{throughTie})
	if err != nil {
		t.Fatal(err)
	}
	assertEntityValues(t, within, "a", "b")

	if removed, err := members.remove(entity("a")); err != nil || !removed {
		t.Fatalf("remove tied member = %v, %v; want true, nil", removed, err)
	}
	assertEntityValues(t, members.all(), "b", "later")
}

func orderedCursor(value string) datastore.Boundary {
	return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
}

func assertEntityValues(t *testing.T, actual []storage.EntityKey, expected ...string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("entities = %v, want %v", actual, expected)
	}
	for index := range expected {
		if actual[index].Value != expected[index] {
			t.Fatalf("entities[%d] = %q, want %q", index, actual[index].Value, expected[index])
		}
	}
}
