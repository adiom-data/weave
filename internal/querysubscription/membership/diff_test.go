package membership

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func testKey(value string) storage.EntityKey {
	return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: value}
}

func TestMembershipDeltaRoundTrip(t *testing.T) {
	a, b, c, d := testKey("a"), testKey("b"), testKey("c"), testKey("d")
	tests := []struct {
		name   string
		before []storage.EntityKey
		after  []storage.EntityKey
	}{
		{name: "insert", before: []storage.EntityKey{a, c}, after: []storage.EntityKey{a, b, c}},
		{name: "remove", before: []storage.EntityKey{a, b, c}, after: []storage.EntityKey{a, c}},
		{name: "move", before: []storage.EntityKey{a, b, c}, after: []storage.EntityKey{c, a, b}},
		{name: "replace", before: []storage.EntityKey{a, b}, after: []storage.EntityKey{c, d}},
		{name: "unchanged", before: []storage.EntityKey{a, b}, after: []storage.EntityKey{a, b}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			delta := Diff(test.before, test.after)
			actual, err := querysubscription.ApplyMembership(test.before, delta)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, test.after) {
				t.Fatalf("querysubscription.ApplyMembership(%v, %+v) = %v, want %v", test.before, delta, actual, test.after)
			}
		})
	}
}
