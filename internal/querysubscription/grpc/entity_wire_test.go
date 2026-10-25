package querygrpc

import (
	"reflect"
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestEntityRoutingWireRoundTrip(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "public", Name: "links"}, Value: "1"}
	for _, target := range []q.EntityTarget{
		{SubscriptionID: "explicit"},
		{SubscriptionID: "parent", Reference: &q.ReferenceRoute{Selector: "bound-selector", Parents: []storage.EntityKey{key}}},
	} {
		installation := q.EntityInstallation{Target: target, Snapshot: q.EntitySnapshot{Revision: 7, Checkpoint: &q.Position{SourceID: "test", Value: 7}}}
		encoded, err := entityInstallationToProto(installation)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := entityInstallationFromProto(encoded)
		if err != nil || !reflect.DeepEqual(installation, decoded) {
			t.Fatalf("installation round trip: %+v, %v", decoded, err)
		}
		effect := q.EntityEffect{Target: target, Delta: q.EntityDelta{Clear: true, ClearChangeIndex: 7}}
		encodedEffect, err := entityEffectToProto(effect)
		if err != nil {
			t.Fatal(err)
		}
		decodedEffect, err := entityEffectFromProto(encodedEffect)
		if err != nil || !reflect.DeepEqual(effect, decodedEffect) {
			t.Fatalf("effect round trip: %+v, %v", decodedEffect, err)
		}
		encoded.Target.SubscriptionId = ""
		if _, err := entityInstallationFromProto(encoded); err == nil {
			t.Fatal("accepted an installation without an owner")
		}
	}
	batch := q.Batch{BindingResets: []storage.EntityKey{key}}
	encoded, err := batchToProto(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := batchFromProto(encoded)
	if err != nil || !reflect.DeepEqual(batch, decoded) {
		t.Fatalf("binding reset round trip: %+v, %v", decoded, err)
	}
}

func TestQueryRowExpansionWireRoundTrip(t *testing.T) {
	key := func(id string) storage.EntityKey {
		return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: id}
	}
	expanded := &q.EntitySnapshot{Row: &q.Row{Key: key("target"), Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("shared")}}}, Revision: 7, Checkpoint: &q.Position{SourceID: "test", Value: 7}}
	snapshot := q.QuerySnapshot{Bounds: allBounds(), OwnedReferences: true, Rows: []q.QueryRow{
		{Row: q.Row{Key: key("a")}, Expanded: expanded},
		{Row: q.Row{Key: key("b")}, Expanded: expanded},
		{Row: q.Row{Key: key("absent")}, Expanded: &q.EntitySnapshot{Revision: 7}},
		{Row: q.Row{Key: key("unresolved"), Pending: true}},
	}}
	encoded, err := snapshotToProto(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := snapshotFromProto(encoded)
	if err != nil || !reflect.DeepEqual(snapshot, decoded) {
		t.Fatalf("row expansion round trip: %+v, %v", decoded, err)
	}
	decoded.Rows[0].Expanded.Row.Record["title"].Data[0] = '!'
	if string(decoded.Rows[1].Expanded.Row.Record["title"].Data) != "shared" {
		t.Fatal("decoded repeated targets alias each other")
	}
	encoded.Rows[0].Row = nil
	if _, err := snapshotFromProto(encoded); err == nil {
		t.Fatal("accepted an expansion without a source row")
	}
}
