package local

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type exactEntityTestPlan struct {
	nonComparingPlan
	key storage.EntityKey
}

func (p exactEntityTestPlan) LookupKey() storage.EntityKey { return p.key }

func TestEntitySelectionSpecializesOnlyProvenIdentities(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	base := nonComparingPlan{relation: key.Relation}
	initial := []entitycache.EntitySnapshot{{Key: key, State: entitycache.EntityCurrent}}
	identity, ok := newEntitySelection(exactEntityTestPlan{base, key}, initial).(*identitySelector)
	if !ok || !identity.present || identity.key != key {
		t.Fatal("proven identity did not use a payload-free selector")
	}
	if _, ok := newEntitySelection(base, initial).(*entitySelector); !ok {
		t.Fatal("unproven selector lost its candidate tracking")
	}
	if _, err := identity.apply(delivery.SubscriptionBatch{Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: storage.EntityKey{Relation: key.Relation, Value: "other"}}}}); err == nil {
		t.Fatal("foreign entity was accepted by an exact selector")
	}
}

func BenchmarkEntitySelectionInitialization(b *testing.B) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	base := nonComparingPlan{relation: key.Relation}
	initial := []entitycache.EntitySnapshot{{Key: key, State: entitycache.EntityCurrent, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: bytes.Repeat([]byte("x"), 256)}}}}
	for _, test := range []struct {
		name string
		plan datastore.PreparedQuery
	}{{"candidates", base}, {"identity", exactEntityTestPlan{base, key}}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = newEntitySelection(test.plan, initial)
			}
		})
	}
}

func TestIdentitySelectionMatchesGeneralSelectionForTransactionSequences(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "one"}
	for _, present := range []bool{false, true} {
		for _, clear := range []bool{false, true} {
			for length := 0; length <= 3; length++ {
				combinations := 1
				for range length {
					combinations *= 3
				}
				for sequence := range combinations {
					for _, zeroIndices := range []bool{false, true} {
						general := &entitySelector{rows: map[storage.EntityKey]querysubscription.Row{}}
						if present {
							general.order = []storage.EntityKey{key}
							general.rows[key] = querysubscription.Row{Key: key, Record: storage.Record{"id": {Data: []byte("initial")}}}
						}
						identity := &identitySelector{key: key, present: present}
						native := delivery.SubscriptionBatch{Clear: clear}
						after := present && !clear
						encoded := sequence
						for index := range length {
							kind := []changeprocessor.QueryChangeKind{changeprocessor.QueryAdded, changeprocessor.QueryUpdated, changeprocessor.QueryRemoved}[encoded%3]
							encoded /= 3
							event := delivery.SubscriptionEvent{Kind: kind, Key: key, ChangeIndex: uint32(index + 1), Pending: index%2 == 1}
							if zeroIndices {
								event.ChangeIndex = 0
							}
							after = kind != changeprocessor.QueryRemoved
							if after {
								event.Record = storage.Record{"id": {Data: []byte{byte('a' + index)}}}
							}
							native.Events = append(native.Events, event)
						}
						if present && !clear && !after {
							native.Membership.Removed = []storage.EntityKey{key}
						}
						if after && (!present || clear) {
							native.Membership.Insertions = []delivery.MembershipInsertion{{Key: key, FinalIndex: 0}}
						}
						want, wantErr := general.apply(native)
						got, err := identity.apply(native)
						if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
							t.Fatalf("present=%v clear=%v length=%d sequence=%d zeroIndices=%v: got=%+v err=%v want=%+v err=%v", present, clear, length, sequence, zeroIndices, got, err, want, wantErr)
						}
						for _, change := range got.Changes {
							if len(change.Record["id"].Data) > 0 {
								change.Record["id"].Data[0] = '!'
							}
						}
						for _, event := range native.Events {
							if string(event.Record["id"].Data) == "!" {
								t.Fatal("identity output aliases a source event")
							}
						}
					}
				}
			}
		}
	}
}

func TestEntityBatchHidesQueryWindowDetails(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	key := storage.EntityKey{Relation: relation, Value: "one"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}
	selector := &entitySelector{order: []storage.EntityKey{key}, rows: map[storage.EntityKey]querysubscription.Row{key: {Key: key, Record: record}}}
	batch, err := selector.apply(delivery.SubscriptionBatch{
		Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated, Key: key, Record: record}},
		Window: &delivery.SubscriptionWindowTransition{Token: "hidden"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Changes) != 1 || len(batch.Changes[0].Cursor) != 0 || len(batch.Membership.Removed) != 0 || batch.Window != nil {
		t.Fatalf("entity batch leaked query details: %+v", batch)
	}
}

func TestEntityBatchKeepsFirstOfMultipleMatchingRows(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	one := storage.EntityKey{Relation: relation, Value: "one"}
	two := storage.EntityKey{Relation: relation, Value: "two"}
	zero := storage.EntityKey{Relation: relation, Value: "zero"}
	oneRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}
	twoRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("two")}}
	zeroRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("zero")}}
	selector := &entitySelector{order: []storage.EntityKey{one}, rows: map[storage.EntityKey]querysubscription.Row{one: {Key: one, Record: oneRecord}}}
	unchanged, err := selector.apply(delivery.SubscriptionBatch{
		Events:     []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: two, Record: twoRecord, ChangeIndex: 1}},
		Membership: delivery.MembershipDelta{Insertions: []delivery.MembershipInsertion{{Key: two, FinalIndex: 1}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(unchanged.Changes) != 0 {
		t.Fatalf("later candidate changed selected entity: %+v", unchanged)
	}
	switched, err := selector.apply(delivery.SubscriptionBatch{
		Events:     []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: zero, Record: zeroRecord, ChangeIndex: 2}},
		Membership: delivery.MembershipDelta{Insertions: []delivery.MembershipInsertion{{Key: zero, FinalIndex: 0}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(switched.Changes) != 2 || switched.Changes[0].Kind != querysubscription.Removed || switched.Changes[0].Key != one || switched.Changes[1].Kind != querysubscription.Added || switched.Changes[1].Key != zero {
		t.Fatalf("selected entity switch = %+v", switched)
	}
	promoted, err := selector.apply(delivery.SubscriptionBatch{
		Events:     []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryRemoved, Key: zero, ChangeIndex: 3}},
		Membership: delivery.MembershipDelta{Removed: []storage.EntityKey{zero}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(promoted.Changes) != 2 || promoted.Changes[0].Kind != querysubscription.Removed || promoted.Changes[0].Key != zero || promoted.Changes[1].Kind != querysubscription.Added || promoted.Changes[1].Key != one {
		t.Fatalf("candidate promotion = %+v", promoted)
	}
}
