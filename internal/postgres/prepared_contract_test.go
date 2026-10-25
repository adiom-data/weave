package postgres

import (
	"bytes"
	"reflect"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

// No database or connection is provided: evaluation of a published plan must
// be self-contained, including identity, cursor, and predicate operations.
func contractPrepared() *postgresPrepared {
	order := []storage.OrderTerm{{Field: "id"}}
	return &postgresPrepared{plan: &plan{
		adapter: newPostgresTypeAdapter(), schema: "app", table: "items", relationID: 42,
		fingerprint: "prepared-contract",
		primary:     []string{"id"}, order: order,
		columns: map[string]column{"id": {name: "id", typeOID: 20, valueType: storage.TypeInteger, supported: true, primary: true}},
		query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: "items"}, Order: append([]storage.OrderTerm(nil), order...),
			Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "id", Operator: storage.ComparisonGreater, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("0")}}},
		},
	}}
}

func contractRecord() storage.Record {
	return storage.Record{"id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("10")}}
}

func TestPreparedQueryOwnsReturnedDefinitionAndStableKeys(t *testing.T) {
	p := contractPrepared()
	definition, fingerprint := p.Query(), p.Fingerprint()
	copy := p.Query()
	copy.Relation.Name = "changed"
	copy.Order[0].Descending = true
	copy.Filter.Comparison.Value.Data[0] = '9'
	if !reflect.DeepEqual(p.Query(), definition) || p.Fingerprint() != fingerprint {
		t.Fatal("returned definition mutated the plan")
	}
	record := contractRecord()
	before := storage.CloneRecord(record)
	if matches, err := p.Matches(record); err != nil || !matches {
		t.Fatalf("matches=%v err=%v", matches, err)
	}
	if _, err := p.EntityKey(record); err != nil {
		t.Fatal(err)
	}
	key, err := p.Key(record)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := p.EncodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("evaluation mutated its input record")
	}
	// Changing the original record must not change an indexed sort key.
	record["id"].Data[0] = '9'
	if _, err := p.Key(record); err != nil {
		t.Fatal(err)
	}
	again, err := p.EncodeKey(key)
	if err != nil || !bytes.Equal(again, cursor) {
		t.Fatalf("key aliases input record: %v", err)
	}
	// Cursor input and output buffers likewise cannot be retained scratch space.
	input := append([]byte(nil), cursor...)
	decoded, err := p.DecodeKey(input)
	if err != nil {
		t.Fatal(err)
	}
	clear(input)
	clear(again)
	reencoded, err := p.EncodeKey(decoded)
	if err != nil || !bytes.Equal(reencoded, cursor) {
		t.Fatalf("decoded key retained mutable cursor input: %v", err)
	}
	if compared, err := p.Compare(key, decoded); err != nil || compared != 0 {
		t.Fatalf("stable key comparison=%d err=%v", compared, err)
	}
}

func TestPreparedQueryConcurrentEvaluationDoesNotMutateSharedInputs(t *testing.T) {
	p := contractPrepared()
	record := contractRecord()
	before := storage.CloneRecord(record)
	definition := p.Query()
	key, err := p.Key(record)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := p.EntityKey(record)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := p.EncodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				if matches, err := p.Matches(record); err != nil || !matches {
					t.Errorf("concurrent match=%v err=%v", matches, err)
					return
				}
				if got, err := p.EntityKey(record); err != nil || got != identity {
					t.Errorf("concurrent identity=%+v err=%v", got, err)
					return
				}
				fresh, err := p.Key(record)
				if err != nil {
					t.Error(err)
					return
				}
				if compared, err := p.Compare(key, fresh); err != nil || compared != 0 {
					t.Errorf("concurrent comparison=%d err=%v", compared, err)
					return
				}
				encoded, err := p.EncodeKey(fresh)
				if err != nil || !bytes.Equal(encoded, cursor) {
					t.Errorf("concurrent encoding=%q err=%v", encoded, err)
					return
				}
				decoded, err := p.DecodeKey(cursor)
				if err != nil {
					t.Error(err)
					return
				}
				if compared, err := p.Compare(fresh, decoded); err != nil || compared != 0 {
					t.Errorf("concurrent decode comparison=%d err=%v", compared, err)
					return
				}
				owned := p.Query()
				owned.Order[0].Descending = true
				owned.Filter.Comparison.Value.Data[0] = '9'
			}
		}()
	}
	workers.Wait()
	if !reflect.DeepEqual(record, before) || !reflect.DeepEqual(p.Query(), definition) {
		t.Fatal("concurrent evaluation mutated its inputs or plan")
	}
}
