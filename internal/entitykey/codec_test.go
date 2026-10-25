package entitykey

import (
	"reflect"
	"testing"
)

func TestRoundTripArbitraryCompositeKey(t *testing.T) {
	parts := []Part{{Name: "tenant=id", TypeOID: 25, Value: []byte("a\x1fb")}, {Name: "id", TypeOID: 2950, Value: []byte("uuid")}}
	decoded, err := Decode(Encode(parts))
	if err != nil || !reflect.DeepEqual(decoded, parts) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}

func TestCanonicalEncodeIgnoresComponentOrder(t *testing.T) {
	first := []Part{{Name: "tenant_id", TypeOID: 20, Value: []byte("10")}, {Name: "id", TypeOID: 20, Value: []byte("20")}}
	second := []Part{{Name: "id", TypeOID: 20, Value: []byte("20")}, {Name: "tenant_id", TypeOID: 20, Value: []byte("10")}}
	if CanonicalEncode(first) != CanonicalEncode(second) {
		t.Fatal("equivalent composite identities encoded differently")
	}
	if first[0].Name != "tenant_id" {
		t.Fatal("canonical encoding mutated its input")
	}
	decoded, err := Decode(CanonicalEncode(first))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[0].Name != "id" || decoded[1].Name != "tenant_id" {
		t.Fatalf("canonical component order = %+v", decoded)
	}
}
