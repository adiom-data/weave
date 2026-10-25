package postgres

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestUnsignedCursorRoundTripAndValidation(t *testing.T) {
	p := &plan{fingerprint: "query", order: []storage.OrderTerm{{Field: "title"}, {Field: "id"}}}
	text := "a.b\"\n日本語"
	values := []*string{&text, nil}
	cursor, err := p.encodeCursor(values)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(cursor) {
		t.Fatalf("cursor is not an unsigned payload: %q", cursor)
	}
	decoded, err := p.decodeCursor(cursor)
	if err != nil || !reflect.DeepEqual(decoded, values) {
		t.Fatalf("round trip = %v, %v", decoded, err)
	}
	for _, invalid := range []string{
		"", "null", "{", string(cursor) + ".obsolete-signature",
		`{"q":"other-query","v":["a",null]}`,
		`{"q":"query","v":["a"]}`,
		`{"q":"query","v":[1,null]}`,
	} {
		if _, err := p.decodeCursor([]byte(invalid)); err == nil {
			t.Errorf("accepted invalid cursor %q", invalid)
		}
	}
	// Query identity and shape are checked, not authenticity. Protecting a
	// cursor at an untrusted client boundary is an application-layer concern.
	if _, err := p.decodeCursor([]byte(`{"q":"query","v":["another",null]}`)); err != nil {
		t.Fatalf("unsigned valid cursor rejected: %v", err)
	}
}
