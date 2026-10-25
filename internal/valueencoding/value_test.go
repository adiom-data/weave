package valueencoding

import (
	"bytes"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

func TestEncodingAgreesWithLogicalEquality(t *testing.T) {
	text := storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("value")}
	values := []storage.Value{
		{}, {State: storage.ValueNull}, {State: storage.ValueUnavailable},
		text, {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("other")},
		{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")},
		{Type: storage.TypeList, State: storage.ValuePresent},
		{Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{}},
		{Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{text}},
		{Type: storage.TypeObject, State: storage.ValuePresent},
		{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{}},
		{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{"a": text, "b": text}},
		{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{"b": text, "a": text}},
		{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{"c": text}},
		storage.NewDateTime(time.Date(2026, 10, 28, 0, 0, 0, 123, time.UTC)),
		storage.NewDateTime(time.Date(2026, 10, 28, 0, 0, 0, 124, time.UTC)),
		storage.NewDateTime(time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	for i, left := range values {
		for j, right := range values {
			want := bytes.Equal(CanonicalBytes(left), CanonicalBytes(right))
			if got := storage.EqualValues(left, right); got != want {
				t.Fatalf("values %d/%d: logical equality %v differs from identity equality %v", i, j, got, want)
			}
		}
	}
}

func TestDateTimePreservesFiniteValuesOutsideProtobufRange(t *testing.T) {
	want := time.Date(-100, time.February, 3, 4, 5, 6, 7, time.FixedZone("source", 3600))
	value := storage.NewDateTime(want)
	want = want.UTC()
	if !value.DateTime.Equal(want) || value.DateTime.Location() != time.UTC {
		t.Fatalf("datetime = %s, want %s", value.DateTime, want)
	}
	encoded, err := ScalarBytes(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseScalarBytes(storage.TypeDateTime, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !storage.EqualValues(value, decoded) {
		t.Fatalf("identity round trip = %s, want %s", decoded.DateTime, value.DateTime)
	}
}
