package value

import (
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func scalar(t *testing.T, kind storage.ValueType, s string) storage.Value {
	t.Helper()
	v, err := storage.ParseScalar(kind, []byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func list(values ...storage.Value) storage.Value {
	return storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: values}
}
func object(values map[string]storage.Value) storage.Value {
	return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: values}
}

func TestEqualityAndMembershipUseCanonicalTypes(t *testing.T) {
	i := Int(1)
	f := scalar(t, storage.TypeFloat, "1")
	nan := scalar(t, storage.TypeFloat, "NaN")
	uuid := scalar(t, storage.TypeUUID, "00000000-0000-0000-0000-000000000001")
	text := scalar(t, storage.TypeText, string(uuid.Data))
	for _, test := range []struct {
		a, b storage.Value
		want storage.Value
	}{
		{i, i, True}, {i, f, False}, {f, i, False}, {uuid, text, False}, {uuid, uuid, True}, {nan, nan, False},
		{Null, storage.Value{Type: storage.TypeText, State: storage.ValueNull}, True},
		{list(i), list(f), False}, {list(i, Unknown), list(i, Int(2)), Unknown},
		{list(Unknown, i), list(Int(2), Int(3)), False},
		{object(map[string]storage.Value{}), object(map[string]storage.Value{"a": Null}), False},
		{scalar(t, storage.TypeDecimal, "1.00"), scalar(t, storage.TypeDecimal, "1e0"), True},
	} {
		got, err := Equal(test.a, test.b)
		if err != nil || !storage.EqualValues(got, test.want) {
			t.Fatalf("Equal(%+v,%+v)=%+v %v; want %+v", test.a, test.b, got, err, test.want)
		}
	}
	for _, test := range []struct{ collection, needle, want storage.Value }{
		{list(i), f, False}, {list(uuid), text, False}, {list(Unknown, i), i, True},
		{list(Unknown, i), Int(2), Unknown}, {list(i), Int(2), False},
		{object(map[string]storage.Value{"x": Unknown}), scalar(t, storage.TypeText, "x"), True},
	} {
		got, err := Contains(test.collection, test.needle)
		if err != nil || !storage.EqualValues(got, test.want) {
			t.Fatalf("Contains=%+v %v; want %+v", got, err, test.want)
		}
	}
}

func TestOrderingAndUnicode(t *testing.T) {
	if _, err := Compare(Int(1), scalar(t, storage.TypeFloat, "1")); err == nil {
		t.Fatal("mixed numeric ordering accepted")
	}
	if n, err := Compare(Int(9007199254740993), Int(9007199254740992)); err != nil || n <= 0 {
		t.Fatalf("lost integer precision: %d %v", n, err)
	}
	for _, op := range []string{"<", "<=", ">", ">="} {
		got, err := Relational(op, scalar(t, storage.TypeFloat, "NaN"), scalar(t, storage.TypeFloat, "1"))
		if err != nil || IsTrue(got) {
			t.Fatalf("NaN %s: %+v %v", op, got, err)
		}
	}
	if n, err := Size(scalar(t, storage.TypeText, "x🙂")); err != nil || n != 2 {
		t.Fatalf("Unicode size=%d %v", n, err)
	}
}
