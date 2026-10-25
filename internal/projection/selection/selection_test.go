package selection

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func text(s string) storage.Value {
	return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(s)}
}
func obj(r storage.Record) storage.Value {
	return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: r}
}
func list(v ...storage.Value) storage.Value {
	return storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: v}
}
func field(path string, value *Selection) Field {
	return Field{Path: storage.MustFieldPath(path), Value: value}
}

func TestUnionNestedSelectionsAndWholeValueRestoration(t *testing.T) {
	record := storage.Record{"metadata": obj(storage.Record{"owner": text("Alice"), "secret": text("hidden"), "members": list(obj(storage.Record{"name": text("Bob"), "email": text("b@example")}), storage.Value{State: storage.ValueNull})}), "title": text("Title")}
	before := storage.CloneRecord(record)
	a := Object([]Field{field("metadata", Object([]Field{field("owner", nil)}))})
	b := Object([]Field{field("metadata", Object([]Field{field("members", Elements(Object([]Field{field("name", nil)})))}))})
	whole := Paths([]storage.FieldPath{storage.MustFieldPath("metadata")})
	union := &Union{}
	union.Add(a, 1)
	union.Add(b, 1)
	want := storage.Record{"metadata": obj(storage.Record{"owner": text("Alice"), "members": list(obj(storage.Record{"name": text("Bob")}), storage.Value{State: storage.ValueNull})})}
	if got := union.View(record); !reflect.DeepEqual(got, want) {
		t.Fatalf("union=%v want=%v", got, want)
	}
	union.Add(a, 1)
	union.Add(a, -1)
	if got := union.View(record); !reflect.DeepEqual(got, want) {
		t.Fatal("removing one reference removed another's fields")
	}
	union.Add(whole, 1)
	if got := union.View(record); !reflect.DeepEqual(got["metadata"], record["metadata"]) {
		t.Fatal("whole-value request did not win")
	}
	union.Add(whole, -1)
	if got := union.View(record); !reflect.DeepEqual(got, want) {
		t.Fatal("narrow selections were lost behind whole-value request")
	}
	union.Add(a, -1)
	if got := union.View(record); !reflect.DeepEqual(got, b.View(record)) {
		t.Fatal("selection did not shrink")
	}
	union.Add(b, -1)
	if got := union.View(record); len(got) != 0 || !empty(&union.root) {
		t.Fatal("idle union retained selection nodes")
	}
	union.Add(nil, 1)
	if got := union.View(record); !reflect.DeepEqual(got, record) {
		t.Fatal("whole entity selection changed")
	}
	union.Add(nil, -1)
	if !reflect.DeepEqual(record, before) {
		t.Fatal("selection mutated accepted data")
	}
}

func TestSelectionPreservesMissingNullAndNestedArrayShape(t *testing.T) {
	mask := Object([]Field{field("missing", nil), field("null", Object([]Field{field("name", nil)})), field("nested", Elements(Elements(Object([]Field{field("name", nil)}))))})
	record := storage.Record{"null": {State: storage.ValueNull}, "nested": list(list(obj(storage.Record{"name": text("Alice"), "extra": text("not selected")})), list(), storage.Value{State: storage.ValueNull})}
	want := storage.Record{"null": {State: storage.ValueNull}, "nested": list(list(obj(storage.Record{"name": text("Alice")})), list(), storage.Value{State: storage.ValueNull})}
	if got := mask.View(record); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}

func TestRepeatedPathsAndLiteralNames(t *testing.T) {
	mask := Object([]Field{
		{Path: storage.MustFieldPath("a.b"), Value: Object([]Field{field("first", nil)})},
		{Path: storage.MustFieldPath("a.b"), Value: Object([]Field{field("second", nil)})},
		{Path: storage.MustFieldPath("a", "b")},
	})
	record := storage.Record{"a.b": obj(storage.Record{"first": text("1"), "second": text("2"), "third": text("3")}), "a": obj(storage.Record{"b": text("nested")})}
	want := storage.Record{"a.b": obj(storage.Record{"first": text("1"), "second": text("2")}), "a": obj(storage.Record{"b": text("nested")})}
	var union Union
	union.Add(mask, 1)
	if got := union.View(record); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v", got)
	}
	union.Add(mask, -1)
	if !empty(&union.root) {
		t.Fatal("duplicate paths leaked counts")
	}
}
