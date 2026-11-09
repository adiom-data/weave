package projectionserver

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/selection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"google.golang.org/protobuf/proto"
)

// Deliberately isolates output shaping from datastore/network/auth latency.
// Payload strings are 64 bytes, with eight payload fields per array element.
// The wide-object fixture has size payload fields and one selected name.
func embeddedBenchmarkFixture(kind string, size int) (storage.Record, *fieldSelection) {
	text := func(value string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}
	}
	object := func(fields storage.Record) storage.Value {
		return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: fields}
	}
	payload := strings.Repeat("x", 64)
	metadata := storage.Record{"name": text("Name")}
	if kind == "object" {
		for i := 0; i < size; i++ {
			metadata[fmt.Sprintf("unused_%04d", i)] = text(payload)
		}
	} else {
		items := make([]storage.Value, size)
		for i := range items {
			fields := storage.Record{"name": text(fmt.Sprintf("Item %d", i))}
			for j := 0; j < 8; j++ {
				fields[fmt.Sprintf("unused_%04d", j)] = text(payload)
			}
			items[i] = object(fields)
		}
		metadata["items"] = storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: items}
	}
	return storage.Record{"metadata": object(metadata)}, embeddedBenchmarkMask(kind, size, "selected")
}

// Keep the explicit recursive-all mask distinct from selecting the whole
// metadata value: identical output, but the former exercises every mask node.
func embeddedBenchmarkMask(kind string, size int, mode string) *fieldSelection {
	count := size
	if kind == "array" {
		count = 8
	}
	fields := []selection.Field{{Path: storage.MustFieldPath("name")}}
	if mode != "selected" {
		for i := 0; i < count; i++ {
			if mode == "dense" && i == 0 {
				continue
			}
			fields = append(fields, selection.Field{Path: storage.MustFieldPath(fmt.Sprintf("unused_%04d", i))})
		}
	}
	selected := selection.Object(fields)
	if kind == "array" {
		fields = []selection.Field{{Path: storage.MustFieldPath("items"), Value: selection.Elements(selected)}}
		if mode != "selected" {
			fields = append(fields, selection.Field{Path: storage.MustFieldPath("name")})
		}
		selected = selection.Object(fields)
	}
	return &fieldSelection{stored: selection.Object([]selection.Field{{Path: storage.MustFieldPath("metadata"), Value: selected}})}
}

type embeddedBenchmarkMode struct {
	name string
	mask *fieldSelection
	// The payload field is omitted only in sparse and dense selections.
	payloadSelected bool
}

func embeddedBenchmarkModes(kind string, size int, narrow *fieldSelection) []embeddedBenchmarkMode {
	return []embeddedBenchmarkMode{
		{"whole", selectColumns([]storage.FieldPath{storage.MustFieldPath("metadata")}), true},
		{"selected", narrow, false},
		{"dense", embeddedBenchmarkMask(kind, size, "dense"), false},
		{"all", embeddedBenchmarkMask(kind, size, "all"), true},
	}
}

func embeddedBenchmarkChange(record storage.Record, kind, field string) storage.Record {
	result := storage.CloneRecord(record)
	fields := result["metadata"].Object
	if kind == "array" {
		items := fields["items"].List
		fields = items[len(items)/2].Object
	}
	fields[field] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("Changed")}
	return result
}

func embeddedBenchmarkCases(b *testing.B, run func(*testing.B, string, storage.Record, embeddedBenchmarkMode)) {
	for _, kind := range []string{"object", "array"} {
		sizes := []int{16, 256, 1024}
		if kind == "array" {
			sizes = []int{100, 1000, 10000}
		}
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/%d", kind, size), func(b *testing.B) {
				record, narrow := embeddedBenchmarkFixture(kind, size)
				for _, mode := range embeddedBenchmarkModes(kind, size, narrow) {
					b.Run(mode.name, func(b *testing.B) {
						run(b, kind, record, mode)
					})
				}
			})
		}
	}
}

// Initial publication from an already accepted image: selection, ownership
// copies, canonical protobuf values, and binary serialization. Excludes the
// full-record acceptance clone measured by BenchmarkEmbeddedUpdate below.
func BenchmarkEmbeddedPublication(b *testing.B) {
	embeddedBenchmarkCases(b, func(b *testing.B, _ string, record storage.Record, mode embeddedBenchmarkMode) {
		var union selection.Union
		union.Add(storedSelection(mode.mask), 1)
		var bytes uint64
		b.ReportAllocs()
		for b.Loop() {
			entity := modelEntity{hasImage: true, record: record}
			publication, err := entity.prepare("entity", union.View)
			if err != nil || publication == nil {
				b.Fatalf("publication=%v err=%v", publication, err)
			}
			encoded, err := proto.Marshal(publication.event)
			if err != nil {
				b.Fatal(err)
			}
			bytes += uint64(len(encoded))
		}
		b.ReportMetric(float64(bytes)/float64(b.N), "wire-bytes/op")
	})
}

// Live entity updates through the real connection model: full-record acceptance
// clone, version handling, selected-image comparison, publication, and binary
// serialization. Alternating immutable images change name or a payload field;
// explicit fixture expectations distinguish an emitted event from suppression.
// Query/visibility work is excluded.
func BenchmarkEmbeddedUpdate(b *testing.B) {
	embeddedBenchmarkCases(b, func(b *testing.B, kind string, record storage.Record, mode embeddedBenchmarkMode) {
		for _, change := range []struct {
			name, field string
			emits       bool
		}{
			{"name", "name", true},
			{"payload", "unused_0000", mode.payloadSelected},
		} {
			b.Run(change.name, func(b *testing.B) {
				records := [2]storage.Record{record, embeddedBenchmarkChange(record, kind, change.field)}
				model := newConnectionModel()
				key := modelKey(snapshotTestRelation(), "embedded-benchmark")
				masks := map[string]*fieldSelection{"root": mode.mask}
				event := proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: "root", InstanceID: "root", EntityNode: true, Kind: q.Updated, Key: key, Record: record, Version: q.RecordVersion{Revision: 1}}
				initial, err := model.applyProjection("scope", proj.ProjectionBatch{Events: []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, Node: "root", InstanceID: "root", EntityNode: true}, event}}, masks)
				if err != nil || len(initial.Entities) != 1 {
					b.Fatalf("initial=%v err=%v", initial, err)
				}
				var iteration, bytes, emissions uint64
				// Expectations come from the fixture contract, independently of
				// prepare/View, so a regression cannot redefine the oracle.
				wantEvents := 0
				if change.emits {
					wantEvents = 1
				}
				b.ReportAllocs()
				for b.Loop() {
					iteration++
					event.Version.Revision = iteration + 1
					event.Record = records[iteration%2]
					batch, err := model.applyProjection("scope", proj.ProjectionBatch{Events: []proj.ProjectionEvent{event}}, masks)
					if err != nil {
						b.Fatal(err)
					}
					if len(batch.Entities) != wantEvents {
						b.Fatalf("entities=%d want=%d", len(batch.Entities), wantEvents)
					}
					if len(batch.Entities) > 0 {
						encoded, err := proto.Marshal(batch)
						if err != nil {
							b.Fatal(err)
						}
						emissions++
						bytes += uint64(len(encoded))
					}
				}
				b.ReportMetric(float64(bytes)/float64(b.N), "wire-bytes/op")
				b.ReportMetric(float64(emissions)/float64(b.N), "entity-events/op")
			})
		}
	})
}

// Counts selection changes only: adding/removing one overlapping reference
// should depend on that mask's size, not the number of subscribers or rows.
func BenchmarkEmbeddedSelectionReferences(b *testing.B) {
	_, mask := embeddedBenchmarkFixture("array", 1)
	for _, references := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("references=%d", references), func(b *testing.B) {
			var union selection.Union
			for i := 0; i < references; i++ {
				union.Add(storedSelection(mask), 1)
			}
			b.ReportAllocs()
			for b.Loop() {
				union.Add(storedSelection(mask), 1)
				union.Add(storedSelection(mask), -1)
			}
		})
	}
}

func TestEmbeddedBenchmarkFixtures(t *testing.T) {
	for _, kind := range []string{"object", "array"} {
		record, narrow := embeddedBenchmarkFixture(kind, 8)
		for _, mode := range embeddedBenchmarkModes(kind, 8, narrow) {
			t.Run(kind+"/"+mode.name, func(t *testing.T) {
				var union selection.Union
				union.Add(storedSelection(mode.mask), 1)
				entity := &modelEntity{hasImage: true, record: record}
				initial, err := entity.prepare("entity", union.View)
				if err != nil || initial == nil {
					t.Fatalf("initial=%v err=%v", initial, err)
				}
				fields := initial.record["metadata"].Object
				if kind == "array" {
					wantRootFields := 2
					if mode.name == "selected" {
						wantRootFields = 1
					}
					if len(fields) != wantRootFields || len(fields["items"].List) != 8 {
						t.Fatal("array fixture changed shape")
					}
					fields = fields["items"].List[0].Object
				}
				wantFields := 9
				switch mode.name {
				case "selected":
					wantFields = 1
				case "dense":
					wantFields = 8
				}
				if len(fields) != wantFields {
					t.Fatalf("fields=%d want=%d", len(fields), wantFields)
				}
				if _, present := fields["name"]; !present {
					t.Fatal("fixture omitted name")
				}
				if _, present := fields["unused_0000"]; present != mode.payloadSelected {
					t.Fatal("payload selection disagrees with fixture contract")
				}
				if mode.payloadSelected && !reflect.DeepEqual(initial.record, record) {
					t.Fatal("whole/recursive-all fixture changed the full image")
				}
				entity.output, entity.outputRecord = initial.output, initial.record
				for _, change := range []struct {
					field string
					emits bool
				}{
					{"name", true}, {"unused_0000", mode.payloadSelected},
				} {
					entity.record = embeddedBenchmarkChange(record, kind, change.field)
					publication, err := entity.prepare("entity", union.View)
					if err != nil || (publication != nil) != change.emits {
						t.Fatalf("field=%s publication=%v wantEmission=%t err=%v", change.field, publication, change.emits, err)
					}
				}
			})
		}
	}
}
