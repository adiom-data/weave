package postgres

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPostgresCodecReuseIsConcurrentAndOutputsAreOwned(t *testing.T) {
	var workers sync.WaitGroup
	for n := range 12 {
		workers.Go(func() {
			for i := range 100 {
				text := strconv.Itoa(n*100 + i)
				encoded, err := roundTripPostgresText(pgtype.Int8OID, []byte(text))
				if err != nil || string(encoded) != text {
					t.Errorf("integer round trip = %q, %v", encoded, err)
					return
				}
				// Subsequent conversions must not reuse an output's backing bytes.
				if _, err := roundTripPostgresText(pgtype.Float8OID, []byte("1.25")); err != nil {
					t.Error(err)
					return
				}
				if string(encoded) != text {
					t.Error("codec output changed after reuse")
				}
				instant := time.Date(2026, 10, 30, 12, 0, i, n*1000, time.UTC)
				native, err := encodePostgresDateTime(pgtype.TimestamptzOID, instant)
				if err != nil {
					t.Error(err)
					return
				}
				decoded, err := (postgresTypeAdapter{}).decodeDateTime(pgtype.TimestamptzOID, []byte(native))
				if err != nil || !decoded.Equal(instant) {
					t.Errorf("datetime round trip = %v, %v", decoded, err)
				}
				if _, err := roundTripPostgresText(pgtype.Int8OID, []byte("invalid")); err == nil {
					t.Error("invalid conversion unexpectedly succeeded")
				}
			}
		})
	}
	workers.Wait()
}

func TestPostgresTemporalTypesShareDateTimeRepresentation(t *testing.T) {
	adapter := postgresTypeAdapter{}
	tests := []cdc.Cell{
		{TypeOID: 1082, Text: []byte("2026-10-23")},
		{TypeOID: 1114, Text: []byte("2026-10-23 00:00:00")},
		{TypeOID: 1184, Text: []byte("2026-10-22 17:00:00-07")},
	}
	var want time.Time
	for _, cell := range tests {
		value, supported, err := adapter.decodeCell(cell)
		if err != nil || !supported || value.Type != storage.TypeDateTime {
			t.Fatalf("decode %+v = %+v, %v, %v", cell, value, supported, err)
		}
		if want.IsZero() {
			want = value.DateTime
		} else if !value.DateTime.Equal(want) {
			t.Fatalf("datetime %s != %s", value.DateTime, want)
		}
	}
}

func TestPostgresTemporalAdapterNormalizesPrecisionAndSaturatesBCAndInfinity(t *testing.T) {
	input, err := storage.ParseDateTime("2026-10-23T12:00:00.123456789Z")
	if err != nil {
		t.Fatal(err)
	}
	comparison := &storage.Comparison{
		Field: "instant", Operator: storage.ComparisonEqual,
		Value: storage.Literal{Type: storage.TypeDateTime, DateTime: input},
	}
	_, err = (postgresTypeAdapter{}).prepareComparison(column{name: "instant", typeOID: 1184, valueType: storage.TypeDateTime, supported: true}, comparison)
	if err != nil {
		t.Fatal(err)
	}
	if normalized := comparison.Value.DateTime; !normalized.Equal(time.Date(2026, 10, 23, 12, 0, 0, 123456000, time.UTC)) {
		t.Fatalf("normalized datetime = %s", normalized)
	}

	value, supported, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: 1082, Text: []byte("0001-01-01 BC")})
	if err != nil || !supported || !value.DateTime.Equal(time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("BC date decode = %+v supported=%v err=%v", value, supported, err)
	}
	native, err := (postgresTypeAdapter{}).encodeScalar(1082, value)
	if err != nil || native != "0001-01-01 BC" {
		t.Fatalf("BC date encode = %#v err=%v", native, err)
	}
	value, supported, err = (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: 1184, Text: []byte("infinity")})
	if err != nil || !supported || !value.DateTime.Equal(postgresPositiveInfinity) {
		t.Fatalf("infinity decode = %+v supported=%v err=%v", value, supported, err)
	}
	native, err = (postgresTypeAdapter{}).encodeScalar(1184, value)
	if err != nil || native != "infinity" {
		t.Fatalf("infinity encode = %#v err=%v", native, err)
	}
}

func TestComparisonLiteralsRoundTripThroughSnapshotParameterTypes(t *testing.T) {
	tests := []struct {
		name      string
		column    column
		field     storage.FieldPath
		valueType storage.ValueType
		input     string
		want      string
	}{
		{
			name: "real uses float4 precision", column: column{name: "score", typeOID: 700, valueType: storage.TypeFloat, supported: true},
			field: "score", valueType: storage.TypeFloat, input: "0.1", want: strconv.FormatFloat(float64(float32(0.1)), 'g', -1, 64),
		},
		{
			name: "date endpoint uses snapshot timestamptz", column: column{name: "day", typeOID: 1082, valueType: storage.TypeDateTime, supported: true},
			field: "day", valueType: storage.TypeDateTime, input: "2024-01-01T12:00:00.123456789Z", want: "2024-01-01 12:00:00.123456Z",
		},
		{
			name: "json integer retains its logical type", column: column{name: "properties", typeOID: 114, supported: true},
			field: storage.MustFieldPath("properties", "count"), valueType: storage.TypeInteger, input: "1", want: "1",
		},
		{
			name: "timestamp without timezone uses a native timestamp", column: column{name: "instant", typeOID: 1114, valueType: storage.TypeDateTime, supported: true},
			field: "instant", valueType: storage.TypeDateTime, input: "2024-01-01T00:00:00.0000006Z", want: "2024-01-01 00:00:00Z",
		},
		{
			name: "scientific decimal remains exact", column: column{name: "amount", typeOID: 1700, valueType: storage.TypeDecimal, supported: true},
			field: "amount", valueType: storage.TypeDecimal, input: "1.234567890123456789e19", want: "1.234567890123456789e19",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := storage.ParseScalar(test.valueType, []byte(test.input))
			if err != nil {
				t.Fatal(err)
			}
			literal, err := storage.LiteralFromValue(value)
			if err != nil {
				t.Fatal(err)
			}
			comparison := &storage.Comparison{Field: test.field, Operator: storage.ComparisonEqual, Value: literal}
			if _, err := (postgresTypeAdapter{}).prepareComparison(test.column, comparison); err != nil {
				t.Fatal(err)
			}
			got := string(comparison.Value.Data)
			if test.valueType == storage.TypeDateTime {
				got = comparison.Value.DateTime.Format("2006-01-02 15:04:05.999999999Z07:00")
			}
			if got != test.want {
				t.Fatalf("normalized literal = %q, want %q", got, test.want)
			}
			if comparison.Value.Type != test.valueType {
				t.Fatalf("logical type changed to %s", comparison.Value.Type)
			}
		})
	}
}

func TestPostgresRealPromotesItsNativeFloat32Value(t *testing.T) {
	value, supported, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: 700, Text: []byte("0.1")})
	if err != nil || !supported {
		t.Fatalf("decode real: supported=%v err=%v", supported, err)
	}
	want := strconv.FormatFloat(float64(float32(0.1)), 'g', -1, 64)
	if value.Type != storage.TypeFloat || string(value.Data) != want {
		t.Fatalf("real decoded as %q, want %q", value.Data, want)
	}
}

func TestPostgresJSONBecomesRecursiveLogicalValue(t *testing.T) {
	value, supported, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: 3802, Text: []byte(`{"name":"task","count":2,"labels":["a",null]}`)})
	if err != nil || !supported || value.Type != storage.TypeObject {
		t.Fatalf("decode = %+v, %v, %v", value, supported, err)
	}
	if value.Object["name"].Type != storage.TypeText || value.Object["count"].Type != storage.TypeInteger {
		t.Fatalf("object = %+v", value.Object)
	}
	labels := value.Object["labels"]
	if labels.Type != storage.TypeList || len(labels.List) != 2 || labels.List[1].State != storage.ValueNull {
		t.Fatalf("labels = %+v", labels)
	}
}

func TestPostgresJSONExtremeNumbersRemainLoadable(t *testing.T) {
	value, supported, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{
		TypeOID: 114,
		Text:    []byte(`{"large":1e999999999999999999999,"small":-1e-999999999999999999999}`),
	})
	if err != nil || !supported {
		t.Fatalf("decode extreme JSON numbers: supported=%v err=%v", supported, err)
	}
	if got := string(value.Object["large"].Data); got != "Infinity" {
		t.Fatalf("large number = %q, want Infinity", got)
	}
	if got := string(value.Object["small"].Data); got != "0" {
		t.Fatalf("small number = %q, want 0", got)
	}
}

func TestPostgresByteaAndUnsupportedTypePolicy(t *testing.T) {
	adapter := postgresTypeAdapter{}
	value, supported, err := adapter.decodeCell(cdc.Cell{TypeOID: 17, Text: []byte(`\x00ff`)})
	if err != nil || !supported || value.Type != storage.TypeBytes || !bytes.Equal(value.Data, []byte{0, 255}) {
		t.Fatalf("bytea = %+v, %v, %v", value, supported, err)
	}
	if _, supported, err := adapter.decodeCell(cdc.Cell{TypeOID: 999999, Text: []byte("native")}); err != nil || supported {
		t.Fatalf("unsupported type = supported %v, err %v", supported, err)
	}
	if _, supported, err := adapter.decodeCell(cdc.Cell{TypeOID: 999999, Missing: true}); err != nil || supported {
		t.Fatalf("unsupported unchanged-TOAST type = supported %v, err %v", supported, err)
	}
}
