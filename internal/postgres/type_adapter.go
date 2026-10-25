package postgres

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

// postgresTypeAdapter is the sole translation boundary between PostgreSQL's
// OIDs/text protocol and the engine's logical value model. Unknown native
// types are omitted from the logical relation view; callers separately reject
// their use for identity, selection, filtering, or ordering.
type postgresTypeAdapter struct{}

func newPostgresTypeAdapter() *postgresTypeAdapter { return &postgresTypeAdapter{} }

// pgtype.Map memoizes encode/scan plans and is not safe for concurrent use.
// Borrow one exclusively per conversion. Only pgx's built-in types are used;
// maps and their plans never contain application records or custom registrations.
var postgresCodecMaps = sync.Pool{New: func() any { return pgtype.NewMap() }}

// PostgreSQL infinities use finite sentinels outside PostgreSQL's complete
// finite date range. The convention remains private to this adapter; the
// storage-neutral model only sees ordinary time.Time values.
var (
	postgresNegativeInfinity = time.Date(-4713, time.January, 1, 0, 0, 0, 0, time.UTC)
	postgresPositiveInfinity = time.Date(5874898, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// postgresTypeMapping is the single description of how a native PostgreSQL
// type participates in the logical model. A dynamic mapping has no single
// value type: each JSON value is typed after it is decoded.
type postgresTypeMapping struct {
	logicalType storage.ValueType
	dynamic     bool
}

func postgresMapping(oid uint32) (postgresTypeMapping, bool) {
	switch oid {
	case 16:
		return postgresTypeMapping{logicalType: storage.TypeBool}, true
	case 20, 21, 23, 26:
		return postgresTypeMapping{logicalType: storage.TypeInteger}, true
	case 700, 701:
		return postgresTypeMapping{logicalType: storage.TypeFloat}, true
	case 25, 1043:
		return postgresTypeMapping{logicalType: storage.TypeText}, true
	case 17:
		return postgresTypeMapping{logicalType: storage.TypeBytes}, true
	case 1082, 1114, 1184:
		return postgresTypeMapping{logicalType: storage.TypeDateTime}, true
	case 1700:
		return postgresTypeMapping{logicalType: storage.TypeDecimal}, true
	case 2950:
		return postgresTypeMapping{logicalType: storage.TypeUUID}, true
	case 114, 3802:
		return postgresTypeMapping{dynamic: true}, true
	default:
		return postgresTypeMapping{}, false
	}
}

func (postgresTypeAdapter) logicalType(oid uint32) (storage.ValueType, bool) {
	mapping, supported := postgresMapping(oid)
	return mapping.logicalType, supported
}

func (a postgresTypeAdapter) decodeCell(cell cdc.Cell) (storage.Value, bool, error) {
	mapping, supported := postgresMapping(cell.TypeOID)
	if !supported {
		return storage.Value{}, false, nil
	}
	if cell.Missing {
		return storage.Value{State: storage.ValueUnavailable}, true, nil
	}
	if cell.Null {
		return storage.Value{State: storage.ValueNull}, true, nil
	}
	if mapping.dynamic {
		value, err := decodeJSONValue(cell.Text)
		return value, true, err
	}
	data := cell.Text
	if mapping.logicalType == storage.TypeDateTime {
		instant, err := a.decodeDateTime(cell.TypeOID, data)
		if err != nil {
			return storage.Value{}, true, err
		}
		return storage.NewDateTime(instant), true, nil
	}
	if mapping.logicalType == storage.TypeBytes {
		var err error
		data, err = decodeBytea(data)
		if err != nil {
			return storage.Value{}, true, err
		}
	} else if cell.TypeOID == 700 {
		// PostgreSQL emits the shortest text that round-trips to float4. Parse at
		// the native precision before promoting it into the engine's sole float64
		// type; parsing the text directly as float64 changes the stored value.
		value, err := strconv.ParseFloat(string(data), 32)
		if err != nil {
			return storage.Value{}, true, fmt.Errorf("invalid PostgreSQL real %q", data)
		}
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			data = []byte(strconv.FormatFloat(value, 'g', -1, 64))
		}
	}
	value, err := storage.ParseScalar(mapping.logicalType, data)
	return value, true, err
}

func decodeBytea(data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, []byte(`\x`)) {
		return nil, errors.New("only PostgreSQL hex bytea output is supported")
	}
	decoded := make([]byte, hex.DecodedLen(len(data)-2))
	if _, err := hex.Decode(decoded, data[2:]); err != nil {
		return nil, fmt.Errorf("decode bytea: %w", err)
	}
	return decoded, nil
}

func (a postgresTypeAdapter) encodeScalar(oid uint32, value storage.Value) (any, error) {
	mapping, supported := postgresMapping(oid)
	if !supported || mapping.dynamic {
		return nil, fmt.Errorf("PostgreSQL type %d has no scalar logical encoding", oid)
	}
	want := mapping.logicalType
	if value.State == storage.ValueNull {
		return nil, nil
	}
	if value.State != storage.ValuePresent || value.Type != want {
		return nil, fmt.Errorf("logical %s cannot encode PostgreSQL type %d", value.Type, oid)
	}
	if want == storage.TypeBytes {
		return append([]byte(nil), value.Data...), nil
	}
	if want == storage.TypeDateTime {
		return encodePostgresDateTime(oid, value.DateTime)
	}
	text := string(value.Data)
	return text, nil
}

type postgresComparisonKind uint8

const (
	postgresComparisonScalar postgresComparisonKind = iota + 1
	postgresComparisonJSON
	postgresComparisonDate
	postgresComparisonMismatch
	postgresComparisonBoundedConstant
)

// postgresComparison is the adapter's single compiled description of a
// predicate leaf. Both literal normalization and snapshot SQL use it.
type postgresComparison struct {
	kind         postgresComparisonKind
	parameterOID uint32
}

func (a postgresTypeAdapter) prepareComparison(column column, comparison *storage.Comparison) (postgresComparison, error) {
	prepared := postgresComparison{}
	isJSON := column.typeOID == pgtype.JSONOID || column.typeOID == pgtype.JSONBOID
	if len(comparison.Field.Segments()) > 1 || isJSON {
		prepared.kind = postgresComparisonJSON
		switch comparison.Value.Type {
		case storage.TypeBool:
			prepared.parameterOID = pgtype.BoolOID
		case storage.TypeInteger, storage.TypeDecimal:
			prepared.parameterOID = pgtype.NumericOID
		case storage.TypeText:
			prepared.parameterOID = pgtype.TextOID
		default:
			return prepared, nil
		}
	} else if comparison.Value.Type != column.valueType {
		prepared.kind = postgresComparisonMismatch
		return prepared, nil
	} else if minimum, maximum, bounded := boundedIntegerDomain(column.typeOID); bounded {
		value, err := strconv.ParseInt(string(comparison.Value.Data), 10, 64)
		if err != nil {
			return postgresComparison{}, err
		}
		if value < minimum || value > maximum {
			prepared.kind = postgresComparisonBoundedConstant
			return prepared, nil
		}
		prepared.kind, prepared.parameterOID = postgresComparisonScalar, column.typeOID
	} else if column.typeOID == pgtype.DateOID {
		prepared.kind, prepared.parameterOID = postgresComparisonDate, pgtype.TimestamptzOID
	} else {
		prepared.kind, prepared.parameterOID = postgresComparisonScalar, column.typeOID
	}

	if err := a.normalizeComparisonLiteral(prepared.parameterOID, comparison); err != nil {
		return postgresComparison{}, err
	}
	return prepared, nil
}

// normalizeComparisonLiteral converts a logical query literal through its
// compiled snapshot parameter representation and back into the logical model.
func (a postgresTypeAdapter) normalizeComparisonLiteral(oid uint32, comparison *storage.Comparison) error {
	// PostgreSQL numeric is exact and snapshot SQL consumes our canonical
	// decimal text directly. pgtype.Numeric.ScanScientific passes through
	// float64 and therefore must not be used for arbitrary-precision values.
	if oid == pgtype.NumericOID {
		return nil
	}
	text := append([]byte(nil), comparison.Value.Data...)
	if comparison.Value.Type == storage.TypeBytes {
		text = []byte(`\x` + hex.EncodeToString(comparison.Value.Data))
	}
	if comparison.Value.Type == storage.TypeDateTime {
		native, err := encodePostgresDateTime(oid, comparison.Value.DateTime)
		if err != nil {
			return err
		}
		text = []byte(native)
	}
	nativeText, err := roundTripPostgresText(oid, text)
	if err != nil {
		return err
	}
	decoded, supported, err := a.decodeCell(cdc.Cell{TypeOID: oid, Text: nativeText})
	if err != nil {
		return err
	}
	if !supported || decoded.State != storage.ValuePresent || decoded.Type != comparison.Value.Type {
		return fmt.Errorf("PostgreSQL parameter type %d cannot represent logical %s", oid, comparison.Value.Type)
	}
	literal, err := storage.LiteralFromValue(decoded)
	if err != nil {
		return err
	}
	comparison.Value = literal
	return nil
}

func postgresParameterType(oid uint32) (string, error) {
	switch oid {
	case pgtype.BoolOID:
		return "boolean", nil
	case pgtype.Int2OID:
		return "smallint", nil
	case pgtype.Int4OID:
		return "integer", nil
	case pgtype.Int8OID:
		return "bigint", nil
	case pgtype.OIDOID:
		return "oid", nil
	case pgtype.Float4OID:
		return "real", nil
	case pgtype.Float8OID:
		return "double precision", nil
	case pgtype.TextOID:
		return "text", nil
	case pgtype.VarcharOID:
		return "varchar", nil
	case pgtype.ByteaOID:
		return "bytea", nil
	case pgtype.TimestampOID:
		return "timestamp", nil
	case pgtype.TimestamptzOID:
		return "timestamptz", nil
	case pgtype.NumericOID:
		return "numeric", nil
	case pgtype.UUIDOID:
		return "uuid", nil
	default:
		return "", fmt.Errorf("PostgreSQL parameter type %d is not supported", oid)
	}
}

func roundTripPostgresText(oid uint32, text []byte) ([]byte, error) {
	types := postgresCodecMaps.Get().(*pgtype.Map)
	defer postgresCodecMaps.Put(types)
	nativeType, ok := types.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("PostgreSQL parameter type %d is not registered", oid)
	}
	value, err := nativeType.Codec.DecodeValue(types, oid, pgtype.TextFormatCode, text)
	if err != nil {
		return nil, err
	}
	binary, err := types.Encode(oid, pgtype.BinaryFormatCode, value, nil)
	if err != nil {
		return nil, err
	}
	value, err = nativeType.Codec.DecodeValue(types, oid, pgtype.BinaryFormatCode, binary)
	if err != nil {
		return nil, err
	}
	return types.Encode(oid, pgtype.TextFormatCode, value, nil)
}

func (a postgresTypeAdapter) decodeDateTime(oid uint32, data []byte) (time.Time, error) {
	types := postgresCodecMaps.Get().(*pgtype.Map)
	defer postgresCodecMaps.Put(types)
	var instant pgtype.Timestamptz
	switch oid {
	case pgtype.DateOID:
		var value pgtype.Date
		if err := types.Scan(oid, pgtype.TextFormatCode, data, &value); err != nil {
			return time.Time{}, err
		}
		instant = pgtype.Timestamptz{Time: value.Time.UTC(), InfinityModifier: value.InfinityModifier, Valid: value.Valid}
	case pgtype.TimestampOID:
		var value pgtype.Timestamp
		if err := types.Scan(oid, pgtype.TextFormatCode, data, &value); err != nil {
			return time.Time{}, err
		}
		instant = pgtype.Timestamptz{Time: value.Time.UTC(), InfinityModifier: value.InfinityModifier, Valid: value.Valid}
	case pgtype.TimestamptzOID:
		if err := types.Scan(oid, pgtype.TextFormatCode, data, &instant); err != nil {
			return time.Time{}, err
		}
	default:
		return time.Time{}, fmt.Errorf("PostgreSQL type %d is not temporal", oid)
	}
	return a.normalizeDateTime(instant)
}

// normalizeDateTime is the PostgreSQL adapter's single native-loss boundary.
// A future adapter policy can reject infinities here instead of mapping them.
func (postgresTypeAdapter) normalizeDateTime(instant pgtype.Timestamptz) (time.Time, error) {
	if !instant.Valid {
		return time.Time{}, errors.New("invalid PostgreSQL datetime")
	}
	switch instant.InfinityModifier {
	case pgtype.NegativeInfinity:
		return postgresNegativeInfinity, nil
	case pgtype.Infinity:
		return postgresPositiveInfinity, nil
	case pgtype.Finite:
		return storage.NormalizeDateTime(instant.Time.Truncate(time.Microsecond)), nil
	default:
		return time.Time{}, errors.New("unknown PostgreSQL datetime infinity modifier")
	}
}

func encodePostgresDateTime(oid uint32, value time.Time) (string, error) {
	types := postgresCodecMaps.Get().(*pgtype.Map)
	defer postgresCodecMaps.Put(types)
	instant := pgtype.Timestamptz{Time: storage.NormalizeDateTime(value).Truncate(time.Microsecond), Valid: true}
	switch {
	case value.Equal(postgresNegativeInfinity):
		instant.InfinityModifier = pgtype.NegativeInfinity
	case value.Equal(postgresPositiveInfinity):
		instant.InfinityModifier = pgtype.Infinity
	}
	var native any
	switch oid {
	case pgtype.DateOID:
		instant.Time = time.Date(instant.Time.Year(), instant.Time.Month(), instant.Time.Day(), 0, 0, 0, 0, time.UTC)
		native = pgtype.Date(instant)
	case pgtype.TimestampOID:
		native = pgtype.Timestamp(instant)
	case pgtype.TimestamptzOID:
		native = instant
	default:
		return "", fmt.Errorf("PostgreSQL type %d is not temporal", oid)
	}
	encoded, err := types.Encode(oid, pgtype.TextFormatCode, native, nil)
	return string(encoded), err
}

func decodeJSONValue(data []byte) (storage.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return storage.Value{}, fmt.Errorf("decode JSON value: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return storage.Value{}, err
	}
	return logicalJSONValue(raw)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("JSON value has trailing data")
	}
	return err
}

func logicalJSONValue(raw any) (storage.Value, error) {
	switch value := raw.(type) {
	case nil:
		return storage.Value{State: storage.ValueNull}, nil
	case bool:
		data, _ := storage.Canonicalize(storage.TypeBool, []byte(strconv.FormatBool(value)))
		return storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: data}, nil
	case string:
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}, nil
	case json.Number:
		text := value.String()
		if !strings.ContainsAny(text, ".eE") {
			if data, err := storage.Canonicalize(storage.TypeInteger, []byte(text)); err == nil {
				return storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: data}, nil
			}
		}
		data, err := storage.Canonicalize(storage.TypeDecimal, []byte(text))
		if err != nil {
			return storage.Value{}, err
		}
		return storage.Value{Type: storage.TypeDecimal, State: storage.ValuePresent, Data: data}, nil
	case []any:
		result := storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: make([]storage.Value, len(value))}
		for i := range value {
			child, err := logicalJSONValue(value[i])
			if err != nil {
				return storage.Value{}, err
			}
			result.List[i] = child
		}
		return result, nil
	case map[string]any:
		result := storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: make(map[string]storage.Value, len(value))}
		for key, rawChild := range value {
			child, err := logicalJSONValue(rawChild)
			if err != nil {
				return storage.Value{}, err
			}
			result.Object[key] = child
		}
		return result, nil
	default:
		return storage.Value{}, fmt.Errorf("unsupported decoded JSON value %T", raw)
	}
}
