// Package storage defines shared logical values, records, and query definitions.
// Datastore snapshot, change-feed, and evaluation contracts are internal.
package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrResumeRequired   = errors.New("change feed can no longer resume")
	ErrIncompleteRecord = errors.New("record is incomplete")
)

type SourceID string

// FieldPath is an exact sequence of object-field names. Its underlying encoded
// form is comparable so it can safely key routing maps; callers must use
// NewFieldPath when a path contains more than one segment. A plain string is
// retained as the canonical encoding of a one-segment path for compactness and
// backwards compatibility with source-neutral adapters.
type FieldPath string

const encodedPathPrefix = "\x00"

func NewFieldPath(segments ...string) (FieldPath, error) {
	if len(segments) == 0 {
		return "", errors.New("field path requires at least one segment")
	}
	for _, segment := range segments {
		if segment == "" {
			return "", errors.New("field path segments cannot be empty")
		}
	}
	if len(segments) == 1 && segments[0] != "" && segments[0][0] != 0 {
		return FieldPath(segments[0]), nil
	}
	encoded, err := json.Marshal(segments)
	if err != nil {
		return "", err
	}
	return FieldPath(encodedPathPrefix + string(encoded)), nil
}

func MustFieldPath(segments ...string) FieldPath {
	path, err := NewFieldPath(segments...)
	if err != nil {
		panic(err)
	}
	return path
}

func (p FieldPath) Segments() []string {
	segments, _ := p.parseSegments()
	return segments
}

func (p FieldPath) Validate() error {
	_, err := p.parseSegments()
	return err
}

func (p FieldPath) parseSegments() ([]string, error) {
	encoded := string(p)
	if encoded == "" {
		return nil, errors.New("field path is empty")
	}
	if encoded[0] != 0 {
		return []string{encoded}, nil
	}
	var segments []string
	if err := json.Unmarshal([]byte(encoded[1:]), &segments); err != nil {
		return nil, fmt.Errorf("invalid encoded field path: %w", err)
	}
	if len(segments) == 0 {
		return nil, errors.New("field path requires at least one segment")
	}
	for _, segment := range segments {
		if segment == "" {
			return nil, errors.New("field path segments cannot be empty")
		}
	}
	return segments, nil
}

func (p FieldPath) Root() string {
	segments := p.Segments()
	if len(segments) == 0 {
		return ""
	}
	return segments[0]
}

func (p FieldPath) Display() string {
	encoded, _ := json.Marshal(p.Segments())
	return string(encoded)
}

// RelationKey is stable for the lifetime of a source relation. Native adapters
// should include an opaque relation/collection identity when names can be reused.
type RelationKey struct {
	Source    SourceID
	Namespace string
	Name      string
	OpaqueID  string
}

// EntityKey is the source's canonical encoding of a primary/document key.
type EntityKey struct {
	Relation RelationKey
	Value    string
}

type ValueState uint8

const (
	ValueMissing ValueState = iota
	ValueNull
	ValuePresent
	ValueUnavailable
)

// ValueType is one of the engine's datastore-independent logical types.
type ValueType string

const (
	TypeBool     ValueType = "bool"
	TypeInteger  ValueType = "integer"
	TypeDecimal  ValueType = "decimal"
	TypeFloat    ValueType = "float"
	TypeText     ValueType = "text"
	TypeBytes    ValueType = "bytes"
	TypeUUID     ValueType = "uuid"
	TypeDateTime ValueType = "datetime"
	TypeList     ValueType = "list"
	TypeObject   ValueType = "object"
)

// Value is the engine's canonical logical representation. Byte-backed scalar
// types use Data, datetimes use DateTime, and List/Object contain recursive
// values. Native datastore encodings must not cross the adapter boundary.
type Value struct {
	Type     ValueType
	State    ValueState
	Data     []byte
	DateTime time.Time
	List     []Value
	Object   map[string]Value
}

// Validate checks that a value is a well-formed member of the logical model.
// It deliberately accepts ValueUnavailable because partial change images use
// that state; authoritative-record validation rejects it separately.
func (value Value) Validate() error {
	return value.validate(true)
}

// ValidateComplete additionally rejects unavailable values at any recursive
// depth and is used for authoritative entity images.
func (value Value) ValidateComplete() error {
	return value.validate(false)
}

func (value Value) validate(allowIncomplete bool) error {
	switch value.State {
	case ValueMissing:
		if value.hasPayload() {
			return fmt.Errorf("incomplete value carries a payload")
		}
		return errors.New("missing values must be represented by an absent field")
	case ValueUnavailable:
		if value.hasPayload() {
			return fmt.Errorf("incomplete value carries a payload")
		}
		if !allowIncomplete {
			return errors.New("value is incomplete")
		}
		return nil
	case ValueNull:
		if value.Type != "" && !knownValueType(value.Type) {
			return fmt.Errorf("null value has unknown type %q", value.Type)
		}
		if len(value.Data) != 0 || !value.DateTime.IsZero() || value.List != nil || value.Object != nil {
			return errors.New("null value carries a payload")
		}
		return nil
	case ValuePresent:
	default:
		return fmt.Errorf("unknown value state %d", value.State)
	}

	if !knownValueType(value.Type) {
		return fmt.Errorf("present value has unknown type %q", value.Type)
	}
	switch value.Type {
	case TypeList:
		if len(value.Data) != 0 || !value.DateTime.IsZero() || value.Object != nil {
			return errors.New("list value has a scalar or object payload")
		}
		for index, child := range value.List {
			if err := child.validate(allowIncomplete); err != nil {
				return fmt.Errorf("list item %d: %w", index, err)
			}
		}
	case TypeObject:
		if len(value.Data) != 0 || !value.DateTime.IsZero() || value.List != nil {
			return errors.New("object value has a scalar or list payload")
		}
		for field, child := range value.Object {
			if err := child.validate(allowIncomplete); err != nil {
				return fmt.Errorf("object field %q: %w", field, err)
			}
		}
	case TypeDateTime:
		if len(value.Data) != 0 || value.List != nil || value.Object != nil {
			return errors.New("datetime value has a non-datetime payload")
		}
		if !IsCanonicalDateTime(value.DateTime) {
			return errors.New("datetime value is not canonical")
		}
	default:
		if value.List != nil || value.Object != nil {
			return errors.New("scalar value has a recursive payload")
		}
		if !value.DateTime.IsZero() {
			return errors.New("non-datetime scalar carries a datetime payload")
		}
		canonical, err := Canonicalize(value.Type, value.Data)
		if err != nil {
			return err
		}
		if !bytes.Equal(canonical, value.Data) {
			return fmt.Errorf("%s value is not canonical", value.Type)
		}
	}
	return nil
}

func (value Value) hasPayload() bool {
	return value.Type != "" || len(value.Data) != 0 || !value.DateTime.IsZero() || value.List != nil || value.Object != nil
}

func knownValueType(valueType ValueType) bool {
	switch valueType {
	case TypeBool, TypeInteger, TypeDecimal, TypeFloat, TypeText, TypeBytes, TypeUUID, TypeDateTime, TypeList, TypeObject:
		return true
	default:
		return false
	}
}

// Record is a normalized entity image. A complete record replaces the previous
// image: absent fields are not part of the current record.
type Record map[string]Value

func CloneRecord(record Record) Record {
	if record == nil {
		return nil
	}
	clone := make(Record, len(record))
	for field, value := range record {
		clone[field] = CloneValue(value)
	}
	return clone
}

// QuerySpec is deliberately source-neutral. Literals are typed engine values;
// adapters compile them without applying source-native implicit coercions.
// Order may be empty, in which case the adapter resolves it to a stable,
// unique ordering over the relation's entity identity.
type QuerySpec struct {
	Relation RelationName
	Filter   *Predicate
	Order    []OrderTerm
}

type RelationName struct {
	Namespace string
	Name      string
}

type Predicate struct {
	Comparison *Comparison
	Null       *NullTest
	Junction   *Junction
}

type ComparisonOperator uint8

const (
	ComparisonEqual ComparisonOperator = iota + 1
	ComparisonNotEqual
	ComparisonLess
	ComparisonLessOrEqual
	ComparisonGreater
	ComparisonGreaterOrEqual
)

type Comparison struct {
	Field    FieldPath
	Operator ComparisonOperator
	Value    Literal
}

type Literal struct {
	Type     ValueType
	Data     []byte
	DateTime time.Time
}

// LiteralFromValue copies a present scalar value into a query literal.
func LiteralFromValue(value Value) (Literal, error) {
	if value.State != ValuePresent || value.Type == TypeList || value.Type == TypeObject {
		return Literal{}, errors.New("query literal requires a present scalar value")
	}
	if err := value.ValidateComplete(); err != nil {
		return Literal{}, err
	}
	return Literal{Type: value.Type, Data: append([]byte(nil), value.Data...), DateTime: value.DateTime}, nil
}

// Value returns the literal as a present logical scalar.
func (literal Literal) Value() Value {
	return Value{Type: literal.Type, State: ValuePresent, Data: append([]byte(nil), literal.Data...), DateTime: literal.DateTime}
}

type NullTest struct {
	Field  FieldPath
	IsNull bool
}

type JunctionOperator uint8

const (
	JunctionAnd JunctionOperator = iota + 1
	JunctionOr
)

type Junction struct {
	Operator JunctionOperator
	Operands []*Predicate
}

type OrderTerm struct {
	Field      FieldPath
	Descending bool
	NullsFirst bool
}

// Fence identifies the change-feed point through which a snapshot must be
// reconciled. Native resume tokens stay adapter-private; Source and Order
// provide the engine's comparable progress coordinate.
type Fence interface {
	StorageFence()
	Source() SourceID
	// Order is a source-global, durable, monotonically increasing coordinate.
	// Positions are comparable only when Source returns the same identity.
	Order() uint64
	// StableID is an adapter-defined, deterministic serialization used for
	// diagnostics and keys. Ordering must use Order, never lexical StableID.
	StableID() string
}

type Checkpoint interface {
	StorageCheckpoint()
	Source() SourceID
	// Order is a source-global, durable, monotonically increasing coordinate.
	// Positions are comparable only when Source returns the same identity.
	Order() uint64
	// StableID is an adapter-defined, deterministic serialization used for
	// diagnostics and keys. Ordering must use Order, never lexical StableID.
	StableID() string
}

// TransactionMessage is opaque application metadata committed with a source
// transaction. Kind and payload semantics belong to higher layers, not the
// query engine. Adapters own the bytes and must not mutate them after delivery.
// An entirely empty message is an unusable-metadata placeholder. Adapters may
// replace oversized messages with it, and may coalesce an over-budget tail into
// one placeholder. Progress does not promise usable payloads for such messages.
type TransactionMessage struct {
	Kind    string
	Payload []byte
}
