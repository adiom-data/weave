// Package storage exposes supported contracts and forwards construction to internal implementations.
package storage

import (
	"time"

	impl "github.com/adiom-data/weave/internal/storage"
)

type ReferenceFetch = impl.ReferenceFetch
type ReferenceBinding = impl.ReferenceBinding

// MessageEvent contains only committed transaction metadata and source progress.
// Checkpoint covers every message through that position after the feed's start;
// an empty Messages slice also advances progress for row-only transactions and
// barriers. Messages (including empty placeholders) retain transaction order.
// No row changes, snapshot state, or acknowledgment capability are exposed.
type MessageEvent = impl.MessageEvent

type MessageFeed = impl.MessageFeed

// MessageSource is an independent, non-acking view of a source's metadata.
// OpenMessageFeed attaches at the current capture point, not historical replay,
// and must establish capture before returning. A lost interval terminates the
// feed with ErrResumeRequired; reopening starts a new history, never silently
// resumes an old downstream consumer. ID is stable for the source's lifetime.
type MessageSource = impl.MessageSource

// SourcePosition is the common ordering contract of checkpoints and fences.
// Native resume-token interpretation remains the adapter's responsibility.
type SourcePosition = impl.SourcePosition

// ComparePositions compares positions only within the same nonempty source.
// Zero is a valid coordinate; nil or foreign positions are errors, not bounds.
func ComparePositions(left, right SourcePosition) (int, error) {
	return impl.ComparePositions(left, right)
}

// CheckpointCovers is a pure comparison of an already-applied checkpoint and
// a snapshot fence. It never consults a feed, a queue, or adapter receive state.
func CheckpointCovers(checkpoint Checkpoint, fence Fence) (bool, error) {
	return impl.CheckpointCovers(checkpoint, fence)
}

var ErrResumeRequired = impl.ErrResumeRequired

var ErrIncompleteRecord = impl.ErrIncompleteRecord

type SourceID = impl.SourceID

// FieldPath is an exact sequence of object-field names. Its underlying encoded
// form is comparable so it can safely key routing maps; callers must use
// NewFieldPath when a path contains more than one segment. A plain string is
// retained as the canonical encoding of a one-segment path for compactness and
// backwards compatibility with source-neutral adapters.
type FieldPath = impl.FieldPath

func NewFieldPath(segments ...string) (FieldPath, error) { return impl.NewFieldPath(segments...) }

func MustFieldPath(segments ...string) FieldPath { return impl.MustFieldPath(segments...) }

// RelationKey is stable for the lifetime of a source relation. Native adapters
// should include an opaque relation/collection identity when names can be reused.
type RelationKey = impl.RelationKey

// EntityKey is the source's canonical encoding of a primary/document key.
type EntityKey = impl.EntityKey

type ValueState = impl.ValueState

const ValueMissing = impl.ValueMissing

const ValueNull = impl.ValueNull

const ValuePresent = impl.ValuePresent

const ValueUnavailable = impl.ValueUnavailable

// ValueType is one of the engine's datastore-independent logical types.
type ValueType = impl.ValueType

const TypeBool = impl.TypeBool

const TypeInteger = impl.TypeInteger

const TypeDecimal = impl.TypeDecimal

const TypeFloat = impl.TypeFloat

const TypeText = impl.TypeText

const TypeBytes = impl.TypeBytes

const TypeUUID = impl.TypeUUID

const TypeDateTime = impl.TypeDateTime

const TypeList = impl.TypeList

const TypeObject = impl.TypeObject

// Value is the engine's canonical logical representation. Byte-backed scalar
// types use Data, datetimes use DateTime, and List/Object contain recursive
// values. Native datastore encodings must not cross the adapter boundary.
type Value = impl.Value

// Record is a normalized entity image. A complete record replaces the previous
// image: absent fields are not part of the current record.
type Record = impl.Record

func CloneRecord(record Record) Record { return impl.CloneRecord(record) }

// QuerySpec is deliberately source-neutral. Literals are typed engine values;
// adapters compile them without applying source-native implicit coercions.
// Order may be empty, in which case the adapter resolves it to a stable,
// unique ordering over the relation's entity identity.
type QuerySpec = impl.QuerySpec

type RelationName = impl.RelationName

type Predicate = impl.Predicate

type ComparisonOperator = impl.ComparisonOperator

const ComparisonEqual = impl.ComparisonEqual

const ComparisonNotEqual = impl.ComparisonNotEqual

const ComparisonLess = impl.ComparisonLess

const ComparisonLessOrEqual = impl.ComparisonLessOrEqual

const ComparisonGreater = impl.ComparisonGreater

const ComparisonGreaterOrEqual = impl.ComparisonGreaterOrEqual

type Comparison = impl.Comparison

type Literal = impl.Literal

// LiteralFromValue copies a present scalar value into a query literal.
func LiteralFromValue(value Value) (Literal, error) { return impl.LiteralFromValue(value) }

type NullTest = impl.NullTest

type JunctionOperator = impl.JunctionOperator

const JunctionAnd = impl.JunctionAnd

const JunctionOr = impl.JunctionOr

type Junction = impl.Junction

type OrderTerm = impl.OrderTerm

// Fence identifies the change-feed point through which a snapshot must be
// reconciled. Native resume tokens stay adapter-private; Source and Order
// provide the engine's comparable progress coordinate.
type Fence = impl.Fence

type Checkpoint = impl.Checkpoint

// TransactionMessage is opaque application metadata committed with a source
// transaction. Kind and payload semantics belong to higher layers, not the
// query engine. Adapters own the bytes and must not mutate them after delivery.
// An entirely empty message is an unusable-metadata placeholder. Adapters may
// replace oversized messages with it, and may coalesce an over-budget tail into
// one placeholder. Progress does not promise usable payloads for such messages.
type TransactionMessage = impl.TransactionMessage

// Canonicalize converts source text into the engine representation for a
// supported scalar type. The returned bytes are stable for equal values.
func Canonicalize(valueType ValueType, data []byte) ([]byte, error) {
	return impl.Canonicalize(valueType, data)
}

// ParseScalar converts a source-neutral textual scalar into a logical value.
// Datastore adapters should prefer constructing datetimes from their native
// temporal values and calling NewDateTime.
func ParseScalar(valueType ValueType, data []byte) (Value, error) {
	return impl.ParseScalar(valueType, data)
}

// ParseDateTime accepts the source-neutral textual forms used by tests and
// non-protobuf callers. The public protobuf API uses google.protobuf.Timestamp.
func ParseDateTime(text string) (time.Time, error) { return impl.ParseDateTime(text) }

// NewDateTime constructs a canonical finite UTC datetime. Range restrictions
// belong to datastore and wire adapters, not to the storage-neutral model.
func NewDateTime(instant time.Time) Value { return impl.NewDateTime(instant) }

func NormalizeDateTime(instant time.Time) time.Time { return impl.NormalizeDateTime(instant) }

func IsCanonicalDateTime(instant time.Time) bool { return impl.IsCanonicalDateTime(instant) }

func CloneValue(value Value) Value { return impl.CloneValue(value) }

func EqualValues(left, right Value) bool { return impl.EqualValues(left, right) }

// Lookup resolves an exact object-only path. List positions are deliberately
// not part of the query model.
func Lookup(record Record, path FieldPath) (Value, bool) { return impl.Lookup(record, path) }

// SetPath materializes one selected object path into a record. Intermediate
// objects are created as needed; list traversal is intentionally unsupported.
func SetPath(record Record, path FieldPath, value Value) error {
	return impl.SetPath(record, path, value)
}

// EvaluatePredicate applies the engine's type-strict, source-independent
// predicate semantics. Different types are unequal and never ordered.
func EvaluatePredicate(predicate *Predicate, record Record) (bool, error) {
	return impl.EvaluatePredicate(predicate, record)
}

// CompareValues implements the ordering of canonical scalar values. Values of
// different types are deliberately incomparable; query preparation must make
// predicate endpoints and relation fields agree.
func CompareValues(left, right Value) (int, bool) { return impl.CompareValues(left, right) }
