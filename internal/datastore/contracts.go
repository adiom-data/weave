// Package datastore defines private contracts between storage adapters and the engine.
package datastore

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/storage"
)

var (
	ErrNotFound    = errors.New("entity not found")
	ErrInvalidated = errors.New("relation invalidated")
)

type QueryFingerprint string

// AuthoritativeRecord is a complete entity/document image at the source
// snapshot that produced it. Absence of a field is authoritative: consumers
// must remove any cached field that is not present. Adapters must never wrap a
// projection, patch, or source-native partial image in this type.
type AuthoritativeRecord struct {
	Record storage.Record
}

// Validate rejects source-native partial values at the adapter boundary. A
// complete record may omit a field (that omission is authoritative), but a
// field it does contain must have a definite present or null state.
func (record AuthoritativeRecord) Validate() error {
	if record.Record == nil {
		return errors.New("authoritative record is nil")
	}
	for field, value := range record.Record {
		if value.State != storage.ValuePresent && value.State != storage.ValueNull {
			return fmt.Errorf("authoritative field %q has incomplete state %d", field, value.State)
		}
		if err := value.ValidateComplete(); err != nil {
			return fmt.Errorf("authoritative field %q: %w", field, err)
		}
	}
	return nil
}

// SortKey is opaque to the coordinator. A prepared query may retain a
// datastore-efficient representation, but comparison must reproduce the
// engine's logical ordering semantics exactly. Keys are immutable after return:
// the engine may retain them in indexes and use them concurrently. Adapters
// must not return views into input records or reusable scratch storage.
type SortKey interface {
	StorageSortKey()
}

// EncodedSortKey is useful for adapters whose canonical key can be represented
// as authenticated bytes. More complex adapters may use their own implementation.
type EncodedSortKey []byte

func (EncodedSortKey) StorageSortKey() {}

type BoundaryKind uint8

const (
	BoundaryStart BoundaryKind = iota
	BoundaryCursor
	BoundaryEnd
)

type Boundary struct {
	Kind BoundaryKind
	Key  SortKey
}

type Bounds struct {
	Left  Boundary
	Right Boundary
}

type WindowRequest struct {
	Bounds  Bounds
	Limit   int
	Reverse bool
}

// PreparedQuery is an immutable, concurrency-safe evaluation plan. All methods
// perform in-memory work only: no database/network IO, waiting on external
// progress, or mutation of input records/keys. PrepareQuery may perform IO and
// must finish normalization/metadata discovery before publishing the plan.
// Methods may be called concurrently by CDC, snapshot workers, and consumers.
type PreparedQuery interface {
	// Fingerprint identifies both immutable query semantics and the native
	// relation incarnation. Recreating a table/collection must change it even
	// when the qualified name and query text are unchanged.
	Fingerprint() QueryFingerprint
	Relation() storage.RelationKey
	EntityKey(storage.Record) (storage.EntityKey, error)
	// Matches and Key return storage.ErrIncompleteRecord when a required normalized
	// field is unavailable. That lets the engine defer only the undecidable
	// query effect without exposing adapter-specific patch semantics. Key and
	// Compare define the requested cursor order. Compare may return zero for
	// distinct entities with equal requested sort values; such entities share an
	// ambiguous cursor bucket and complete pagination within that bucket is not
	// guaranteed. The engine privately distinguishes observed members by entity.
	Matches(storage.Record) (bool, error)
	Key(storage.Record) (SortKey, error)
	Compare(SortKey, SortKey) (int, error)
	// EncodeKey returns caller-owned bytes. DecodeKey must not retain mutable
	// input bytes; both it and Key return stable, immutable sort keys.
	EncodeKey(SortKey) ([]byte, error)
	DecodeKey([]byte) (SortKey, error)
	// Query returns the canonical query model prepared by the adapter, including
	// any stable identity ordering resolved for an omitted Order. The engine
	// derives routing registrations from it; adapters do not know about routing
	// indexes.
	// The returned definition is caller-owned; changing it must not alter the
	// prepared plan, its fingerprint, or future evaluations.
	Query() storage.QuerySpec
}

type SnapshotSession interface {
	// All reads use one consistent source snapshot for Relation and return whole,
	// authoritative entities. Reads for a projection spanning other relations use
	// independent sessions and are not promised to share a common point in time.
	Relation() storage.RelationKey
	FetchEntity(context.Context, storage.EntityKey) (AuthoritativeRecord, error)
	// LoadWindowKeys returns only ordered cursor keys and must avoid loading
	// entity payload columns. Results are in nondecreasing query order, remain inside
	// request.Bounds, and contain at most request.Limit keys even when Reverse
	// asks the datastore to select rows from the right edge.
	LoadWindowKeys(context.Context, PreparedQuery, WindowRequest) ([]SortKey, error)
	// LoadWindow returns at most request.Limit matching authoritative records in
	// nondecreasing query order and inside request.Bounds. Callers may continue a
	// forward scan by moving the open left bound to the final returned key within
	// the same snapshot session. If the limit splits an equal-key bucket, that
	// continuation may omit remaining members of the bucket.
	LoadWindow(context.Context, PreparedQuery, WindowRequest) ([]AuthoritativeRecord, error)
	// Finish closes the snapshot and returns a source fence at or after its
	// observation point. Once the same source feed covers that fence, applying
	// the intervening committed batches makes these rows safe to publish.
	Finish(context.Context) (storage.Fence, error)
	// Finish and Abort are terminal; no method may be used afterward.
	Abort(context.Context) error
}

type ChangeKind uint8

const (
	ChangeInsert ChangeKind = iota + 1
	ChangeUpdate
	ChangeDelete
	// ChangeClearRelation atomically removes every entity in one logical
	// relation. Adapters may map native operations such as PostgreSQL TRUNCATE
	// or an equivalent collection-wide clear to this datastore-neutral effect.
	ChangeClearRelation
	ChangeInvalidate
)

type AppliedKind uint8

const (
	AppliedDeleted AppliedKind = iota + 1
	AppliedComplete
	AppliedPartial
)

// AppliedChange is a tagged source transition. Deleted carries no record;
// Complete carries an authoritative whole entity; Partial carries a normalized
// image that must be fenced and hydrated before it is authoritative.
type AppliedChange struct {
	Kind   AppliedKind
	Key    storage.EntityKey
	Record storage.Record
}

func (a AppliedChange) Exists() bool {
	return a.Kind == AppliedComplete || a.Kind == AppliedPartial
}

func (a AppliedChange) Complete() bool { return a.Kind == AppliedComplete }

// Change owns native patch semantics. For example, PostgreSQL merges unchanged
// TOAST fields while MongoDB applies updateDescription deltas. AppliedPartial
// means storage.Record is normalized but not authoritative and must be hydrated before
// it can be exposed as a complete entity payload.
// A delivered Change and its native payload are immutable. Methods may be
// called concurrently by the CDC consumer and snapshot preparation workers.
type Change interface {
	Kind() ChangeKind
	Relation() storage.RelationKey
	// BeforeKey resolves the source-native identity used to locate the cached
	// value preceding this change. Resolution is deliberately fallible and is
	// performed by the processor, after a committed batch has been partitioned
	// by relation, so a malformed identity can be isolated to that relation.
	BeforeKey() (storage.EntityKey, error)
	// AfterKey resolves the resulting identity without materializing the row.
	// For a deletion it returns BeforeKey; relation-wide changes have no key.
	// Capture/routing may call it before Apply, including for partial tuples.
	AfterKey() (storage.EntityKey, error)
	Apply(previous storage.Record) (AppliedChange, error)
}

type CommittedChanges struct {
	Changes []Change
	// Messages preserve source order within the committed transaction, including
	// transactions with no row changes. Their position is storage.Checkpoint, not the
	// position at which an uncommitted message was originally written.
	Messages []storage.TransactionMessage
	// SourceOrdinals optionally retains each change's one-based position in a
	// larger source transaction after a batch has been partitioned. A source feed
	// normally leaves it empty because Changes is already in source order. When
	// supplied by any caller, the coordinator preserves it. It must have the same
	// length as Changes and contain positive, strictly increasing values.
	SourceOrdinals []uint32
	Checkpoint     storage.Checkpoint
	Atomic         bool
}

// ChangeEvent is one ordered item from a source feed. A transaction carries a
// complete committed source unit; progress proves that no transaction at or
// before its checkpoint can appear later. Exactly one field must be set.
type ChangeEvent struct {
	Transaction *CommittedChanges
	Progress    storage.Checkpoint
}

func TransactionEvent(transaction CommittedChanges) ChangeEvent {
	return ChangeEvent{Transaction: &transaction}
}

func ProgressEvent(position storage.Checkpoint) ChangeEvent {
	return ChangeEvent{Progress: position}
}

type ChangeFeed interface {
	// Receive returns source transactions and authoritative progress as distinct
	// ordered events and must unblock promptly when its context is cancelled. It
	// is called by one consumer at a time; adapters need not support concurrent
	// Receive calls.
	Receive(context.Context) (ChangeEvent, error)
	// Covers reports whether this feed has received source progress at or beyond
	// a same-source fence. It must never infer coverage merely from an empty or
	// momentarily drained transport queue. This is not an engine application
	// watermark: prefetching consumers must track applied progress themselves.
	Covers(storage.Fence) bool
	// Ack may run concurrently with Receive. It is called only after the engine
	// has applied and published a complete batch, including authoritative
	// progress-only batches. It authorizes the
	// adapter to advance its durable consumer checkpoint. An adapter may replay
	// acknowledged work, but must never resume after unacknowledged work.
	Ack(context.Context, storage.Checkpoint) error
	Close() error
}

// ProgressRequester is an optional data-source capability for sparse logical
// streams. The engine calls it only after normal feed activity has failed to
// cover a pending snapshot fence. Implementations must inject an ordered,
// authoritative source event; the request itself never establishes coverage.
// It may be called concurrently with change-feed Receive.
//
// Every DataSource must either make fences eventually coverable through
// natural, position-bearing feed activity even when no rows change, or
// implement ProgressRequester. Otherwise quiet-source snapshot initialization
// cannot make progress.
type ProgressRequester interface {
	RequestProgress(context.Context, storage.Fence) error
}

// ReadFenceDecoder validates an opaque application fence against this source.
// It must reject unreachable/future positions before they enter the shared
// progress loop. It may perform IO, outside the coordinator actor.
type ReadFenceDecoder interface {
	DecodeReadFence(context.Context, string) (storage.Fence, error)
}

type DataSource interface {
	ID() storage.SourceID
	// PrepareQuery resolves an omitted order to the datastore's stable, unique
	// entity-identity order before returning the PreparedQuery.
	PrepareQuery(context.Context, storage.QuerySpec) (PreparedQuery, error)
	// BeginSnapshot uses adapter-owned relation metadata carried by a prepared
	// query. Callers must not reconstruct native schema metadata from storage.RelationKey.
	// The returned session may serve other prepared queries for the same relation.
	BeginSnapshot(context.Context, PreparedQuery) (SnapshotSession, error)
	// OpenFeed resumes from this adapter's durable consumer checkpoint. Its
	// storage is adapter-owned and may be external to the source database.
	// OpenFeed does not return until the durable capture point exists; snapshots
	// may begin immediately after it succeeds without missing a later progress
	// marker during source initialization.
	OpenFeed(context.Context) (ChangeFeed, error)
}

// Storage is the constructed source boundary consumed by a query server.
// Source capture and resource lifetime belong to its creator, not the server.
type Storage interface {
	DataSource
	storage.MessageSource
	Close() error
	Wait(context.Context) error
}
