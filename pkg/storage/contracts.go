// Package storage exposes supported contracts and forwards construction to internal implementations.
package storage

import (
	impl "github.com/adiom-data/weave/internal/datastore"
)

var ErrNotFound = impl.ErrNotFound

var ErrInvalidated = impl.ErrInvalidated

type QueryFingerprint = impl.QueryFingerprint

// AuthoritativeRecord is a complete entity/document image at the source
// snapshot that produced it. Absence of a field is authoritative: consumers
// must remove any cached field that is not present. Adapters must never wrap a
// projection, patch, or source-native partial image in this type.
type AuthoritativeRecord = impl.AuthoritativeRecord

// SortKey is opaque to the coordinator. A prepared query may retain a
// datastore-efficient representation, but comparison must reproduce the
// engine's logical ordering semantics exactly. Keys are immutable after return:
// the engine may retain them in indexes and use them concurrently. Adapters
// must not return views into input records or reusable scratch
type SortKey = impl.SortKey

// EncodedSortKey is useful for adapters whose canonical key can be represented
// as authenticated bytes. More complex adapters may use their own implementation.
type EncodedSortKey = impl.EncodedSortKey

type BoundaryKind = impl.BoundaryKind

const BoundaryStart = impl.BoundaryStart

const BoundaryCursor = impl.BoundaryCursor

const BoundaryEnd = impl.BoundaryEnd

type Boundary = impl.Boundary

type Bounds = impl.Bounds

type WindowRequest = impl.WindowRequest

// PreparedQuery is an immutable, concurrency-safe evaluation plan. All methods
// perform in-memory work only: no database/network IO, waiting on external
// progress, or mutation of input records/keys. PrepareQuery may perform IO and
// must finish normalization/metadata discovery before publishing the plan.
// Methods may be called concurrently by CDC, snapshot workers, and consumers.
type PreparedQuery = impl.PreparedQuery

// Optional exact-identity specialization. General predicates remain supported
// through PreparedQuery when the adapter cannot prove a complete identity.
type IdentityPreparer = impl.IdentityPreparer
type IdentityQuery = impl.IdentityQuery

type SnapshotSession = impl.SnapshotSession

// Optional one-level reference-loading contracts for datastore adapters.
type PreparedReference = impl.PreparedReference
type ReferenceLookup = impl.ReferenceLookup
type PreparedReferenceFetch = impl.ReferenceFetch
type ReferencePreparer = impl.ReferencePreparer
type ReferenceWindowReader = impl.ReferenceWindowReader
type ReferenceWindowRow = impl.ReferenceWindowRow

type ChangeKind = impl.ChangeKind

const ChangeInsert = impl.ChangeInsert

const ChangeUpdate = impl.ChangeUpdate

const ChangeDelete = impl.ChangeDelete

const ChangeClearRelation = impl.ChangeClearRelation

const ChangeInvalidate = impl.ChangeInvalidate

type AppliedKind = impl.AppliedKind

const AppliedDeleted = impl.AppliedDeleted

const AppliedComplete = impl.AppliedComplete

const AppliedPartial = impl.AppliedPartial

// AppliedChange is a tagged source transition. Deleted carries no record;
// Complete carries an authoritative whole entity; Partial carries a normalized
// image that must be fenced and hydrated before it is authoritative.
type AppliedChange = impl.AppliedChange

// Change owns native patch semantics. For example, PostgreSQL merges unchanged
// TOAST fields while MongoDB applies updateDescription deltas. AppliedPartial
// means Record is normalized but not authoritative and must be hydrated before
// it can be exposed as a complete entity payload.
type Change = impl.Change

type CommittedChanges = impl.CommittedChanges

// ChangeEvent is one ordered item from a source feed. A transaction carries a
// complete committed source unit; progress proves that no transaction at or
// before its checkpoint can appear later. Exactly one field must be set.
type ChangeEvent = impl.ChangeEvent

func TransactionEvent(transaction CommittedChanges) ChangeEvent {
	return impl.TransactionEvent(transaction)
}

func ProgressEvent(position Checkpoint) ChangeEvent { return impl.ProgressEvent(position) }

type ChangeFeed = impl.ChangeFeed

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
type ProgressRequester = impl.ProgressRequester

// ReadFenceDecoder optionally validates opaque post-commit tokens for a source.
type ReadFenceDecoder = impl.ReadFenceDecoder

type DataSource = impl.DataSource

// Storage is the constructed source boundary consumed by a query server.
// Source capture and resource lifetime belong to its creator, not the server.
type Storage = impl.Storage

type MetadataManager = impl.MetadataManager
