// Package querysubscription exposes supported contracts and forwards construction to internal implementations.
package querysubscription

import (
	impl "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/pkg/storage"
)

var ErrBackpressure = impl.ErrBackpressure

var ErrClosed = impl.ErrClosed

// Position is a source-qualified, globally ordered progress coordinate. Order
// is meaningful only among positions carrying the same SourceID. Native resume
// tokens remain owned by the datastore adapter and never cross this boundary.
type Position = impl.Position

func SamePosition(left, right *Position) bool { return impl.SamePosition(left, right) }

func ComparePositions(left, right *Position) (int, error) { return impl.ComparePositions(left, right) }

type InitialWindow = impl.InitialWindow

type ReferenceSource = impl.ReferenceSource

type BoundaryKind = impl.BoundaryKind

const BoundaryStart = impl.BoundaryStart

const BoundaryCursor = impl.BoundaryCursor

const BoundaryEnd = impl.BoundaryEnd

type Boundary = impl.Boundary

type Bounds = impl.Bounds

type BoundarySide = impl.BoundarySide

const BoundaryLeft = impl.BoundaryLeft

const BoundaryRight = impl.BoundaryRight

type BoundaryOperation = impl.BoundaryOperation

const BoundaryMove = impl.BoundaryMove

const BoundaryExtend = impl.BoundaryExtend

const BoundaryContract = impl.BoundaryContract

type BoundaryChange = impl.BoundaryChange

type ChangeKind = impl.ChangeKind

const Added = impl.Added

const Updated = impl.Updated

const Removed = impl.Removed

// RecordVersion identifies an immutable row image within one feed. It is
// separate from the transaction that publishes or authorizes that image.
// Versions from different feeds must not be compared. Zero is a valid bootstrap.
type RecordVersion = impl.RecordVersion

type Row = impl.Row

type Change = impl.Change

type Insertion = impl.Insertion

// MembershipDelta transforms the previous ordered membership by removing all
// Removed keys and then inserting Insertions at their final indexes.
type MembershipDelta = impl.MembershipDelta

// ApplyMembership applies removals first and then insertions in final-index
// order. It rejects malformed deltas rather than silently corrupting a
// consumer's ordered model.
func ApplyMembership(order []storage.EntityKey, delta MembershipDelta) ([]storage.EntityKey, error) {
	return impl.ApplyMembership(order, delta)
}

type DeliveryKind = impl.DeliveryKind

const DeliveryCommit = impl.DeliveryCommit

const DeliveryHydration = impl.DeliveryHydration

const DeliveryWindow = impl.DeliveryWindow

// DeliveryMetadata identifies one complete feed delivery. Revision and
// PreviousRevision are local to one feed; Checkpoint identifies a source
// commit across feeds consuming the same source. A zero Kind means the batch
// is not part of the ordered feed chain.
type DeliveryMetadata = impl.DeliveryMetadata

type WindowTransition = impl.WindowTransition

type Batch = impl.Batch

// SubscriptionEffect is one subscription's contribution to a complete feed
// delivery. A source transaction may have no effects for this feed and still
// be delivered so outer state such as authorization advances atomically.
type SubscriptionEffect = impl.SubscriptionEffect

type DeliveryEnvelope = impl.DeliveryEnvelope

// Progress closes the feed's source history through Position: no transaction
// at or before that position may be emitted later. Revision places it in the
// same feed-local predecessor chain as deliveries.
type Progress = impl.Progress

type SubscriptionFailure = impl.SubscriptionFailure

// Event is the ordered output of one feed. Exactly one field is populated.
type Event = impl.Event

type QuerySnapshot = impl.QuerySnapshot
type QueryRow = impl.QueryRow
type ReferenceRoute = impl.ReferenceRoute
type EntityTarget = impl.EntityTarget
type EntityDelta = impl.EntityDelta
type EntityEffect = impl.EntityEffect
type EntityInstallation = impl.EntityInstallation

type QuerySubscribeRequest = impl.QuerySubscribeRequest

// EntitySubscribeRequest exposes at most one row. Complete primary-key
// equalities permit direct entity lookup; other predicates use a count-one
// query and select the first covered candidate in query order. If another
// candidate becomes first, removal and addition share one delivery.
type EntitySubscribeRequest = impl.EntitySubscribeRequest

// ValidateQuerySubscribeRequest checks the transport-independent request shape.
// Datastore-specific query validity remains the implementation's concern.
func ValidateQuerySubscribeRequest(request QuerySubscribeRequest) error {
	return impl.ValidateQuerySubscribeRequest(request)
}

func ValidateEntitySubscribeRequest(request EntitySubscribeRequest) error {
	return impl.ValidateEntitySubscribeRequest(request)
}

// FeedOptions configure one consumer feed. BufferLimit is an
// implementation-independent backpressure bound, not a query property.
type FeedOptions = impl.FeedOptions

// Subscription is the common lifecycle handle. Live effects are delivered by
// Feed.Next so a source transaction remains grouped across subscriptions.
type Subscription = impl.Subscription

// QuerySubscription adds mutable cursor-window control to a subscription.
type QuerySubscription = impl.QuerySubscription

// EntitySubscription intentionally exposes no window control.
type EntitySubscription = impl.EntitySubscription

// QuerySnapshotInstaller installs a subscription and its authoritative snapshot
// before a delivery containing that subscription becomes visible through
// Feed.Next. Complete delivery envelopes are staged so transactions spanning
// existing and installing subscriptions remain atomic. Implementations close
// the subscription and return the installer error when installation fails.
type QuerySnapshotInstaller = impl.QuerySnapshotInstaller

// EntitySnapshot contains the zero-or-one result of an entity selector.
// Entity effects use Batch.Changes; query membership, cursors, and window
// transitions are removed by the implementation boundary.
type EntitySnapshot = impl.EntitySnapshot

type EntitySnapshotInstaller = impl.EntitySnapshotInstaller

// Feed scopes subscription identifiers, ordered source progress, and
// lifecycle. A future remote implementation can map one Feed to one
// bidirectional transport without exposing engine groups or coordinator state.
// Subscription IDs are unique for the complete feed lifetime and are
// consumed when a well-formed subscribe operation begins, even if that attempt
// later fails. They cannot be reused after Close; this keeps canceled and retired
// remote delivery metadata unambiguous. All methods must be safe for concurrent
// use: projection expansion can subscribe children while existing subscriptions
// deliver.
type Feed = impl.Feed

type Client = impl.Client
