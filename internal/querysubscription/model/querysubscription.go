// Package querysubscription defines the boundary between consumers of live
// entity/query subscriptions and the in-process reactive engine. The contract
// is phrased in records, opaque cursors, and ordered membership deltas so an
// implementation may later live behind a network transport.
package querysubscription

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/adiom-data/weave/internal/storage"
)

var (
	// ErrBackpressure means the consumer could not keep up with its bounded
	// subscription stream and must obtain a fresh snapshot.
	ErrBackpressure = errors.New("query subscription buffer is full; resynchronization required")
	// ErrClosed is returned after a subscription or projection is closed
	// normally. Consumers should not report it as a synchronization failure.
	ErrClosed = errors.New("query subscription is closed")
)

// Position is a source-qualified, globally ordered progress coordinate. Order
// is meaningful only among positions carrying the same SourceID. Native resume
// tokens remain owned by the datastore adapter and never cross this boundary.
type Position struct {
	SourceID storage.SourceID
	Value    uint64
}

func (p *Position) Source() storage.SourceID {
	if p == nil {
		return ""
	}
	return p.SourceID
}

func (p *Position) StableID() string {
	if p == nil {
		return ""
	}
	return fmt.Sprint(p.Value)
}

func (p *Position) Order() uint64 {
	if p == nil {
		return 0
	}
	return p.Value
}

func (*Position) StorageCheckpoint() {}
func (*Position) StorageFence()      {}

func SamePosition(left, right *Position) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.SourceID == right.SourceID && left.Value == right.Value
}

func ComparePositions(left, right *Position) (int, error) {
	if left == nil || right == nil {
		return 0, errors.New("positions are required")
	}
	return storage.ComparePositions(left, right)
}

// SnapshotPosition is the freshness floor of an image. A consumed checkpoint
// cannot weaken a newer snapshot fence. It is not proof of feed progress.
func SnapshotPosition(checkpoint, fence *Position) (*Position, error) {
	position := checkpoint
	if position == nil {
		position = fence
	}
	if position == nil || position.SourceID == "" {
		return nil, errors.New("snapshot requires a source position")
	}
	if checkpoint != nil && fence != nil {
		compared, err := ComparePositions(checkpoint, fence)
		if err != nil {
			return nil, err
		}
		if compared < 0 {
			position = fence
		}
	}
	copy := *position
	return &copy, nil
}

type InitialWindow struct {
	FromEnd bool
	Count   int
}

type BoundaryKind uint8

const (
	BoundaryStart BoundaryKind = iota + 1
	BoundaryCursor
	BoundaryEnd
)

type Boundary struct {
	Kind   BoundaryKind
	Cursor []byte
}

type Bounds struct {
	Left  Boundary
	Right Boundary
}

type BoundarySide uint8

const (
	BoundaryLeft BoundarySide = iota + 1
	BoundaryRight
)

type BoundaryOperation uint8

const (
	BoundaryMove BoundaryOperation = iota + 1
	BoundaryExtend
	BoundaryContract
)

type BoundaryChange struct {
	Side      BoundarySide
	Operation BoundaryOperation
	Count     int
	Cursor    []byte
}

type ChangeKind uint8

const (
	Added ChangeKind = iota + 1
	Updated
	Removed
)

// RecordVersion identifies an immutable row image within one feed. It is
// separate from the transaction that publishes or authorizes that image.
// Versions from different feeds must not be compared. Zero is a valid bootstrap.
type RecordVersion struct {
	Revision    uint64
	ChangeIndex uint32
}

type Row struct {
	Key         storage.EntityKey
	Record      storage.Record
	Pending     bool
	Cursor      []byte
	ChangeIndex uint32
}

type Change struct {
	Kind ChangeKind
	Row
}

type Insertion struct {
	Key        storage.EntityKey
	FinalIndex int
}

// MembershipDelta transforms the previous ordered membership by removing all
// Removed keys and then inserting Insertions at their final indexes.
type MembershipDelta struct {
	Removed    []storage.EntityKey
	Insertions []Insertion
}

// ApplyMembership applies removals first and then insertions in final-index
// order. It rejects malformed deltas rather than silently corrupting a
// consumer's ordered model.
func ApplyMembership(order []storage.EntityKey, delta MembershipDelta) ([]storage.EntityKey, error) {
	present := make(map[storage.EntityKey]struct{}, len(order)+len(delta.Insertions))
	for _, key := range order {
		if _, duplicate := present[key]; duplicate {
			return nil, errors.New("ordered membership contains a duplicate key")
		}
		present[key] = struct{}{}
	}
	removed := make(map[storage.EntityKey]struct{}, len(delta.Removed))
	for _, key := range delta.Removed {
		if _, duplicate := removed[key]; duplicate {
			return nil, errors.New("membership delta removes a key more than once")
		}
		if _, exists := present[key]; !exists {
			return nil, errors.New("membership delta removes an absent key")
		}
		removed[key] = struct{}{}
		delete(present, key)
	}
	remaining := make([]storage.EntityKey, 0, len(order))
	for _, key := range order {
		if _, drop := removed[key]; !drop {
			remaining = append(remaining, key)
		}
	}
	finalLength := len(remaining) + len(delta.Insertions)
	previousIndex := -1
	for _, insertion := range delta.Insertions {
		if insertion.FinalIndex < 0 || insertion.FinalIndex >= finalLength {
			return nil, errors.New("membership delta insertion index is out of bounds")
		}
		if insertion.FinalIndex <= previousIndex {
			return nil, errors.New("membership delta insertions are not in strictly increasing final-index order")
		}
		if _, duplicate := present[insertion.Key]; duplicate {
			return nil, errors.New("membership delta inserts an existing key")
		}
		present[insertion.Key] = struct{}{}
		previousIndex = insertion.FinalIndex
	}
	result := make([]storage.EntityKey, 0, finalLength)
	remainingIndex, insertionIndex := 0, 0
	for finalIndex := 0; finalIndex < finalLength; finalIndex++ {
		if insertionIndex < len(delta.Insertions) && delta.Insertions[insertionIndex].FinalIndex == finalIndex {
			result = append(result, delta.Insertions[insertionIndex].Key)
			insertionIndex++
			continue
		}
		if remainingIndex >= len(remaining) {
			return nil, errors.New("membership delta does not describe a complete final ordering")
		}
		result = append(result, remaining[remainingIndex])
		remainingIndex++
	}
	return result, nil
}

type DeliveryKind uint8

const (
	DeliveryCommit DeliveryKind = iota + 1
	DeliveryHydration
	DeliveryWindow
)

// DeliveryMetadata identifies one complete feed delivery. Revision and
// PreviousRevision are local to one feed; Checkpoint identifies a source
// commit across feeds consuming the same source. A zero Kind means the batch
// is not part of the ordered feed chain.
type DeliveryMetadata struct {
	Kind             DeliveryKind
	Checkpoint       *Position
	Fence            *Position
	Atomic           bool
	Revision         uint64
	PreviousRevision uint64
}

// Validate rejects malformed feed ordering metadata.
func (metadata DeliveryMetadata) Validate() error {
	if metadata.Kind == 0 {
		if metadata.Revision != 0 || metadata.PreviousRevision != 0 || metadata.Atomic || metadata.Checkpoint != nil || metadata.Fence != nil {
			return errors.New("delivery metadata without a kind must be empty")
		}
		return nil
	}
	if metadata.Kind != DeliveryCommit && metadata.Kind != DeliveryHydration && metadata.Kind != DeliveryWindow {
		return errors.New("delivery metadata has an unknown kind")
	}
	if metadata.Revision == 0 {
		return errors.New("delivery metadata requires a revision")
	}
	if metadata.PreviousRevision >= metadata.Revision && metadata.PreviousRevision != 0 {
		return errors.New("delivery predecessor must be earlier than its revision")
	}
	if metadata.Kind == DeliveryCommit && metadata.Checkpoint == nil {
		return errors.New("source transaction delivery requires a position")
	}
	return nil
}

type WindowTransition struct {
	Token      string
	Bounds     Bounds
	Fence      *Position
	Checkpoint *Position
}

type Batch struct {
	Clear            bool
	ClearChangeIndex uint32
	Changes          []Change
	Membership       MembershipDelta
	Window           *WindowTransition
	// BindingResets invalidate reusable images for these parent rows. They
	// change reference ownership, not the referenced entity's data.
	BindingResets []storage.EntityKey
}

// ReferenceRoute associates an entity selection with rows in its owning query.
// Selector prevents late images from an old binding replacing a newer one.
// It is routing metadata, not a different entity payload or subscription ID.
type ReferenceRoute struct {
	Selector string
	Parents  []storage.EntityKey
}

// EntityTarget addresses an explicit entity subscription, or a parent-owned
// interest when Reference is set. SubscriptionID names the owner in either case.
type EntityTarget struct {
	SubscriptionID string
	Reference      *ReferenceRoute
}

func (target EntityTarget) Validate() error {
	if target.SubscriptionID == "" {
		return errors.New("entity target subscription ID is required")
	}
	if route := target.Reference; route != nil && (route.Selector == "" || len(route.Parents) == 0) {
		return errors.New("reference route requires a selector and parent bindings")
	}
	return nil
}

// EntityDelta is the common zero-or-one selection update, independent of
// whether the selection is explicit or owned by a query window.
type EntityDelta struct {
	Clear            bool
	ClearChangeIndex uint32
	Changes          []Change
}

type EntityEffect struct {
	Target EntityTarget
	Delta  EntityDelta
}

// EntityInstallation carries a standalone authoritative image and its routing.
// It is never proof of source progress or a synthetic source transaction.
type EntityInstallation struct {
	Target   EntityTarget
	Snapshot EntitySnapshot
}

// SubscriptionEffect is one subscription's contribution to a complete feed
// delivery. A source transaction may have no effects for this feed and still
// be delivered so outer state such as authorization advances atomically.
type SubscriptionEffect struct {
	SubscriptionID string
	Batch          Batch
}

type DeliveryEnvelope struct {
	Delivery DeliveryMetadata
	// Effects contains query/list contributions. Entities contains the same
	// transaction's explicit and parent-owned entity contributions.
	Effects  []SubscriptionEffect
	Entities []EntityEffect
}

// SubscriptionEffects visits query effects and explicit entity effects through
// the shared row-processing shape. Parent-owned routing is handled by the query
// client before projection materialization; it is not a projection dependency.
func (envelope DeliveryEnvelope) SubscriptionEffects() iter.Seq[SubscriptionEffect] {
	return func(yield func(SubscriptionEffect) bool) {
		for _, effect := range envelope.Effects {
			if !yield(effect) {
				return
			}
		}
		for _, effect := range envelope.Entities {
			if effect.Target.Reference != nil {
				continue
			}
			delta := effect.Delta
			if !yield(SubscriptionEffect{SubscriptionID: effect.Target.SubscriptionID, Batch: Batch{Clear: delta.Clear, ClearChangeIndex: delta.ClearChangeIndex, Changes: delta.Changes}}) {
				return
			}
		}
	}
}

func (envelope DeliveryEnvelope) Validate() error {
	if err := envelope.Delivery.Validate(); err != nil {
		return err
	}
	if envelope.Delivery.Kind == 0 {
		return errors.New("delivery envelope requires ordered metadata")
	}
	seen := make(map[string]struct{}, len(envelope.Effects))
	for _, effect := range envelope.Effects {
		if effect.SubscriptionID == "" {
			return errors.New("subscription effect ID is required")
		}
		if _, duplicate := seen[effect.SubscriptionID]; duplicate {
			return errors.New("delivery contains duplicate subscription effects")
		}
		seen[effect.SubscriptionID] = struct{}{}
	}
	for _, effect := range envelope.Entities {
		if err := effect.Target.Validate(); err != nil {
			return err
		}
		if effect.Target.Reference == nil {
			if _, duplicate := seen[effect.Target.SubscriptionID]; duplicate {
				return errors.New("delivery contains duplicate subscription effects")
			}
			seen[effect.Target.SubscriptionID] = struct{}{}
		}
	}
	return nil
}

// Progress closes the feed's source history through Position: no transaction
// at or before that position may be emitted later. Revision places it in the
// same feed-local predecessor chain as deliveries.
type Progress struct {
	Position         *Position
	Revision         uint64
	PreviousRevision uint64
}

type SubscriptionFailure struct {
	SubscriptionID string
	Cause          error
}

// Event is the ordered output of one feed. Exactly one field is populated.
type Event struct {
	Delivery *DeliveryEnvelope
	Progress *Progress
	Failure  *SubscriptionFailure
	Entity   *EntityInstallation
}

func (event Event) Validate() error {
	populated := 0
	if event.Entity != nil {
		populated++
		if err := event.Entity.Target.Validate(); err != nil {
			return err
		}
	}
	if event.Delivery != nil {
		populated++
		if err := event.Delivery.Validate(); err != nil {
			return err
		}
	}
	if event.Progress != nil {
		populated++
		if event.Progress.Position == nil || event.Progress.Position.SourceID == "" || event.Progress.Revision == 0 {
			return errors.New("progress position and revision are required")
		}
		if event.Progress.PreviousRevision >= event.Progress.Revision && event.Progress.PreviousRevision != 0 {
			return errors.New("progress predecessor must be earlier than its revision")
		}
	}
	if event.Failure != nil {
		populated++
		if event.Failure.SubscriptionID == "" || event.Failure.Cause == nil {
			return errors.New("subscription failure ID and cause are required")
		}
	}
	if populated != 1 {
		return errors.New("subscription feed event must contain exactly one payload")
	}
	return nil
}

// QueryRow keeps a to-one expansion next to the row that selected it. Expanded
// is nil when no image is attached; a non-nil snapshot with Row=nil is a loaded
// selection with no match. It has its own freshness floor, independent of Row.
type QueryRow struct {
	Row
	Expanded *EntitySnapshot
}

type QuerySnapshot struct {
	Bounds          Bounds
	Rows            []QueryRow
	Checkpoint      *Position
	Fence           *Position
	Revision        uint64
	OwnedReferences bool
}

type QuerySubscribeRequest struct {
	ID     string
	Query  storage.QuerySpec
	Window InitialWindow
	// Reference declares a one-hop parent-owned entity expansion. Closing or
	// resizing this query automatically releases the affected child interests.
	Reference *storage.ReferenceFetch
}

// EntitySubscribeRequest exposes at most one row. Complete primary-key
// equalities permit direct entity lookup; other predicates use a count-one
// query and select the first covered candidate in query order. If another
// candidate becomes first, removal and addition share one delivery.
type EntitySubscribeRequest struct {
	ID    string
	Query storage.QuerySpec
	// FetchFrom reuses a matching parent-owned interest without another server
	// subscription. Stale or mismatching origins use ordinary entity selection.
	FetchFrom *ReferenceSource
}

type ReferenceSource struct {
	SubscriptionID string
	ParentKey      storage.EntityKey
}

// ValidateQuerySubscribeRequest checks the transport-independent request shape.
// Datastore-specific query validity remains the implementation's concern.
func ValidateQuerySubscribeRequest(request QuerySubscribeRequest) error {
	if request.ID == "" || strings.ContainsRune(request.ID, 0) {
		return errors.New("query subscription ID is required")
	}
	if request.Window.Count <= 0 {
		return errors.New("query subscription initial window must be positive")
	}
	return request.Reference.Validate()
}

func ValidateEntitySubscribeRequest(request EntitySubscribeRequest) error {
	if request.ID == "" || strings.ContainsRune(request.ID, 0) {
		return errors.New("entity subscription ID is required")
	}
	if from := request.FetchFrom; from != nil && (from.SubscriptionID == "" || from.ParentKey.Value == "" || from.ParentKey.Relation.Name == "") {
		return errors.New("reference source requires a parent subscription and entity key")
	}
	return nil
}

// FeedOptions configure one consumer feed. BufferLimit is an
// implementation-independent backpressure bound, not a query property.
type FeedOptions struct {
	BufferLimit int
}

// Subscription is the common lifecycle handle. Live effects are delivered by
// Feed.Next so a source transaction remains grouped across subscriptions.
type Subscription interface {
	ID() string
	Close(context.Context) error
}

// QuerySubscription adds mutable cursor-window control to a subscription.
type QuerySubscription interface {
	Subscription
	UpdateWindow(context.Context, []BoundaryChange, string) error
}

// EntitySubscription intentionally exposes no window control.
type EntitySubscription interface {
	Subscription
}

// QuerySnapshotInstaller installs a subscription and its authoritative snapshot
// before a delivery containing that subscription becomes visible through
// Feed.Next. Complete delivery envelopes are staged so transactions spanning
// existing and installing subscriptions remain atomic. Implementations close
// the subscription and return the installer error when installation fails.
type QuerySnapshotInstaller func(QuerySubscription, QuerySnapshot) error

// EntitySnapshot contains the zero-or-one result of an entity selector.
// Entity effects use Batch.Changes; query membership, cursors, and window
// transitions are removed by the implementation boundary.
type EntitySnapshot struct {
	Row        *Row
	Checkpoint *Position
	Fence      *Position
	Revision   uint64
}

type EntitySnapshotInstaller func(EntitySubscription, EntitySnapshot) error

// Feed scopes subscription identifiers, ordered source progress, and
// lifecycle. A future remote implementation can map one Feed to one
// bidirectional transport without exposing engine groups or coordinator state.
// Subscription IDs are unique for the complete feed lifetime and are
// consumed when a well-formed subscribe operation begins, even if that attempt
// later fails. They cannot be reused after Close; this keeps canceled and retired
// remote delivery metadata unambiguous. All methods must be safe for concurrent
// use: projection expansion can subscribe children while existing subscriptions
// deliver.
type Feed interface {
	SubscribeQuery(context.Context, QuerySubscribeRequest, QuerySnapshotInstaller) error
	SubscribeEntity(context.Context, EntitySubscribeRequest, EntitySnapshotInstaller) error
	// RequestProgress asks the implementation to report its latest
	// already-observed source position as an ordered Progress event. It does not
	// advance the source. The reported position is at least the checkpoint of
	// every snapshot whose subscription completed before this call began.
	// Completion only means the request was accepted; consumers synchronize by
	// continuing to call Next. Existing queued coverage may satisfy a redundant
	// request without another event. If no progress is known, it also succeeds
	// without emitting an event. Consumers may request progress again later.
	RequestProgress(context.Context) error
	// Next returns complete feed deliveries, entity installations, progress,
	// and isolated subscription failures in feed order. It has one consumer.
	Next(context.Context) (Event, error)
	Close(context.Context) error
}

type Client interface {
	OpenFeed(context.Context, FeedOptions) (Feed, error)
}
