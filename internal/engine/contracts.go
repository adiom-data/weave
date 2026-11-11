package engine

import (
	"context"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

type entityReader interface {
	Get(storage.EntityKey) (entitycache.EntitySnapshot, bool)
	Inspect(storage.EntityKey, func(storage.Record, entitycache.EntityState) error) (bool, error)
	IsCurrent(storage.EntityKey) bool
}

type entityLoadState interface {
	BeginRefresh(storage.EntityKey) entitycache.Load
	CompleteMerged(entitycache.Load, storage.Record) (storage.Record, error)
	Fail(entitycache.Load, error) error
	LoadError(entitycache.Load) error
	DiscardIfGeneration(storage.EntityKey, uint64, error) (bool, error)
}

type entityMaintenance interface {
	Size() int
	PrepareRelationRefresh(storage.RelationKey)
	EvictIdle(time.Time, int) ([]storage.EntityKey, bool)
	InvalidateRelation(storage.RelationKey) []storage.EntityKey
}

// entityState composes only the shared-record capabilities used directly by
// the coordinator. Snapshot loading and change evaluation declare their own
// smaller consumer-owned interfaces.
type entityState interface {
	entityReader
	entityLoadState
	entityMaintenance
}

type queryLifecycle interface {
	Size() int
	HasRelation(storage.RelationKey) bool
	Acquire(datastore.PreparedQuery) (queryregistry.QueryDescriptor, bool)
	Release(datastore.QueryFingerprint) (bool, error)
	Plan(datastore.QueryFingerprint) (datastore.PreparedQuery, bool)
	RelationCandidates(storage.RelationKey) []queryregistry.QueryDescriptor
	Discard(datastore.QueryFingerprint) error
	Maintain(time.Time, int) ([]queryregistry.MaintenanceFailure, bool)
}

type queryWindows interface {
	SetWindow(datastore.QueryFingerprint, string, datastore.Bounds) error
	InstallWindow(datastore.QueryFingerprint, string, datastore.Bounds) (bool, error)
	MarkWindowReady(datastore.QueryFingerprint, string) error
	RemoveWindow(datastore.QueryFingerprint, string) (bool, error)
	Coverage(datastore.QueryFingerprint) []datastore.Bounds
	SortKeysInBounds(datastore.QueryFingerprint, datastore.Bounds, int, bool) ([]datastore.SortKey, error)
	PlanInitialWindow(datastore.QueryFingerprint, int, bool) (queryregistry.InitialWindowPlan, error)
	PlanExtension(datastore.QueryFingerprint, datastore.Boundary, int, bool) (queryregistry.ExtensionPlan, error)
	ReadyGaps(datastore.QueryFingerprint, datastore.Bounds) ([]datastore.Bounds, error)
	PruneOutsideCoverage(datastore.QueryFingerprint) error
}

type queryMemberships interface {
	AddMember(datastore.QueryFingerprint, storage.EntityKey) (entitycache.EntitySnapshot, bool, error)
	RemoveMember(datastore.QueryFingerprint, storage.EntityKey) (bool, error)
	RemoveEntity(storage.EntityKey) ([]datastore.QueryFingerprint, error)
	IsMember(datastore.QueryFingerprint, storage.EntityKey) bool
	Members(datastore.QueryFingerprint) []storage.EntityKey
	// A single interval returns query-ordered, caller-owned snapshots. Multiple
	// intervals are visited in input order, deduplicating overlapping entities.
	EntitiesInBounds(datastore.QueryFingerprint, []datastore.Bounds) ([]entitycache.EntitySnapshot, error)
	EntityKeysInBounds(datastore.QueryFingerprint, []datastore.Bounds) ([]storage.EntityKey, error)
	PositionInBounds(datastore.QueryFingerprint, datastore.Bounds, storage.EntityKey) (int, bool, error)
}

// queryState composes the shared-query capabilities used by orchestration;
// routing and mutation during CDC evaluation remain private to changeprocessor.
type queryState interface {
	queryLifecycle
	queryWindows
	queryMemberships
}

type changeEvaluator interface {
	ApplyRelationBatch(datastore.CommittedChanges) (changeprocessor.ProcessResult, error)
	Reconcile(storage.EntityKey, storage.Record, bool) (changeprocessor.ProcessResult, error)
}

type windowReader interface {
	ReadIdentity(context.Context, datastore.IdentityQuery) (*windowloader.PendingWindow, error)
	ReadRanges(context.Context, datastore.PreparedQuery, []datastore.Bounds, int) (*windowloader.PendingWindow, error)
	DiscoverWindows(context.Context, datastore.PreparedQuery, []datastore.WindowRequest) ([][]windowloader.WindowDiscovery, error)
	ReadRefresh(context.Context, datastore.PreparedQuery, datastore.WindowRequest) (*windowloader.PendingWindow, error)
}

type subscriptionDelivery interface {
	OpenGroup(string, int) *delivery.ClientQueue[delivery.GroupMessage]
	SubscribePreparedGrouped(string, string, datastore.PreparedQuery, datastore.Bounds) (*delivery.Subscription, error)
	PublishDelivery(storage.Checkpoint, bool, uint64, delivery.DeliveryKind, []changeprocessor.QueryChange) delivery.PublishResult
	CaptureGroupProgress(string, storage.Checkpoint, uint64) delivery.ProgressReport
	PublishHydration(storage.Fence, storage.Checkpoint, uint64, []changeprocessor.QueryChange) delivery.PublishResult
	QueueWindowTransition(string, datastore.Bounds, delivery.WindowMembershipDelta, string, uint64, storage.Fence, storage.Checkpoint) ([]delivery.SubscriptionEvent, error)
	Unsubscribe(string) (uint64, bool)
	ReleaseGroup(string) bool
	FailQuery(datastore.QueryFingerprint, error) []string
	CloseAll(error)
}
