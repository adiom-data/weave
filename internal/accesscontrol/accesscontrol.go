// Package accesscontrol defines the boundary between transaction-ordered
// reactive delivery and application-specific visibility policy.
package accesscontrol

import (
	"context"
	"errors"
	"sync"

	relationdef "github.com/adiom-data/weave/internal/relation"

	"github.com/adiom-data/weave/internal/storage"
)

// PreparedID identifies one claims-specific connection visibility context for
// the lifetime of a server process. Projection paths are supplied per
// Candidate so diamonds remain occurrence-aware within the same context.
type PreparedID string

type Claims = relationdef.Claims
type ProjectionStep = relationdef.ProjectionStep
type ProjectionPath = relationdef.ProjectionPath
type Scope = relationdef.Scope

func NewProjectionPath(parent *ProjectionPath, step ProjectionStep) *ProjectionPath {
	return relationdef.NewProjectionPath(parent, step)
}

type PrepareRequest struct {
	ID     PreparedID
	Claims Claims
	Scope  Scope
}

type Candidate = relationdef.Candidate

// Invalidation says that a committed authorization checkpoint may have
// changed the final visibility of Candidates or Targets for the Prepared context passed
// to Controller.Synchronize. Candidates are full, immutable entity images from
// that checkpoint: this prevents a slow client from accidentally evaluating a
// newer cache version against an older policy view. All is the safe coarse
// fallback when a policy cannot identify candidates; transports must
// resynchronize rather than guess in that case.
type Invalidation struct {
	Candidates []Candidate
	// Targets request reevaluation against the consumer's retained row image.
	// They carry no replacement entity data and preserve occurrence identity.
	Targets []InvalidationTarget
	All     bool
}

type InvalidationTarget struct {
	InstanceID string
	Key        storage.EntityKey
}

// View is a stable authorization-state snapshot. Implementations retain any
// underlying persistent state until Close. A view must never silently advance
// while an asynchronous evaluation is using it.
type View interface {
	Close() error
}

// Checkpoint proves that the controller's database-derived facts have reached
// one committed source checkpoint. Invalidations are complete for that source
// transaction and are joined with its raw reactive effects before delivery.
type Checkpoint struct {
	Position     storage.Checkpoint
	View         View
	Invalidation Invalidation
}

// SnapshotCheckpoint is the corresponding authorization view used while a
// new table-level snapshot is reconciled through its source fence.
type SnapshotCheckpoint struct {
	Fence storage.Fence
	View  View
}

// Controller owns shared authorization facts and their ordered source
// watermark. Its methods must be concurrency-safe. Synchronize must return the
// immutable view for the requested checkpoint even when another connection has
// already requested a later one. Synchronize returns only the invalidation for
// target; implementations should retain shared policy effects rather than
// materializing them once per prepared connection. Repeated calls for the same
// target and checkpoint must expose the same complete invalidation. View.Close
// lets implementations bound retained history. Calls may block and always run
// outside the coordinator actor and CDC reader. The component that constructs
// a Controller owns its process lifecycle; the reactive service closes only
// Prepared values that it creates.
type Controller interface {
	Prepare(context.Context, PrepareRequest) (Prepared, error)
	Synchronize(context.Context, PreparedID, storage.Checkpoint) (Checkpoint, error)
	AcquireSnapshot(context.Context, storage.Fence) (SnapshotCheckpoint, error)
}

// Prepared owns claims-specific policy state. Evaluate may block or perform
// IO and must be safe for concurrent snapshot initializations on one client
// connection. It evaluates against the supplied immutable View rather than
// implicitly consulting whatever authorization state happens to be current
// later.
type Prepared interface {
	ID() PreparedID
	Evaluate(context.Context, View, []Candidate) ([]bool, error)
	// Release drops dependency ownership for rows that actually left a query,
	// not rows hidden by visibility. Nil keys retires the whole instance.
	Release(context.Context, string, []storage.EntityKey) error
	Close() error
}

// allowAll is the initial policy implementation. It exercises the complete
// lifecycle while retaining no policy facts and producing no invalidations.
type allowAll struct {
	mu       sync.Mutex
	closed   bool
	prepared map[PreparedID]struct{}
}

func NewAllowAll() Controller {
	return &allowAll{prepared: make(map[PreparedID]struct{})}
}

func (a *allowAll) Prepare(_ context.Context, request PrepareRequest) (Prepared, error) {
	if request.ID == "" {
		return nil, errors.New("prepared visibility ID is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errors.New("access controller is closed")
	}
	if _, exists := a.prepared[request.ID]; exists {
		return nil, errors.New("prepared visibility ID already exists")
	}
	a.prepared[request.ID] = struct{}{}
	return &allowAllPrepared{id: request.ID, owner: a}, nil
}

func (a *allowAll) Synchronize(_ context.Context, target PreparedID, position storage.Checkpoint) (Checkpoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return Checkpoint{}, errors.New("access controller is closed")
	}
	if position == nil {
		return Checkpoint{}, errors.New("authorization checkpoint position is required")
	}
	if _, exists := a.prepared[target]; !exists {
		return Checkpoint{}, errors.New("prepared visibility context does not exist")
	}
	return Checkpoint{Position: position, View: allowAllView{}}, nil
}

func (a *allowAll) AcquireSnapshot(_ context.Context, fence storage.Fence) (SnapshotCheckpoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return SnapshotCheckpoint{}, errors.New("access controller is closed")
	}
	if fence == nil {
		return SnapshotCheckpoint{}, errors.New("authorization snapshot fence is required")
	}
	return SnapshotCheckpoint{Fence: fence, View: allowAllView{}}, nil
}

func (a *allowAll) Close() error {
	a.mu.Lock()
	a.closed = true
	a.prepared = make(map[PreparedID]struct{})
	a.mu.Unlock()
	return nil
}

type allowAllView struct{}

func (allowAllView) Close() error { return nil }

type allowAllPrepared struct {
	id    PreparedID
	owner *allowAll
	once  sync.Once
}

func (p *allowAllPrepared) ID() PreparedID { return p.id }

func (*allowAllPrepared) Evaluate(_ context.Context, _ View, candidates []Candidate) ([]bool, error) {
	result := make([]bool, len(candidates))
	for index := range result {
		result[index] = true
	}
	return result, nil
}

func (*allowAllPrepared) Release(context.Context, string, []storage.EntityKey) error { return nil }

func (p *allowAllPrepared) Close() error {
	p.once.Do(func() {
		p.owner.mu.Lock()
		delete(p.owner.prepared, p.id)
		p.owner.mu.Unlock()
	})
	return nil
}
