// Package accesscontrol exposes supported contracts and forwards construction to internal implementations.
package accesscontrol

import (
	"context"
	impl "github.com/adiom-data/weave/internal/accesscontrol"
)

// PreparedID identifies one claims-specific connection visibility context for
// the lifetime of a server process. Projection paths are supplied per
// Candidate so diamonds remain occurrence-aware within the same context.
type PreparedID = impl.PreparedID

// Claims is deliberately opaque to the engine. Authentication happens at the
// transport/application edge; a policy implementation interprets the claims.
type Claims = impl.Claims

func ContextWithClaims(ctx context.Context, claims Claims) context.Context {
	return impl.ContextWithClaims(ctx, claims)
}

func ClaimsFromContext(ctx context.Context) (Claims, error) {
	return impl.ClaimsFromContext(ctx)
}

// ProjectionStep identifies one concrete row occurrence on the directional
// expansion path that produced a candidate. Policies must explicitly approve
// every edge in this order; merely sharing a projection or a connection never
// grants visibility. Node is descriptive. Key.Relation is the canonical object
// namespace policies should use when matching an edge.
type ProjectionStep = impl.ProjectionStep

// ProjectionPath shares immutable ancestor images between concrete occurrences.
type ProjectionPath = impl.ProjectionPath

func NewProjectionPath(parent *ProjectionPath, step ProjectionStep) *ProjectionPath {
	return impl.NewProjectionPath(parent, step)
}

// Scope describes server-derived context for a prepared policy. Projection is
// occurrence-specific and directional, so Project -> ProjectTask -> Task cannot
// accidentally authorize Task -> Project or an unrelated Task. Values may carry
// additional application-defined context without teaching the query engine its
// meaning.
type Scope = impl.Scope

type PrepareRequest = impl.PrepareRequest

// Candidate is an immutable entity image considered for public delivery.
// Context is server-derived, but the projection definition that caused it may
// be client supplied. A policy must validate the complete, ordered expansion
// path against its configured directional edge rules.
type Candidate = impl.Candidate
type Decision = impl.Decision

// Invalidation says that a committed authorization checkpoint may have
// changed the final visibility of Candidates for the Prepared context passed
// to Controller.Synchronize. Candidates are full, immutable entity images from
// that checkpoint: this prevents a slow client from accidentally evaluating a
// newer cache version against an older policy view. All is the safe coarse
// fallback when a policy cannot identify candidates; transports must
// resynchronize rather than guess in that case.
type Invalidation = impl.Invalidation
type InvalidationTarget = impl.InvalidationTarget

// View is a stable authorization-state snapshot. Implementations retain any
// underlying persistent state until Close. A view must never silently advance
// while an asynchronous evaluation is using it.
type View = impl.View

// Checkpoint proves that the controller's database-derived facts have reached
// one committed source checkpoint. Invalidations are complete for that source
// transaction and are joined with its raw reactive effects before delivery.
type Checkpoint = impl.Checkpoint

// SnapshotCheckpoint is the corresponding authorization view used while a
// new table-level snapshot is reconciled through its source fence.
type SnapshotCheckpoint = impl.SnapshotCheckpoint

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
type Controller = impl.Controller

// Prepared owns claims-specific policy state. Evaluate may block or perform
// IO and must be safe for concurrent snapshot initializations on one client
// connection. It evaluates against the supplied immutable View rather than
// implicitly consulting whatever authorization state happens to be current
// later.
type Prepared = impl.Prepared

func NewAllowAll() Controller { return impl.NewAllowAll() }
