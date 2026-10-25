package datastore

import (
	"context"

	"github.com/adiom-data/weave/internal/storage"
)

// ReferenceBinding maps one source field to one target identity field. The
// bindings must cover the target's complete primary key exactly once; additional
// equality guards (such as organization_id) are allowed. This is
// a fetch shape, not part of either component query's logical identity.
type ReferenceBinding = storage.ReferenceBinding

// ReferenceFetch describes one nonrecursive, to-one expansion of a window.
// Target is prepared normally, including adapter access/publication checks.
// Its filter may narrow the referenced entity but never filters the base list.
type ReferenceFetch struct {
	Target   PreparedQuery
	Bindings []ReferenceBinding
}

// ReferenceWindowRow preserves each base row, including when the reference is
// null, dangling, or rejected by the target predicate. Reference=nil means
// absent in this snapshot, not an unresolved asynchronous fetch.
type ReferenceWindowRow struct {
	Row       AuthoritativeRecord
	Reference *AuthoritativeRecord
}

// ReferenceWindowReader is an optional snapshot loading capability. Pagination
// and ordering apply only to the base query, before expansion. Both images use
// the session's snapshot and Finish fence. Results are not subscription state:
// the engine must establish CDC tracking and reconcile them before publication.
// Implementations return complete records, not flattened joined projections.
// Query caches must remain keyed by the component queries, not this fetch shape.
type ReferenceWindowReader interface {
	LoadWindowWithReference(context.Context, PreparedQuery, WindowRequest, ReferenceFetch) ([]ReferenceWindowRow, error)
	// LoadReferences serves a warm base window without rereading its rows. The
	// caller supplies known base images; targets use this session's snapshot.
	LoadReferences(context.Context, ReferenceFetch, []storage.Record) ([]AuthoritativeRecord, error)
}

// ReferencePreparer validates native identity bindings once. Resolve binds a
// lookup without compiling a target query; Bind prepares a selector only when
// an actual child subscription requests it.
type ReferencePreparer interface {
	PrepareReference(context.Context, PreparedQuery, storage.ReferenceFetch) (PreparedReference, error)
}

// PreparedReference is immutable and concurrency-safe. Its methods do no IO
// and must not mutate or retain the supplied parent record. Bound plans and
// definitions own any values they retain from it.
// Fetch returns caller-owned bindings. Resolve returns a bound lookup with no
// query/window state and reads only the declared binding source fields; other
// parent fields need not be present. Bind returns an ordinary prepared plan and
// a caller-owned selector definition, plus the complete target identity.
type PreparedReference interface {
	Fetch() ReferenceFetch
	Resolve(storage.Record) (ReferenceLookup, bool, error)
	Bind(storage.Record) (PreparedQuery, storage.QuerySpec, storage.EntityKey, bool, error)
}
