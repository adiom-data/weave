package datastore

import "github.com/adiom-data/weave/internal/storage"

// IdentityPreparer optionally specializes an entity selector whose predicate
// guarantees a complete primary key. The returned plan has an entity-selector
// fingerprint distinct from a windowed query, and shares immutable metadata.
// Additional predicates remain part of Matches; this method performs no IO.
type IdentityPreparer interface {
	PrepareIdentity() (IdentityQuery, bool, error)
}

type IdentityQuery interface {
	PreparedQuery
	LookupKey() storage.EntityKey
}

// ReferenceLookup binds a validated reference without preparing a query or
// creating subscription state. Matches includes static and bound predicates.
type ReferenceLookup interface {
	Key() storage.EntityKey
	Matches(storage.Record) (bool, error)
	// UnfilteredIdentity means a missing joined image proves the entity itself
	// absent, rather than merely rejected by a static or bound guard.
	UnfilteredIdentity() bool
}
