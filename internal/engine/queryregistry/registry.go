// Package queryregistry owns shared prepared queries, their covered windows,
// membership indexes, and conservative CDC routing indexes.
package queryregistry

import (
	"fmt"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/expiryindex"
	"github.com/adiom-data/weave/internal/storage"
)

type QueryDescriptor struct {
	Plan datastore.PreparedQuery
	// Identity is non-nil only for the exact-entity backend. It has no ordered
	// range membership; the shared processor evaluates its remaining predicate.
	Identity *storage.EntityKey
}

type RoutingInterest struct {
	Field  storage.FieldPath
	Values bool
}

// InitialWindowPlan describes what ready shared coverage can contribute to an
// initial edge request. Bounds is the final window when Resolved is true and
// otherwise the maximal covered edge available for incremental extension.
type InitialWindowPlan struct {
	Bounds     datastore.Bounds
	CachedRows int
	Available  bool
	Resolved   bool
}

// ExtensionPlan describes the contiguous ready coverage adjoining one window
// boundary. Boundary is final when Resolved is true; otherwise Edge is where
// datastore discovery must continue and CachedRows have already been covered.
type ExtensionPlan struct {
	Boundary   datastore.Boundary
	Edge       datastore.Boundary
	CachedRows int
	Available  bool
	Resolved   bool
}

type liveQuery struct {
	plan        datastore.PreparedQuery
	routing     routing.RoutingPlan
	references  int
	members     *orderedMembers
	windows     map[string]datastore.Bounds
	ready       map[string]bool
	coverage    *coverageEvents
	readyCover  *coverageEvents
	activeReady *coverageEvents
	retained    map[uint64]datastore.Bounds
	prune       []datastore.Bounds
	pruneJobs   int
}

type QueryRegistry struct {
	mu            sync.RWMutex
	queries       map[datastore.QueryFingerprint]*liveQuery
	byRelation    map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}
	byRoute       map[routing.RoutingToken]map[datastore.QueryFingerprint]struct{}
	byRange       map[routing.RoutingToken]*rangeIndex
	byField       map[storage.RelationKey]map[storage.FieldPath]map[datastore.QueryFingerprint]struct{}
	valueField    map[routingFieldKey]map[datastore.QueryFingerprint]struct{}
	watchField    map[routingFieldKey]map[datastore.QueryFingerprint]struct{}
	orderField    map[routingFieldKey]map[datastore.QueryFingerprint]struct{}
	fallback      map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}
	byEntity      map[storage.EntityKey]map[datastore.QueryFingerprint]struct{}
	entities      EntityReferences
	idleTTL       time.Duration
	now           func() time.Time
	nextRetention uint64
	retained      map[uint64]*retainedWindow
	expiry        expiryindex.Index[uint64]
	pruning       []*pruneJob
}

func (r *QueryRegistry) Size() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.queries) }

type routingFieldKey struct {
	relation storage.RelationKey
	field    storage.FieldPath
}

type EntityReferences interface {
	Acquire(storage.EntityKey) (entitycache.EntitySnapshot, bool)
	Get(storage.EntityKey) (entitycache.EntitySnapshot, bool)
	ReleaseMany([]storage.EntityKey) error
}

func NewQueryRegistry(entities EntityReferences) *QueryRegistry {
	return NewQueryRegistryWithRetention(entities, 0)
}

// NewQueryRegistryWithRetention keeps ready abandoned coverage live for ttl.
// A zero TTL disables retention; maintenance still drains scheduled pruning.
func NewQueryRegistryWithRetention(entities EntityReferences, ttl time.Duration) *QueryRegistry {
	return &QueryRegistry{
		idleTTL: ttl, now: time.Now, retained: make(map[uint64]*retainedWindow),
		queries:    make(map[datastore.QueryFingerprint]*liveQuery),
		byRelation: make(map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}),
		byRoute:    make(map[routing.RoutingToken]map[datastore.QueryFingerprint]struct{}),
		byRange:    make(map[routing.RoutingToken]*rangeIndex),
		byField:    make(map[storage.RelationKey]map[storage.FieldPath]map[datastore.QueryFingerprint]struct{}),
		valueField: make(map[routingFieldKey]map[datastore.QueryFingerprint]struct{}),
		watchField: make(map[routingFieldKey]map[datastore.QueryFingerprint]struct{}),
		orderField: make(map[routingFieldKey]map[datastore.QueryFingerprint]struct{}),
		fallback:   make(map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}),
		byEntity:   make(map[storage.EntityKey]map[datastore.QueryFingerprint]struct{}),
		entities:   entities,
	}
}

func (r *QueryRegistry) Acquire(plan datastore.PreparedQuery) (QueryDescriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fingerprint := plan.Fingerprint()
	if existing := r.queries[fingerprint]; existing != nil {
		existing.references++
		return QueryDescriptor{Plan: existing.plan}, false
	}
	query := &liveQuery{plan: plan, references: 1, members: newOrderedMembers(plan), windows: make(map[string]datastore.Bounds), ready: make(map[string]bool), coverage: newCoverageEvents(plan), readyCover: newCoverageEvents(plan), activeReady: newCoverageEvents(plan), retained: make(map[uint64]datastore.Bounds)}
	r.queries[fingerprint] = query
	addIndex(r.byRelation, plan.Relation(), fingerprint)
	routingPlan := routing.Compile(plan.Relation(), plan.Query())
	registered := false
	for _, exact := range routingPlan.Exact {
		addIndex(r.byRoute, exact.Token, fingerprint)
		r.addField(plan.Relation(), exact.Field, fingerprint)
		addIndex(r.valueField, routingFieldKey{relation: plan.Relation(), field: exact.Field}, fingerprint)
		registered = true
	}
	rangeFailedFields := make(map[storage.FieldPath]struct{})
	{
		for _, interval := range routingPlan.Ranges {
			index := r.byRange[interval.Domain]
			if index == nil {
				index = newRangeIndex(routing.Compare)
				r.byRange[interval.Domain] = index
			}
			if index.add(rangeEntry{fingerprint: fingerprint, interval: interval}) {
				r.addField(plan.Relation(), interval.Field, fingerprint)
				addIndex(r.valueField, routingFieldKey{relation: plan.Relation(), field: interval.Field}, fingerprint)
				registered = true
			} else {
				rangeFailedFields[interval.Field] = struct{}{}
				if index.empty() {
					delete(r.byRange, interval.Domain)
				}
			}
		}
	}
	for _, field := range routingPlan.WatchFields {
		key := routingFieldKey{relation: plan.Relation(), field: field}
		addIndex(r.watchField, key, fingerprint)
		r.addField(plan.Relation(), field, fingerprint)
		registered = true
	}
	for field := range rangeFailedFields {
		routingPlan.WatchFields = append(routingPlan.WatchFields, field)
		key := routingFieldKey{relation: plan.Relation(), field: field}
		addIndex(r.watchField, key, fingerprint)
		r.addField(plan.Relation(), field, fingerprint)
		registered = true
	}
	for _, field := range routingPlan.OrderFields {
		key := routingFieldKey{relation: plan.Relation(), field: field}
		addIndex(r.orderField, key, fingerprint)
		r.addField(plan.Relation(), field, fingerprint)
		registered = true
	}
	if routingPlan.Fallback || !registered {
		addIndex(r.fallback, plan.Relation(), fingerprint)
	}
	query.routing = routingPlan
	return QueryDescriptor{Plan: plan}, true
}

func (r *QueryRegistry) Plan(fingerprint datastore.QueryFingerprint) (datastore.PreparedQuery, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil, false
	}
	return query.plan, true
}

// Release drops a lifecycle reference. Its result reports whether the query
// was removed immediately; outstanding memberships retire through Maintain.
func (r *QueryRegistry) Release(fingerprint datastore.QueryFingerprint) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.releaseLocked(fingerprint)
}

func (r *QueryRegistry) releaseLocked(fingerprint datastore.QueryFingerprint) (bool, error) {
	query := r.queries[fingerprint]
	if query == nil {
		return false, nil
	}
	if query.references > 0 {
		query.references--
	}
	if query.references > 0 || len(query.retained) > 0 {
		return false, nil
	}
	r.schedulePruneLocked(query)
	if query.pruneJobs > 0 {
		return false, nil
	}
	if len(query.members.byEntity) > 0 {
		// Normal teardown shares the bounded pruning path. Keep ownership and
		// routing registered until maintenance releases the final membership.
		query.prune = append(query.prune, datastore.Bounds{
			Left:  datastore.Boundary{Kind: datastore.BoundaryStart},
			Right: datastore.Boundary{Kind: datastore.BoundaryEnd},
		})
		r.schedulePruneLocked(query)
		return false, nil
	}
	return r.discardLocked(fingerprint)
}

// discardLocked is the terminal failure path: it does not evaluate a broken
// plan or wait for pruning jobs that might themselves fail on that plan.
func (r *QueryRegistry) discardLocked(fingerprint datastore.QueryFingerprint) (bool, error) {
	query := r.queries[fingerprint]
	if query == nil {
		return false, nil
	}
	keys := query.members.all()
	if err := r.entities.ReleaseMany(keys); err != nil {
		return false, fmt.Errorf("release query members: %w", err)
	}
	for id := range query.retained {
		r.expiry.Remove(id)
		delete(r.retained, id)
	}
	delete(r.queries, fingerprint)
	removeIndex(r.byRelation, query.plan.Relation(), fingerprint)
	routing := query.routing
	fields := make(map[storage.FieldPath]struct{})
	valueFields := make(map[storage.FieldPath]struct{})
	for _, exact := range routing.Exact {
		removeIndex(r.byRoute, exact.Token, fingerprint)
		fields[exact.Field] = struct{}{}
		valueFields[exact.Field] = struct{}{}
	}
	for _, interval := range routing.Ranges {
		fields[interval.Field] = struct{}{}
		valueFields[interval.Field] = struct{}{}
		if index := r.byRange[interval.Domain]; index != nil {
			index.remove(fingerprint)
			if index.empty() {
				delete(r.byRange, interval.Domain)
			}
		}
	}
	for _, field := range routing.WatchFields {
		removeIndex(r.watchField, routingFieldKey{relation: query.plan.Relation(), field: field}, fingerprint)
		fields[field] = struct{}{}
	}
	for _, field := range routing.OrderFields {
		removeIndex(r.orderField, routingFieldKey{relation: query.plan.Relation(), field: field}, fingerprint)
		fields[field] = struct{}{}
	}
	for field := range fields {
		r.removeField(query.plan.Relation(), field, fingerprint)
	}
	for field := range valueFields {
		removeIndex(r.valueField, routingFieldKey{relation: query.plan.Relation(), field: field}, fingerprint)
	}
	removeIndex(r.fallback, query.plan.Relation(), fingerprint)
	for _, key := range keys {
		removeIndex(r.byEntity, key, fingerprint)
	}
	return true, nil
}
