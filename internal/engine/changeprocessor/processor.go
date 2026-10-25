// Package changeprocessor applies datastore-native changes to shared entity
// state and derives query membership transitions. It performs no IO and owns
// no client delivery state.
package changeprocessor

import (
	"errors"
	"fmt"
	"sort"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrFatalSource = errors.New("fatal source contract violation")

type QueryChangeKind uint8

const (
	QueryAdded QueryChangeKind = iota + 1
	QueryUpdated
	QueryRemoved
	// QueryCleared removes every current member of one shared query. It is a
	// first-class transition so bulk source operations such as TRUNCATE do not
	// expand into one queued removal per entity.
	QueryCleared
)

type QueryChange struct {
	Query datastore.QueryFingerprint
	Kind  QueryChangeKind
	Key   storage.EntityKey
	// PreviousRecord is the immutable entity image immediately before this
	// source change. It is populated for updates and removals so downstream
	// delivery can derive a subscription-window transition without retaining a
	// per-subscription copy of query membership.
	PreviousRecord storage.Record
	Record         storage.Record
	// Pending means query structure is known but the complete normalized entity
	// payload still requires asynchronous hydration.
	Pending bool
	// ChangeIndex identifies one source change within a committed batch. Zero
	// denotes a snapshot/reconciliation change without a source ordinal.
	ChangeIndex uint32
}

type EntityActionKind uint8

const (
	EntityHydrate EntityActionKind = iota + 1
	EntityCancelHydration
	// EntityDiscardPartial removes an incomplete cache value after query
	// failures have released their references. It prevents an irrelevant
	// partial tuple from becoming permanent shared state.
	EntityDiscardPartial
)

type EntityAction struct {
	Kind EntityActionKind
	Key  storage.EntityKey
	// Queries is populated for EntityHydrate with the exact set whose
	// membership, ordering, or payload still depends on the incomplete row.
	Queries []datastore.QueryFingerprint
}

type QueryFailure struct {
	Query datastore.QueryFingerprint
	Err   error
}

type ProcessResult struct {
	Changes []QueryChange
	// EntityActions contains one normalized final recovery disposition per
	// affected entity in this committed relation batch.
	EntityActions   []EntityAction
	RelationCleared bool
	// Resync lists explicit adapter invalidations that cannot be represented as
	// entity or query transitions. Incomplete entity payloads use EntityActions;
	// Relation clears are represented by QueryCleared changes.
	Resync []storage.RelationKey
	// QueryFailures are isolated evaluation failures. The entity transition is
	// still valid, but the named live query must be discarded. The caller can
	// recreate it through the normal subscription initialization path.
	QueryFailures []QueryFailure
}

// Processor applies source-native changes once to the shared entity cache and
// then evaluates only the live-query candidates selected by QueryRegistry.
// Incomplete native patches may still publish structurally decidable query
// changes. Their entity payload remains pending until fenced hydration.
type Processor struct {
	source   storage.SourceID
	entities EntityStore
	queries  QueryIndex
}

// EntityStore is the entity-cache behavior required by change evaluation.
// The interface lives here because changeprocessor is its consumer.
type EntityStore interface {
	Get(storage.EntityKey) (entitycache.EntitySnapshot, bool)
	StoreCurrent(storage.EntityKey, storage.Record) entitycache.EntitySnapshot
	StorePartial(storage.EntityKey, storage.Record) entitycache.EntitySnapshot
	DeleteAuthoritative(storage.EntityKey) error
	MarkUnreferenced(storage.EntityKey)
	InvalidateRelation(storage.RelationKey) []storage.EntityKey
}

// QueryIndex is the live-query behavior required by change evaluation.
type QueryIndex interface {
	RelationCandidates(storage.RelationKey) []queryregistry.QueryDescriptor
	RoutingFields(storage.RelationKey) []queryregistry.RoutingInterest
	Candidates(storage.RelationKey, storage.EntityKey, []routing.RoutingProbe, bool) []queryregistry.QueryDescriptor
	IsMember(datastore.QueryFingerprint, storage.EntityKey) bool
	AddMember(datastore.QueryFingerprint, storage.EntityKey) (entitycache.EntitySnapshot, bool, error)
	RefreshMember(datastore.QueryFingerprint, storage.EntityKey) error
	RemoveMember(datastore.QueryFingerprint, storage.EntityKey) (bool, error)
	ClearRelation(storage.RelationKey) ([]datastore.QueryFingerprint, error)
	CoversKey(datastore.QueryFingerprint, datastore.SortKey) (bool, error)
}

func NewProcessor(source storage.SourceID, entities EntityStore, queries QueryIndex) *Processor {
	return &Processor{source: source, entities: entities, queries: queries}
}

// ApplyRelationBatch applies one relation's ordered portion of a committed
// source transaction. A mixed-relation batch is a fatal caller contract error.
func (p *Processor) ApplyRelationBatch(batch datastore.CommittedChanges) (ProcessResult, error) {
	type overlayEntity struct {
		record storage.Record
		exists bool
	}
	type resolvedChange struct {
		before      storage.EntityKey
		previous    storage.Record
		hadPrevious bool
		applied     datastore.AppliedChange
		changeIndex uint32
	}
	type resolvedOperation struct {
		change   *resolvedChange
		clear    bool
		relation storage.RelationKey
		index    uint32
	}

	var result ProcessResult
	var resolved []resolvedOperation
	overlay := make(map[storage.EntityKey]overlayEntity)
	var relation storage.RelationKey
	invalidated := false
	cleared := false
	var previousChangeIndex uint32
	if len(batch.SourceOrdinals) != 0 && len(batch.SourceOrdinals) != len(batch.Changes) {
		return ProcessResult{}, fmt.Errorf("%w: source ordinals do not match changes", ErrFatalSource)
	}
	for index, change := range batch.Changes {
		if change == nil {
			return ProcessResult{}, fmt.Errorf("%w: relation batch contains a nil change", ErrFatalSource)
		}
		changeRelation := change.Relation()
		if changeRelation.Source == "" || changeRelation.Name == "" {
			return ProcessResult{}, fmt.Errorf("%w: change has an incomplete relation identity", ErrFatalSource)
		}
		if index == 0 {
			relation = changeRelation
		} else if changeRelation != relation {
			return ProcessResult{}, fmt.Errorf("%w: relation batch mixes %s.%s and %s.%s", ErrFatalSource, relation.Namespace, relation.Name, changeRelation.Namespace, changeRelation.Name)
		}
		if changeRelation.Source != p.source {
			return ProcessResult{}, fmt.Errorf("%w: change belongs to source %q", ErrFatalSource, changeRelation.Source)
		}
		changeIndex := uint32(index + 1)
		if len(batch.SourceOrdinals) != 0 {
			changeIndex = batch.SourceOrdinals[index]
			if changeIndex == 0 {
				return ProcessResult{}, fmt.Errorf("%w: source change index must be positive", ErrFatalSource)
			}
		}
		if changeIndex <= previousChangeIndex {
			return ProcessResult{}, fmt.Errorf("%w: source change indexes must be strictly increasing", ErrFatalSource)
		}
		previousChangeIndex = changeIndex
		if change.Kind() == datastore.ChangeInvalidate {
			invalidated = true
			continue
		}
		if change.Kind() == datastore.ChangeClearRelation {
			// Only the final state after the last clear can be visible at commit.
			resolved = resolved[:0]
			clear(overlay)
			cleared = true
			resolved = append(resolved, resolvedOperation{clear: true, relation: relation, index: changeIndex})
			continue
		}
		if invalidated {
			continue
		}
		beforeKey, err := change.BeforeKey()
		if err != nil {
			return ProcessResult{}, fmt.Errorf("resolve before-key for %s.%s: %w", change.Relation().Namespace, change.Relation().Name, err)
		}
		previousState, staged := overlay[beforeKey]
		var previous storage.Record
		hadPrevious := false
		if staged {
			previous, hadPrevious = previousState.record, previousState.exists
		} else if !cleared {
			cached, ok := p.entities.Get(beforeKey)
			if ok {
				previous, hadPrevious = cached.Record, true
			}
		}
		applied, err := change.Apply(storage.CloneRecord(previous))
		if err != nil {
			if errors.Is(err, datastore.ErrInvalidated) {
				invalidated = true
				continue
			}
			return ProcessResult{}, err
		}
		if err := validateAppliedChange(change, beforeKey, applied); err != nil {
			return ProcessResult{}, err
		}
		resolved = append(resolved, resolvedOperation{change: &resolvedChange{before: beforeKey, previous: storage.CloneRecord(previous), hadPrevious: hadPrevious, applied: applied, changeIndex: changeIndex}, relation: relation})
		overlay[beforeKey] = overlayEntity{exists: false}
		if applied.Exists() {
			overlay[applied.Key] = overlayEntity{record: storage.CloneRecord(applied.Record), exists: true}
		}
	}
	if invalidated {
		result.Resync = append(result.Resync, relation)
		return result, nil
	}
	entityActions := make(map[storage.EntityKey]EntityAction)
	var entityActionOrder []storage.EntityKey
	setEntityAction := func(action EntityAction) {
		key := action.Key
		if key.Value == "" {
			return
		}
		if _, exists := entityActions[key]; !exists {
			entityActionOrder = append(entityActionOrder, key)
		}
		entityActions[key] = action
	}
	for _, operation := range resolved {
		if operation.clear {
			fingerprints, err := p.queries.ClearRelation(operation.relation)
			if err != nil {
				return ProcessResult{}, fmt.Errorf("clear relation memberships: %w", err)
			}
			for _, fingerprint := range fingerprints {
				result.Changes = append(result.Changes, QueryChange{Query: fingerprint, Kind: QueryCleared, ChangeIndex: operation.index})
			}
			// Clearing memberships releases query-owned references first. Removing
			// cache generations then cancels any older in-flight hydration so it
			// cannot resurrect a row after the clear. Later changes in this same
			// committed batch may repopulate the relation normally.
			p.entities.InvalidateRelation(operation.relation)
			result.RelationCleared = true
			clear(entityActions)
			entityActionOrder = entityActionOrder[:0]
			continue
		}
		change := *operation.change
		// A primary-key move or delete supersedes any pending hydration for the
		// old identity. The final operation for each identity wins below.
		if !change.applied.Exists() || change.before != change.applied.Key {
			setEntityAction(EntityAction{Kind: EntityCancelHydration, Key: change.before})
		}
		reconciled, err := p.reconcile(change.before, change.previous, change.hadPrevious, change.applied, change.changeIndex, false)
		if err != nil {
			return ProcessResult{}, err
		}
		result.Changes = append(result.Changes, reconciled.changes...)
		result.QueryFailures = append(result.QueryFailures, reconciled.failures...)
		if change.applied.Kind == datastore.AppliedPartial {
			kind := EntityDiscardPartial
			if len(reconciled.hydrationQueries) > 0 {
				kind = EntityHydrate
			}
			setEntityAction(EntityAction{Kind: kind, Key: change.applied.Key, Queries: reconciled.hydrationQueries})
		} else if change.applied.Exists() {
			setEntityAction(EntityAction{Kind: EntityCancelHydration, Key: change.applied.Key})
		}
	}
	for _, key := range entityActionOrder {
		result.EntityActions = append(result.EntityActions, entityActions[key])
	}
	return result, nil
}

func validateAppliedChange(change datastore.Change, before storage.EntityKey, applied datastore.AppliedChange) error {
	relation := change.Relation()
	if relation.Source == "" || relation.Name == "" {
		return fmt.Errorf("%w: change has an incomplete relation identity", ErrFatalSource)
	}
	if change.Kind() == datastore.ChangeUpdate || change.Kind() == datastore.ChangeDelete {
		if before.Relation != relation || before.Value == "" {
			return fmt.Errorf("%w: change before-key is outside its relation", ErrFatalSource)
		}
	}
	switch applied.Kind {
	case datastore.AppliedDeleted:
		if change.Kind() != datastore.ChangeDelete {
			return fmt.Errorf("%w: non-delete change returned a deleted result", ErrFatalSource)
		}
		if applied.Key != before || applied.Record != nil {
			return fmt.Errorf("%w: deleted result must contain only the before-key", ErrFatalSource)
		}
	case datastore.AppliedComplete, datastore.AppliedPartial:
		if change.Kind() == datastore.ChangeDelete {
			return fmt.Errorf("%w: delete change returned an existing entity", ErrFatalSource)
		}
		if applied.Key.Relation != relation || applied.Key.Value == "" {
			return fmt.Errorf("%w: applied entity key is outside the change relation", ErrFatalSource)
		}
		if applied.Kind == datastore.AppliedComplete {
			if applied.Record == nil {
				return fmt.Errorf("%w: complete result has no record", ErrFatalSource)
			}
		}
		for name, value := range applied.Record {
			var valueErr error
			if applied.Kind == datastore.AppliedComplete {
				valueErr = value.ValidateComplete()
			} else {
				valueErr = value.Validate()
			}
			if valueErr != nil {
				return fmt.Errorf("%w: result field %q is invalid: %v", ErrFatalSource, name, valueErr)
			}
			switch value.State {
			case storage.ValuePresent, storage.ValueNull:
			case storage.ValueUnavailable:
				if applied.Kind == datastore.AppliedComplete {
					return fmt.Errorf("%w: complete result field %q is unavailable", ErrFatalSource, name)
				}
			default:
				return fmt.Errorf("%w: result field %q has invalid state %d", ErrFatalSource, name, value.State)
			}
		}
	default:
		return fmt.Errorf("%w: change returned an unknown applied-result kind", ErrFatalSource)
	}
	return nil
}

// Reconcile installs a complete, source-current entity without requiring a
// synthetic CDC event. Hydration and snapshot catch-up use this only after
// establishing their source fence.
func (p *Processor) Reconcile(key storage.EntityKey, record storage.Record, exists bool) (ProcessResult, error) {
	previous, present := p.entities.Get(key)
	applied := datastore.AppliedChange{Kind: datastore.AppliedComplete, Key: key, Record: record}
	if !exists {
		applied = datastore.AppliedChange{Kind: datastore.AppliedDeleted, Key: key}
	}
	reconciled, err := p.reconcile(key, previous.Record, present, applied, 0, true)
	if err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Changes: reconciled.changes, QueryFailures: reconciled.failures}, nil
}

type reconcileResult struct {
	changes          []QueryChange
	failures         []QueryFailure
	hydrationQueries []datastore.QueryFingerprint
}

func (p *Processor) reconcile(beforeKey storage.EntityKey, previousRecord storage.Record, hadPrevious bool, applied datastore.AppliedChange, changeIndex uint32, forceFallback bool) (reconcileResult, error) {
	candidates := make(map[datastore.QueryFingerprint]queryregistry.QueryDescriptor)
	relation := beforeKey.Relation
	if applied.Key.Relation.Source != "" {
		relation = applied.Key.Relation
	}
	for _, query := range p.queries.Candidates(relation, beforeKey, nil, false) {
		if query.Identity != nil || p.queries.IsMember(query.Plan.Fingerprint(), beforeKey) {
			candidates[query.Plan.Fingerprint()] = query
		}
	}
	if applied.Exists() {
		fields := p.queries.RoutingFields(relation)
		probes := p.routingProbes(relation, previousRecord, applied.Record, fields, hadPrevious, applied.Complete() && beforeKey == applied.Key)
		includeFallback := forceFallback || !hadPrevious || beforeKey != applied.Key
		for _, query := range p.queries.Candidates(relation, applied.Key, probes, includeFallback) {
			candidates[query.Plan.Fingerprint()] = query
		}
		if hadPrevious || len(candidates) > 0 {
			if applied.Complete() {
				p.entities.StoreCurrent(applied.Key, applied.Record)
			} else {
				p.entities.StorePartial(applied.Key, applied.Record)
			}
		}
	}

	ordered := make([]datastore.QueryFingerprint, 0, len(candidates))
	for fingerprint := range candidates {
		ordered = append(ordered, fingerprint)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var result reconcileResult
	needsHydration := make(map[datastore.QueryFingerprint]struct{})
	for _, fingerprint := range ordered {
		query := candidates[fingerprint]
		wasMember := p.queries.IsMember(fingerprint, beforeKey)
		if wasMember && beforeKey != applied.Key {
			removed, removeErr := p.queries.RemoveMember(fingerprint, beforeKey)
			if removeErr != nil {
				return reconcileResult{}, removeErr
			}
			if removed {
				result.changes = append(result.changes, QueryChange{Query: fingerprint, Kind: QueryRemoved, Key: beforeKey, PreviousRecord: storage.CloneRecord(previousRecord), ChangeIndex: changeIndex})
			}
			wasMember = false
		}
		isMember := false
		if applied.Exists() {
			matches, matchErr := query.Plan.Matches(applied.Record)
			if matchErr != nil {
				if errors.Is(matchErr, storage.ErrIncompleteRecord) {
					needsHydration[fingerprint] = struct{}{}
					continue
				}
				result.failures = append(result.failures, QueryFailure{Query: fingerprint, Err: fmt.Errorf("evaluate query %q: %w", fingerprint, matchErr)})
				continue
			}
			if matches {
				if query.Identity != nil {
					isMember = applied.Key == *query.Identity
				} else {
					key, keyErr := query.Plan.Key(applied.Record)
					if keyErr != nil {
						if errors.Is(keyErr, storage.ErrIncompleteRecord) {
							needsHydration[fingerprint] = struct{}{}
							continue
						}
						result.failures = append(result.failures, QueryFailure{Query: fingerprint, Err: fmt.Errorf("order query %q: %w", fingerprint, keyErr)})
						continue
					}
					contained, containsErr := p.queries.CoversKey(fingerprint, key)
					if containsErr != nil {
						result.failures = append(result.failures, QueryFailure{Query: fingerprint, Err: fmt.Errorf("bound query %q: %w", fingerprint, containsErr)})
						continue
					}
					isMember = contained
				}
			}
		}

		if wasMember && !isMember {
			removed, removeErr := p.queries.RemoveMember(fingerprint, beforeKey)
			if removeErr != nil {
				return reconcileResult{}, removeErr
			}
			if removed {
				result.changes = append(result.changes, QueryChange{Query: fingerprint, Kind: QueryRemoved, Key: beforeKey, PreviousRecord: storage.CloneRecord(previousRecord), ChangeIndex: changeIndex})
			}
			wasMember = false
		}
		if isMember && !p.queries.IsMember(fingerprint, applied.Key) {
			if applied.Complete() {
				p.entities.StoreCurrent(applied.Key, applied.Record)
			} else {
				p.entities.StorePartial(applied.Key, applied.Record)
			}
			if _, _, addErr := p.queries.AddMember(fingerprint, applied.Key); addErr != nil {
				return reconcileResult{}, addErr
			}
			result.changes = append(result.changes, QueryChange{Query: fingerprint, Kind: QueryAdded, Key: applied.Key, Record: storage.CloneRecord(applied.Record), Pending: !applied.Complete(), ChangeIndex: changeIndex})
		} else if isMember && wasMember {
			if err := p.queries.RefreshMember(fingerprint, applied.Key); err != nil {
				result.failures = append(result.failures, QueryFailure{Query: fingerprint, Err: fmt.Errorf("reorder query %q: %w", fingerprint, err)})
				continue
			}
			result.changes = append(result.changes, QueryChange{Query: fingerprint, Kind: QueryUpdated, Key: applied.Key, PreviousRecord: storage.CloneRecord(previousRecord), Record: storage.CloneRecord(applied.Record), Pending: !applied.Complete(), ChangeIndex: changeIndex})
		}
		if isMember && !applied.Complete() {
			needsHydration[fingerprint] = struct{}{}
		}
	}

	if !applied.Exists() {
		if err := p.entities.DeleteAuthoritative(beforeKey); err != nil {
			return reconcileResult{}, err
		}
	} else if beforeKey != applied.Key {
		if err := p.entities.DeleteAuthoritative(beforeKey); err != nil {
			return reconcileResult{}, err
		}
	}
	if applied.Exists() {
		p.entities.MarkUnreferenced(applied.Key)
	}
	for fingerprint := range needsHydration {
		result.hydrationQueries = append(result.hydrationQueries, fingerprint)
	}
	sort.Slice(result.hydrationQueries, func(i, j int) bool { return result.hydrationQueries[i] < result.hydrationQueries[j] })
	return result, nil
}

func (p *Processor) routingProbes(relation storage.RelationKey, previous, current storage.Record, fields []queryregistry.RoutingInterest, hadPrevious, diffable bool) []routing.RoutingProbe {
	probes := make([]routing.RoutingProbe, 0, len(fields)*2)
	for _, interest := range fields {
		field := interest.Field
		oldValue := recordValue(previous, field)
		newValue := recordValue(current, field)
		if diffable && hadPrevious && equalValue(oldValue, newValue) {
			continue
		}
		if !interest.Values {
			probes = append(probes, routing.RoutingProbe{Field: field, Complete: true, Ordering: hadPrevious})
			continue
		}
		if hadPrevious {
			tokens, points, complete := routing.Probe(relation, field, oldValue)
			probes = append(probes, routing.RoutingProbe{Field: field, Tokens: tokens, Points: points, Complete: complete, Ordering: true})
		}
		tokens, points, complete := routing.Probe(relation, field, newValue)
		probes = append(probes, routing.RoutingProbe{Field: field, Tokens: tokens, Points: points, Complete: complete, Ordering: hadPrevious})
	}
	return probes
}

func recordValue(record storage.Record, field storage.FieldPath) storage.Value {
	if value, ok := storage.Lookup(record, field); ok {
		return value
	}
	return storage.Value{State: storage.ValueMissing}
}

func equalValue(left, right storage.Value) bool {
	return storage.EqualValues(left, right)
}
