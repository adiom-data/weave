package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

func (c *Coordinator) queryEntitiesInBounds(fingerprint datastore.QueryFingerprint, bounds []datastore.Bounds, currentOnly bool) ([]entitycache.EntitySnapshot, error) {
	entities, err := c.queries.EntitiesInBounds(fingerprint, bounds)
	if err != nil {
		return nil, err
	}
	if !currentOnly {
		return entities, nil
	}
	result := entities[:0]
	for _, entity := range entities {
		if entity.State == entitycache.EntityCurrent {
			result = append(result, entity)
		}
	}
	return result, nil
}

func (c *Coordinator) publish(checkpoint storage.Checkpoint, atomic bool, changes []changeprocessor.QueryChange) {
	c.publishDelivery(checkpoint, atomic, delivery.DeliveryCommit, changes)
}

func (c *Coordinator) publishDelivery(checkpoint storage.Checkpoint, atomic bool, kind delivery.DeliveryKind, changes []changeprocessor.QueryChange) {
	c.deliveryRevision++
	// The actor does not process the next source transaction until PublishDelivery
	// returns. This keeps the live WindowOrder view pinned to the final state of
	// changes while the hub captures window-relative insertion indexes.
	result := c.hub.PublishDelivery(checkpoint, atomic, c.deliveryRevision, kind, changes)
	for _, id := range result.Dropped {
		c.dropActive(id)
	}
}

func (c *Coordinator) isolateQueryFailures(failures []changeprocessor.QueryFailure) {
	seen := make(map[datastore.QueryFingerprint]struct{})
	for _, failure := range failures {
		if _, duplicate := seen[failure.Query]; duplicate {
			continue
		}
		seen[failure.Query] = struct{}{}
		c.failQuery(failure.Query, failure.Err)
	}
}

// failQuery tears down every reference to one shared query without trying to
// recompute its coverage. This is the safe cleanup path when the prepared plan
// itself can no longer evaluate records or bounds reliably.
func (c *Coordinator) failQuery(fingerprint datastore.QueryFingerprint, cause error) {
	c.hub.FailQuery(fingerprint, cause)
	for _, id := range c.subscriptions.ActiveIDs(fingerprint) {
		c.subscriptions.RemoveActive(id)
	}
	for _, id := range c.subscriptions.InitializationIDs(fingerprint) {
		c.cancelInitialActivation(id, cause)
		c.subscriptions.CancelInitialization(id, fingerprint)
	}
	if err := c.queries.Discard(fingerprint); err != nil {
		c.stop(fmt.Errorf("discard failed query %q: %w", fingerprint, err))
	}
}

func (c *Coordinator) failRelation(relation storage.RelationKey, cause error) {
	if c.prefetch != nil {
		if err := c.prefetch.InvalidateRelation(relation); err != nil {
			c.stop(err)
		}
	}
	if state, ok := c.recovery.RemoveRelation(relation); ok {
		cancelWindows(state.Windows(), cause)
	}
	c.recovery.RemoveRelationHydrations(relation)
	fingerprints := make(map[datastore.QueryFingerprint]struct{})
	for _, query := range c.queries.RelationCandidates(relation) {
		fingerprints[query.Plan.Fingerprint()] = struct{}{}
	}
	for fingerprint := range fingerprints {
		c.failQuery(fingerprint, fmt.Errorf("relation %s.%s requires resynchronization: %w", relation.Namespace, relation.Name, cause))
	}
	c.entities.InvalidateRelation(relation)
}

func (c *Coordinator) dropActive(id string) {
	active, ok := c.subscriptions.Active(id)
	if !ok {
		return
	}
	if token := c.subscriptions.WindowToken(id); token != "" {
		c.cancelWindowActivation(id, context.Canceled)
		operation := windowUpdateOperation{subscriptionID: id, token: token, stageID: windowStageID(id, token), base: active}
		if _, err := c.queries.RemoveWindow(active.Fingerprint, operation.stageID); err != nil {
			c.failQuery(active.Fingerprint, fmt.Errorf("remove staged window for %q: %w", id, err))
			return
		}
		c.subscriptions.AbortWindow(operation.subscriptionID, operation.token)
	}
	if _, err := c.queries.RemoveWindow(active.Fingerprint, id); err != nil {
		c.failQuery(active.Fingerprint, fmt.Errorf("remove window for %q: %w", id, err))
		return
	}
	if err := c.pruneCoverage(active.Fingerprint); err != nil {
		c.failQuery(active.Fingerprint, fmt.Errorf("prune query for %q: %w", id, err))
		return
	}
	c.subscriptions.RemoveActive(id)
	c.releaseQuery(id, active.Fingerprint)
	if c.hasTerminalError() {
		return
	}
}

func sortEntities(plan datastore.PreparedQuery, entities []entitycache.EntitySnapshot) error {
	type keyedEntity struct {
		entity entitycache.EntitySnapshot
		key    datastore.SortKey
	}
	keyed := make([]keyedEntity, len(entities))
	for i, entity := range entities {
		key, err := plan.Key(entity.Record)
		if err != nil {
			return err
		}
		keyed[i] = keyedEntity{entity: entity, key: key}
	}
	var sortErr error
	sort.Slice(keyed, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		compared, err := plan.Compare(keyed[i].key, keyed[j].key)
		if err != nil {
			sortErr = err
			return false
		}
		return compared < 0
	})
	if sortErr != nil {
		return sortErr
	}
	for i := range keyed {
		entities[i] = keyed[i].entity
	}
	return nil
}

func sortWindowDiscoveries(plan datastore.PreparedQuery, records []windowloader.WindowDiscovery) error {
	var sortErr error
	sort.Slice(records, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		compared, err := plan.Compare(records[i].Key, records[j].Key)
		if err != nil {
			sortErr = err
			return false
		}
		return compared < 0
	})
	if sortErr != nil {
		return sortErr
	}
	return nil
}

func (c *Coordinator) unsubscribeActor(id string) error {
	_, err := c.retireSubscriptionActor(id)
	return err
}

func (c *Coordinator) retireSubscriptionActor(id string) (uint64, error) {
	if _, ok := c.subscriptions.Active(id); !ok {
		return 0, ErrSubscriptionNotFound
	}
	revision, _ := c.hub.Unsubscribe(id)
	c.dropActive(id)
	if err := c.currentTerminalError(); err != nil {
		return revision, err
	}
	return revision, nil
}

func (c *Coordinator) pruneCoverage(fingerprint datastore.QueryFingerprint) error {
	return c.queries.PruneOutsideCoverage(fingerprint)
}

func (c *Coordinator) releaseQuery(id string, fingerprint datastore.QueryFingerprint) {
	if _, err := c.queries.RemoveWindow(fingerprint, id); err != nil {
		// The lifecycle reference must already be removed before this helper is
		// called. Release its query reference directly, then fail the remaining
		// shared query rather than stopping unrelated coordinator work.
		if _, releaseErr := c.queries.Release(fingerprint); releaseErr != nil {
			c.stop(fmt.Errorf("release query after window removal failure for %q: %w", id, releaseErr))
			return
		}
		c.failQuery(fingerprint, fmt.Errorf("remove query window for %q: %w", id, err))
		return
	}
	removed, err := c.queries.Release(fingerprint)
	if err != nil {
		c.stop(fmt.Errorf("release query for %q: %w", id, err))
		return
	}
	if !removed {
		if err := c.pruneCoverage(fingerprint); err != nil {
			c.failQuery(fingerprint, fmt.Errorf("prune released query for %q: %w", id, err))
		}
	}
}

func (c *Coordinator) stop(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	c.mu.Unlock()
	c.hub.CloseAll(err)
}

func (c *Coordinator) terminalError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.err != nil {
		return c.err
	}
	return errors.New("coordinator stopped")
}

func (c *Coordinator) currentTerminalError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *Coordinator) hasTerminalError() bool { return c.currentTerminalError() != nil }
