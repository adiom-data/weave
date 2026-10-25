package queryregistry

import (
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
)

// SetWindow incrementally updates one subscriber's counted interval. Counts
// remain soft because coverage is expressed entirely in stable boundaries.
func (r *QueryRegistry) SetWindow(fingerprint datastore.QueryFingerprint, subscriptionID string, bounds datastore.Bounds) error {
	return r.setWindow(fingerprint, subscriptionID, bounds, true)
}

// InstallWindow registers a window needed by an initialization or transition.
// It is immediately reusable only when already contained by materialized
// coverage; otherwise MarkWindowReady must follow fenced snapshot activation.
func (r *QueryRegistry) InstallWindow(fingerprint datastore.QueryFingerprint, subscriptionID string, bounds datastore.Bounds) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return false, errors.New("live query not found")
	}
	covered, err := query.readyCover.covered(bounds)
	if err != nil {
		return false, err
	}
	if err := r.setWindowLocked(query, subscriptionID, bounds, covered); err != nil {
		return false, err
	}
	return covered, nil
}

func (r *QueryRegistry) setWindow(fingerprint datastore.QueryFingerprint, subscriptionID string, bounds datastore.Bounds, ready bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return errors.New("live query not found")
	}
	return r.setWindowLocked(query, subscriptionID, bounds, ready)
}

func (r *QueryRegistry) setWindowLocked(query *liveQuery, subscriptionID string, bounds datastore.Bounds, ready bool) error {
	compared, err := querywindow.CompareBoundary(query.plan, bounds.Left, bounds.Right)
	if err != nil {
		return err
	}
	if compared > 0 {
		return errors.New("left boundary follows right boundary")
	}
	return r.transitionWindowLocked(query, subscriptionID, &bounds, ready)
}

func (r *QueryRegistry) transitionWindowLocked(query *liveQuery, subscriptionID string, bounds *datastore.Bounds, ready bool) error {
	previous, existed := query.windows[subscriptionID]
	previousReady := query.ready[subscriptionID]
	coverage := query.coverage.clone()
	readyCoverage := query.readyCover.clone()
	activeReady := query.activeReady.clone()
	if existed {
		if err := coverage.remove(previous); err != nil {
			return err
		}
		if previousReady {
			if err := readyCoverage.remove(previous); err != nil {
				return err
			}
			if err := activeReady.remove(previous); err != nil {
				return err
			}
		}
	}
	if bounds != nil {
		if err := coverage.add(*bounds); err != nil {
			return err
		}
		if ready {
			if err := readyCoverage.add(*bounds); err != nil {
				return err
			}
			if err := activeReady.add(*bounds); err != nil {
				return err
			}
		}
	}
	var retained, lost []datastore.Bounds
	if existed && previousReady && r.idleTTL > 0 {
		var err error
		retained, err = activeReady.regionsWithCount(previous, 0)
		if err != nil {
			return err
		}
		for _, interval := range retained {
			if err := coverage.add(interval); err != nil {
				return err
			}
			if err := readyCoverage.add(interval); err != nil {
				return err
			}
		}
	}
	if existed {
		var err error
		lost, err = coverage.regionsWithCount(previous, 0)
		if err != nil {
			return err
		}
	}
	if bounds == nil {
		delete(query.windows, subscriptionID)
		delete(query.ready, subscriptionID)
	} else {
		query.windows[subscriptionID] = *bounds
		query.ready[subscriptionID] = ready
	}
	query.coverage = coverage
	query.readyCover = readyCoverage
	query.activeReady = activeReady
	r.retainLocked(query, retained)
	query.prune = append(query.prune, lost...)
	return nil
}

func (r *QueryRegistry) MarkWindowReady(fingerprint datastore.QueryFingerprint, subscriptionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return errors.New("live query not found")
	}
	if _, exists := query.windows[subscriptionID]; !exists {
		return errors.New("query window not found")
	}
	if query.ready[subscriptionID] {
		return nil
	}
	coverage := query.readyCover.clone()
	if err := coverage.add(query.windows[subscriptionID]); err != nil {
		return err
	}
	activeReady := query.activeReady.clone()
	if err := activeReady.add(query.windows[subscriptionID]); err != nil {
		return err
	}
	query.ready[subscriptionID] = true
	query.readyCover = coverage
	query.activeReady = activeReady
	return nil
}

func (r *QueryRegistry) RemoveWindow(fingerprint datastore.QueryFingerprint, subscriptionID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return false, nil
	}
	if _, exists := query.windows[subscriptionID]; !exists {
		return false, nil
	}
	err := r.transitionWindowLocked(query, subscriptionID, nil, false)
	return err == nil, err
}

func (r *QueryRegistry) Coverage(fingerprint datastore.QueryFingerprint) []datastore.Bounds {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil
	}
	coverage, err := query.coverage.intervals()
	if err != nil {
		return nil
	}
	return coverage
}

// CoversKey evaluates the incremental coverage prefix without visiting client
// windows or materializing the disjoint union.
func (r *QueryRegistry) CoversKey(fingerprint datastore.QueryFingerprint, key datastore.SortKey) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return false, errors.New("live query not found")
	}
	return query.coverage.contains(key)
}

// PlanInitialWindow uses only materialized, ready coverage. It resolves the
// request when the covered edge contains enough rows or reaches the opposite
// sentinel. Otherwise it returns the covered edge and row count so callers can
// fetch only the adjacent missing portion. It never infers absence through an
// uncovered gap.
func (r *QueryRegistry) PlanInitialWindow(fingerprint datastore.QueryFingerprint, count int, fromEnd bool) (InitialWindowPlan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return InitialWindowPlan{}, errors.New("live query not found")
	}
	if count < 1 {
		return InitialWindowPlan{}, errors.New("initial count must be positive")
	}
	full := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	edge, covered, err := query.readyCover.edgeInterval(fromEnd)
	if err != nil || !covered {
		return InitialWindowPlan{}, err
	}
	limit := count
	if fromEnd {
		// The left boundary is open, so retaining N suffix rows requires the
		// predecessor immediately before them.
		limit++
	}
	keys, err := query.members.sortKeysWithin(edge, limit, fromEnd)
	if err != nil {
		return InitialWindowPlan{}, err
	}
	plan := InitialWindowPlan{Bounds: edge, CachedRows: min(len(keys), count), Available: true}
	if !fromEnd && len(keys) >= count {
		plan.Bounds = full
		plan.Bounds.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[count-1]}
		plan.Resolved = true
		return plan, nil
	}
	if fromEnd && len(keys) > count {
		plan.Bounds = full
		plan.Bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[count]}
		plan.Resolved = true
		return plan, nil
	}
	prefixReachesEnd := !fromEnd && edge.Right.Kind == datastore.BoundaryEnd
	suffixHasExactCount := fromEnd && len(keys) == count
	suffixReachesStart := fromEnd && edge.Left.Kind == datastore.BoundaryStart
	if prefixReachesEnd || suffixHasExactCount || suffixReachesStart {
		plan.Resolved = true
	}
	return plan, nil
}

// PlanExtension resolves as much of an extension as possible from contiguous
// ready coverage adjoining boundary. reverse extends the left boundary.
func (r *QueryRegistry) PlanExtension(fingerprint datastore.QueryFingerprint, boundary datastore.Boundary, count int, reverse bool) (ExtensionPlan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return ExtensionPlan{}, errors.New("live query not found")
	}
	if count < 1 {
		return ExtensionPlan{}, errors.New("extension count must be positive")
	}
	interval, covered, err := query.readyCover.adjacentInterval(boundary, reverse)
	if err != nil || !covered {
		return ExtensionPlan{}, err
	}
	// One extra key distinguishes an exact edge-sized result from truncated
	// coverage that continues toward the sentinel.
	limit := count + 1
	keys, err := query.members.sortKeysWithin(interval, limit, reverse)
	if err != nil {
		return ExtensionPlan{}, err
	}
	plan := ExtensionPlan{Available: true, CachedRows: min(len(keys), count)}
	if reverse {
		plan.Edge = interval.Left
		if len(keys) > count {
			plan.Boundary = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[count]}
			plan.Resolved = true
			return plan, nil
		}
		if len(keys) == count || interval.Left.Kind == datastore.BoundaryStart {
			plan.Boundary = interval.Left
			plan.Resolved = true
		}
		return plan, nil
	}
	plan.Edge = interval.Right
	if len(keys) >= count {
		if len(keys) == count && interval.Right.Kind == datastore.BoundaryEnd {
			plan.Boundary = interval.Right
		} else {
			plan.Boundary = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[count-1]}
		}
		plan.Resolved = true
		return plan, nil
	}
	if interval.Right.Kind == datastore.BoundaryEnd {
		plan.Boundary = interval.Right
		plan.Resolved = true
	}
	return plan, nil
}

// ReadyGaps returns only portions of bounds not backed by materialized ready
// coverage. The result is a disjoint ordered set of (left,right] intervals.
func (r *QueryRegistry) ReadyGaps(fingerprint datastore.QueryFingerprint, bounds datastore.Bounds) ([]datastore.Bounds, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil, errors.New("live query not found")
	}
	return query.readyCover.regionsWithCount(bounds, 0)
}

// PruneOutsideCoverage schedules physical cleanup. Maintain rechecks current
// coverage before removing each member; a new window can rescue queued ranges.
func (r *QueryRegistry) PruneOutsideCoverage(fingerprint datastore.QueryFingerprint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil
	}
	r.schedulePruneLocked(query)
	return nil
}

// RoutingFields returns the sparse set of normalized fields with active exact,
// range, coarse-watcher, or ordering registrations for this relation.
