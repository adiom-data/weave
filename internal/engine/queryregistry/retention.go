package queryregistry

import (
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

type retainedWindow struct {
	query  *liveQuery
	bounds datastore.Bounds
}
type pruneJob struct {
	query   *liveQuery
	bounds  datastore.Bounds
	root    *memberNode
	stack   []*memberNode
	started bool
}
type MaintenanceFailure struct {
	Fingerprint datastore.QueryFingerprint
	Cause       error
}

func (r *QueryRegistry) retainLocked(query *liveQuery, bounds []datastore.Bounds) {
	expires := r.now().Add(r.idleTTL)
	for _, interval := range bounds {
		r.nextRetention++
		id := r.nextRetention
		query.retained[id] = interval
		r.retained[id] = &retainedWindow{query: query, bounds: interval}
		r.expiry.Set(id, expires)
	}
}

func (r *QueryRegistry) schedulePruneLocked(query *liveQuery) {
	for _, bounds := range query.prune {
		r.pruning = append(r.pruning, &pruneJob{query: query, bounds: bounds, root: query.members.root})
		query.pruneJobs++
	}
	query.prune = nil
}

// Discard immediately tears down a failed query, including its retention.
// Unlike normal Release, it must not rely on the plan remaining evaluable.
func (r *QueryRegistry) Discard(fingerprint datastore.QueryFingerprint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.discardLocked(fingerprint)
	return err
}

// Maintain expires delivery-less windows and prunes at most budget old
// membership positions. Physical ownership stays in the live query until each
// removal, so delete/TRUNCATE/recovery can still find and release it safely.
func (r *QueryRegistry) Maintain(now time.Time, budget int) (failures []MaintenanceFailure, more bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for n := 0; n < budget; n++ {
		id, expires, ok := r.expiry.First()
		if !ok || expires.After(now) {
			break
		}
		window := r.retained[id]
		query := window.query
		coverage, ready := query.coverage.clone(), query.readyCover.clone()
		err := coverage.remove(window.bounds)
		if err == nil {
			err = ready.remove(window.bounds)
		}
		var lost []datastore.Bounds
		if err == nil {
			lost, err = coverage.regionsWithCount(window.bounds, 0)
		}
		r.expiry.Remove(id)
		delete(r.retained, id)
		delete(query.retained, id)
		if err != nil {
			failures = append(failures, MaintenanceFailure{query.plan.Fingerprint(), err})
			continue
		}
		query.coverage, query.readyCover = coverage, ready
		query.prune = append(query.prune, lost...)
		// An empty job also retires a now-unowned, empty query.
		if len(query.prune) == 0 && query.references == 0 && len(query.retained) == 0 {
			query.prune = append(query.prune, window.bounds)
		}
		r.schedulePruneLocked(query)
	}
	for n := 0; n < budget && len(r.pruning) > 0; n++ {
		job := r.pruning[0]
		query := job.query
		fingerprint := query.plan.Fingerprint()
		var err error
		var key storage.EntityKey
		var found bool
		current := r.queries[fingerprint] == query
		if current {
			key, found, err = job.next()
		}
		if err == nil && found {
			if sortKey, exists := query.members.byEntity[key]; exists {
				var covered bool
				covered, err = query.coverage.contains(sortKey)
				if err == nil && !covered {
					_, err = r.removeMemberLocked(query, key)
				}
			}
		}
		if err != nil || !found {
			r.pruning[0] = nil
			r.pruning = r.pruning[1:]
			query.pruneJobs--
			if err == nil && current && query.references == 0 && len(query.retained) == 0 && query.pruneJobs == 0 {
				_, err = r.releaseLocked(fingerprint)
			}
		}
		if err != nil {
			failures = append(failures, MaintenanceFailure{fingerprint, err})
		}
	}
	_, expires, due := r.expiry.First()
	return failures, len(r.pruning) > 0 || due && !expires.After(now)
}

func (j *pruneJob) next() (storage.EntityKey, bool, error) {
	push := func(root *memberNode) error {
		for root != nil {
			after, err := querywindow.AfterLeft(j.query.plan, root.key, j.bounds.Left)
			if err != nil {
				return err
			}
			if after {
				j.stack = append(j.stack, root)
				root = root.left
			} else {
				root = root.right
			}
		}
		return nil
	}
	if !j.started {
		j.started = true
		if err := push(j.root); err != nil {
			return storage.EntityKey{}, false, err
		}
		j.root = nil
	}
	if len(j.stack) == 0 {
		return storage.EntityKey{}, false, nil
	}
	node := j.stack[len(j.stack)-1]
	j.stack = j.stack[:len(j.stack)-1]
	inside, err := querywindow.AtOrBeforeRight(j.query.plan, node.key, j.bounds.Right)
	if err != nil || !inside {
		j.stack = nil
		return storage.EntityKey{}, false, err
	}
	if err := push(node.right); err != nil {
		return storage.EntityKey{}, false, err
	}
	return node.entity, true, nil
}
