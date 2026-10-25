package selectorindex

import (
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

func (r *Index) HasRelation(relation storage.RelationKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byRelation[relation]) > 0 || r.QueryRegistry.HasRelation(relation)
}

func (r *Index) RelationCandidates(relation storage.RelationKey) []queryregistry.QueryDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.QueryRegistry.RelationCandidates(relation)
	for fp := range r.byRelation[relation] {
		result = append(result, descriptor(r.identities[fp]))
	}
	return result
}
func (r *Index) Candidates(relation storage.RelationKey, key storage.EntityKey, probes []routing.RoutingProbe, fallback bool) []queryregistry.QueryDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.QueryRegistry.Candidates(relation, key, probes, fallback)
	for fp := range r.byKey[key] {
		result = append(result, descriptor(r.identities[fp]))
	}
	return result
}
func (r *Index) IsMember(fp datastore.QueryFingerprint, key storage.EntityKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		return e.key == key && e.member
	}
	return r.QueryRegistry.IsMember(fp, key)
}
func (r *Index) AddMember(fp datastore.QueryFingerprint, key storage.EntityKey) (entitycache.EntitySnapshot, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		return r.add(e, key)
	}
	return r.QueryRegistry.AddMember(fp, key)
}
func (r *Index) RemoveMember(fp datastore.QueryFingerprint, key storage.EntityKey) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if key != e.key {
			return false, nil
		}
		return r.remove(e)
	}
	return r.QueryRegistry.RemoveMember(fp, key)
}
func (r *Index) RefreshMember(fp datastore.QueryFingerprint, key storage.EntityKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if key != e.key || !e.member {
			return errors.New("identity member not found")
		}
		return nil
	}
	return r.QueryRegistry.RefreshMember(fp, key)
}
func (r *Index) Members(fp datastore.QueryFingerprint) []storage.EntityKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if e.member {
			return []storage.EntityKey{e.key}
		}
		return nil
	}
	return r.QueryRegistry.Members(fp)
}
func (r *Index) RemoveEntity(key storage.EntityKey) ([]datastore.QueryFingerprint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, err := r.QueryRegistry.RemoveEntity(key)
	if err != nil {
		return nil, err
	}
	for fp := range r.byKey[key] {
		if removed, err := r.remove(r.identities[fp]); err != nil {
			return nil, err
		} else if removed {
			result = append(result, fp)
		}
	}
	return result, nil
}
func (r *Index) ClearRelation(relation storage.RelationKey) ([]datastore.QueryFingerprint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, err := r.QueryRegistry.ClearRelation(relation)
	if err != nil {
		return nil, err
	}
	for fp := range r.byRelation[relation] {
		e := r.identities[fp]
		if _, err := r.remove(e); err != nil {
			return nil, err
		}
		result = append(result, fp)
	}
	return result, nil
}
func (r *Index) CoversKey(fp datastore.QueryFingerprint, key datastore.SortKey) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identities[fp] != nil {
		return true, nil
	}
	return r.QueryRegistry.CoversKey(fp, key)
}
func (r *Index) InstallWindow(fp datastore.QueryFingerprint, id string, bounds datastore.Bounds) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if !fullBounds(bounds) {
			return false, errors.New("identity selector has no cursor window")
		}
		e.owners[id] = e.ready
		return e.ready, nil
	}
	return r.QueryRegistry.InstallWindow(fp, id, bounds)
}
func (r *Index) SetWindow(fp datastore.QueryFingerprint, id string, bounds datastore.Bounds) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if !fullBounds(bounds) {
			return errors.New("identity selector has no cursor window")
		}
		e.owners[id] = true
		e.ready = true
		return nil
	}
	return r.QueryRegistry.SetWindow(fp, id, bounds)
}
func (r *Index) MarkWindowReady(fp datastore.QueryFingerprint, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if _, exists := e.owners[id]; !exists {
			return errors.New("identity owner missing")
		}
		e.owners[id] = true
		e.ready = true
		return nil
	}
	return r.QueryRegistry.MarkWindowReady(fp, id)
}
func (r *Index) RemoveWindow(fp datastore.QueryFingerprint, id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		_, exists := e.owners[id]
		delete(e.owners, id)
		return exists, nil
	}
	return r.QueryRegistry.RemoveWindow(fp, id)
}
func (r *Index) Coverage(fp datastore.QueryFingerprint) []datastore.Bounds {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identities[fp] != nil {
		return []datastore.Bounds{full()}
	}
	return r.QueryRegistry.Coverage(fp)
}
func (r *Index) ReadyGaps(fp datastore.QueryFingerprint, bounds datastore.Bounds) ([]datastore.Bounds, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if !fullBounds(bounds) {
			return nil, errors.New("identity selector has no cursor window")
		}
		if e.ready {
			return nil, nil
		}
		return []datastore.Bounds{full()}, nil
	}
	return r.QueryRegistry.ReadyGaps(fp, bounds)
}
func (r *Index) PruneOutsideCoverage(fp datastore.QueryFingerprint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identities[fp] != nil {
		return nil
	}
	return r.QueryRegistry.PruneOutsideCoverage(fp)
}
func (r *Index) PlanInitialWindow(fp datastore.QueryFingerprint, count int, reverse bool) (queryregistry.InitialWindowPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.identities[fp]
	if e == nil {
		return r.QueryRegistry.PlanInitialWindow(fp, count, reverse)
	}
	if !e.ready {
		if row, exists := r.entities.Get(e.key); exists && row.State == entitycache.EntityCurrent {
			matched, err := e.plan.Matches(row.Record)
			if err != nil {
				return queryregistry.InitialWindowPlan{}, err
			}
			if matched {
				if _, _, err := r.add(e, e.key); err != nil {
					return queryregistry.InitialWindowPlan{}, err
				}
			}
			e.ready = true
		}
	}
	return queryregistry.InitialWindowPlan{Bounds: full(), Available: e.ready, Resolved: true}, nil
}
func (r *Index) PlanExtension(fp datastore.QueryFingerprint, boundary datastore.Boundary, count int, reverse bool) (queryregistry.ExtensionPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identities[fp] != nil {
		return queryregistry.ExtensionPlan{}, errors.New("identity selector cannot resize")
	}
	return r.QueryRegistry.PlanExtension(fp, boundary, count, reverse)
}
func (r *Index) PositionInBounds(fp datastore.QueryFingerprint, bounds datastore.Bounds, key storage.EntityKey) (int, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		if !fullBounds(bounds) {
			return 0, false, errors.New("identity selector has no cursor window")
		}
		return 0, e.member && e.key == key, nil
	}
	return r.QueryRegistry.PositionInBounds(fp, bounds, key)
}
func (r *Index) EntityKeysInBounds(fp datastore.QueryFingerprint, bounds []datastore.Bounds) ([]storage.EntityKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		for _, b := range bounds {
			if !fullBounds(b) {
				return nil, errors.New("identity selector has no cursor window")
			}
		}
		if e.member && len(bounds) > 0 {
			return []storage.EntityKey{e.key}, nil
		}
		return nil, nil
	}
	return r.QueryRegistry.EntityKeysInBounds(fp, bounds)
}

func (r *Index) EntitiesInBounds(fp datastore.QueryFingerprint, bounds []datastore.Bounds) ([]entitycache.EntitySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.identities[fp]
	if e == nil {
		return r.QueryRegistry.EntitiesInBounds(fp, bounds)
	}
	for _, b := range bounds {
		if !fullBounds(b) {
			return nil, errors.New("identity selector has no cursor window")
		}
	}
	if !e.member || len(bounds) == 0 {
		return nil, nil
	}
	row, exists := r.entities.Get(e.key)
	if !exists {
		return nil, errors.New("identity member missing from cache")
	}
	return []entitycache.EntitySnapshot{row}, nil
}
func (r *Index) SortKeysInBounds(fp datastore.QueryFingerprint, bounds datastore.Bounds, limit int, reverse bool) ([]datastore.SortKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.identities[fp]
	if e == nil {
		return r.QueryRegistry.SortKeysInBounds(fp, bounds, limit, reverse)
	}
	if !fullBounds(bounds) {
		return nil, errors.New("identity selector has no cursor window")
	}
	if !e.member {
		return nil, nil
	}
	row, exists := r.entities.Get(e.key)
	if !exists {
		return nil, errors.New("identity member missing from cache")
	}
	key, err := e.plan.Key(row.Record)
	if err != nil {
		return nil, err
	}
	return []datastore.SortKey{key}, nil
}
