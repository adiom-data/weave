package subscriptions

import (
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type State struct {
	Reference     datastore.PreparedReference
	Fingerprint   datastore.QueryFingerprint
	Plan          datastore.PreparedQuery
	Bounds        datastore.Bounds
	RelationEpoch uint64
}

type Initialization struct {
	Fingerprint datastore.QueryFingerprint
	Relation    storage.RelationKey
	Epoch       uint64
	Phase       OperationPhase
	Bounds      datastore.Bounds
	Reads       []datastore.Bounds
	Fence       storage.Fence
}

type OperationPhase uint8

const (
	// OperationPlanning permits cache inspection and boundary discovery.
	OperationPlanning OperationPhase = iota + 1
	// OperationMaterializing has pinned its target and must receive a fenced
	// authoritative read before it can commit.
	OperationMaterializing
	// OperationReady has either proven ready cache coverage or activated and
	// reconciled its required materialization read.
	OperationReady
)

type WindowOperation struct {
	Token  string
	Phase  OperationPhase
	Bounds datastore.Bounds
	Reads  []datastore.Bounds
	Fence  storage.Fence
}

// Registry owns coordinator-local subscription lifecycle state. It is actor
// confined and intentionally does not use internal locks.
type Registry struct {
	active       map[string]State
	initializing map[string]Initialization
	windows      map[string]WindowOperation
	epochs       map[storage.RelationKey]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		active:       make(map[string]State),
		initializing: make(map[string]Initialization),
		windows:      make(map[string]WindowOperation),
		epochs:       make(map[storage.RelationKey]uint64),
	}
}

func (r *Registry) Exists(id string) bool {
	_, active := r.active[id]
	_, initializing := r.initializing[id]
	return active || initializing
}

func (r *Registry) BeginInitialization(id string, plan datastore.PreparedQuery) {
	r.initializing[id] = Initialization{Fingerprint: plan.Fingerprint(), Relation: plan.Relation(), Epoch: r.epochs[plan.Relation()], Phase: OperationPlanning}
}

func (r *Registry) Initialization(id string) (Initialization, bool) {
	state, ok := r.initializing[id]
	state.Reads = append([]datastore.Bounds(nil), state.Reads...)
	return state, ok
}

func (r *Registry) InitializationCurrent(id string, plan datastore.PreparedQuery) bool {
	state, ok := r.initializing[id]
	return ok && state.Fingerprint == plan.Fingerprint() && state.Relation == plan.Relation() && state.Epoch == r.epochs[plan.Relation()]
}

func (r *Registry) InitializationInPhase(id string, plan datastore.PreparedQuery, phase OperationPhase) bool {
	state, ok := r.initializing[id]
	return ok && state.Fingerprint == plan.Fingerprint() && state.Relation == plan.Relation() && state.Epoch == r.epochs[plan.Relation()] && state.Phase == phase
}

func (r *Registry) StageInitialization(id string, plan datastore.PreparedQuery, bounds datastore.Bounds, reads []datastore.Bounds) bool {
	if !r.InitializationInPhase(id, plan, OperationPlanning) {
		return false
	}
	state := r.initializing[id]
	state.Bounds = bounds
	state.Reads = append([]datastore.Bounds(nil), reads...)
	state.Phase = OperationReady
	if len(reads) > 0 {
		state.Phase = OperationMaterializing
	}
	r.initializing[id] = state
	return true
}

func (r *Registry) ReadyInitialization(id string, plan datastore.PreparedQuery, fence storage.Fence) bool {
	if fence == nil || !r.InitializationInPhase(id, plan, OperationMaterializing) {
		return false
	}
	state := r.initializing[id]
	state.Phase = OperationReady
	state.Fence = fence
	r.initializing[id] = state
	return true
}

func (r *Registry) CancelInitialization(id string, fingerprint datastore.QueryFingerprint) bool {
	state, ok := r.initializing[id]
	if !ok || state.Fingerprint != fingerprint {
		return false
	}
	delete(r.initializing, id)
	return true
}

func (r *Registry) Activate(id string, state State) {
	r.active[id] = state
	delete(r.initializing, id)
}

func (r *Registry) Active(id string) (State, bool) {
	state, ok := r.active[id]
	return state, ok
}

func (r *Registry) ActiveIDs(fingerprint datastore.QueryFingerprint) []string {
	var result []string
	for id, state := range r.active {
		if state.Fingerprint == fingerprint {
			result = append(result, id)
		}
	}
	return result
}

func (r *Registry) RemoveActive(id string) {
	delete(r.active, id)
	delete(r.windows, id)
}

func (r *Registry) BeginWindow(id, token string) (State, bool) {
	state, ok := r.active[id]
	if _, exists := r.windows[id]; !ok || exists {
		return State{}, false
	}
	state.RelationEpoch = r.epochs[state.Plan.Relation()]
	r.active[id] = state
	r.windows[id] = WindowOperation{Token: token, Phase: OperationPlanning}
	return state, true
}

func (r *Registry) WindowMatches(id, token string) bool {
	operation, ok := r.windows[id]
	return ok && token != "" && operation.Token == token
}

func (r *Registry) WindowInPhase(id, token string, phase OperationPhase) bool {
	operation, ok := r.windows[id]
	return ok && token != "" && operation.Token == token && operation.Phase == phase
}

func (r *Registry) StageWindow(id, token string, bounds datastore.Bounds, reads []datastore.Bounds) bool {
	if !r.WindowInPhase(id, token, OperationPlanning) {
		return false
	}
	operation := r.windows[id]
	operation.Bounds = bounds
	operation.Reads = append([]datastore.Bounds(nil), reads...)
	operation.Phase = OperationReady
	if len(reads) > 0 {
		operation.Phase = OperationMaterializing
	}
	r.windows[id] = operation
	return true
}

func (r *Registry) ReadyWindow(id, token string, fence storage.Fence) bool {
	if fence == nil || !r.WindowInPhase(id, token, OperationMaterializing) {
		return false
	}
	operation := r.windows[id]
	operation.Phase = OperationReady
	operation.Fence = fence
	r.windows[id] = operation
	return true
}

func (r *Registry) WindowOperation(id, token string) (WindowOperation, bool) {
	operation, ok := r.windows[id]
	operation.Reads = append([]datastore.Bounds(nil), operation.Reads...)
	return operation, ok && token != "" && operation.Token == token
}

func (r *Registry) FinishWindow(id, token string, state State) bool {
	if !r.WindowInPhase(id, token, OperationReady) {
		return false
	}
	r.active[id] = state
	delete(r.windows, id)
	return true
}

func (r *Registry) AbortWindow(id, token string) bool {
	if !r.WindowMatches(id, token) {
		return false
	}
	delete(r.windows, id)
	return true
}

func (r *Registry) WindowToken(id string) string {
	return r.windows[id].Token
}
func (r *Registry) Epoch(relation storage.RelationKey) uint64 { return r.epochs[relation] }
func (r *Registry) BumpEpoch(relation storage.RelationKey) uint64 {
	r.epochs[relation]++
	return r.epochs[relation]
}

func (r *Registry) InitializationIDs(fingerprint datastore.QueryFingerprint) []string {
	var result []string
	for id, state := range r.initializing {
		if state.Fingerprint == fingerprint {
			result = append(result, id)
		}
	}
	return result
}
