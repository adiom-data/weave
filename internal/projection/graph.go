package projection

import (
	"github.com/adiom-data/weave/internal/projection/retry"
	querysubscription "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// projectionGraph owns registered instances and their expansion lifetimes. It
// performs no IO, scheduling, or publication. Runtime code borrows instances
// under Projection.mu to apply row state or capture output; ownership transitions
// go through the graph methods. It shares that lock rather than adding another
// lock domain. Transitions concurrent with loading also require processMu.
type projectionGraph struct {
	instances   map[string]*projectionInstance
	rootID      string
	activeSlots int
	failed      map[string]*projectionInstance
}

type projectionInstance struct {
	id        string
	slot      *instanceSlot
	node      *ProjectionNode
	spec      storage.QuerySpec
	depth     int
	path      []string
	lineage   []PathStep
	parentID  string
	parentKey storage.EntityKey
	// opening owns this interest until the entire sibling expansion attaches.
	// Presence in instances alone does not make an obsolete load current.
	opening      *projectionExpansion
	subscription querysubscription.Subscription
	query        querysubscription.QuerySubscription
	initial      projectionInitialState
	order        []storage.EntityKey
	members      map[storage.EntityKey]*projectionMember
	// localEmpty is proved by an unmatched binding, without a source read.
	// Its parent remains part of the projection's snapshot/fencing checks.
	localEmpty bool
	// published is the structural publication boundary. Feed delivery may
	// advance an instance while its snapshot is loading, but no row from that
	// instance is exposed until a current image has introduced the instance.
	published  bool
	revision   uint64
	checkpoint *querysubscription.Position
	fence      *querysubscription.Position
	failure    error
	recovery   retry.State
	replaces   string // root predecessor, retired at the replacement publication
}

// snapshotPosition distinguishes source snapshots from locally derived empty
// topology and failed placeholders. Source-backed instances still require a
// real checkpoint or fence, even when their result contains no rows.
// The caller holds Projection.mu.
func (instance *projectionInstance) snapshotPosition() (*querysubscription.Position, error) {
	if instance.localEmpty || instance.failure != nil {
		return nil, nil
	}
	return querysubscription.SnapshotPosition(instance.checkpoint, instance.fence)
}

// A member owns one immutable row image and its expansion state, all guarded
// by Projection.mu. Row images are replaced on upsert; expansion tokens change
// only when selectors change or the row is removed/readded.
type projectionMember struct {
	record         storage.Record
	version        querysubscription.RecordVersion
	cursor         []byte
	pending        bool
	children       []string
	expanded       bool // An empty completed expansion differs from one not yet opened.
	expansionToken *expansionGeneration
	// Bindings are computed once from this immutable image. Expansion workers
	// and the feed consumer may request them concurrently.
	bindings *memberBindings
	// Published predecessors remain resident and count toward instance limits
	// until the replacement for their particular child slot is published.
	replacementRoots map[*ProjectionNode][]string
}

// Equivalent immutable row images share this selector generation. The loading
// signal serializes inline and queued passes without holding actor locks on IO.
// All access is guarded by Projection.mu.
type expansionGeneration struct{ loading chan struct{} }

func newProjectionMember(record storage.Record, version querysubscription.RecordVersion, cursor []byte, pending bool) *projectionMember {
	return &projectionMember{record: storage.CloneRecord(record), version: version, cursor: append([]byte(nil), cursor...), pending: pending, expansionToken: &expansionGeneration{}}
}

// capture requires Projection.mu and owns its payloads independently of future
// updates or expansion.
func (m *projectionMember) capture() projectionMember {
	return projectionMember{record: storage.CloneRecord(m.record), version: m.version, cursor: append([]byte(nil), m.cursor...), pending: m.pending, children: append([]string(nil), m.children...), expanded: m.expanded}
}

// projectionInitialState is the common snapshot information needed while a
// projection instance is opening. Bounds are meaningful only for query nodes;
// entity nodes do not manufacture a query snapshot merely to share this path.
type projectionInitialState struct {
	Bounds     querysubscription.Bounds
	Rows       []querysubscription.QueryRow
	Checkpoint *querysubscription.Position
	Fence      *querysubscription.Position
	Revision   uint64
}

type projectionExpansion struct {
	parent   *projectionInstance
	key      storage.EntityKey
	member   *projectionMember
	revision uint64
}

// expansionCurrentLocked fences both replacement rows and completed loads.
// Member identities cannot be reused while an older worker still holds one.
func (g *projectionGraph) expansionCurrentLocked(expansion projectionExpansion) bool {
	return expansion.member != nil && g.instanceCurrentLocked(expansion.parent) &&
		expansion.parent.members[expansion.key] != nil &&
		expansion.parent.members[expansion.key].expansionToken == expansion.member.expansionToken &&
		!expansion.parent.members[expansion.key].pending && !expansion.parent.members[expansion.key].expanded
}

// initializeRowsLocked transfers a newly installed snapshot into graph members
// and returns the child expansion work. The runtime decides when to perform IO.
func (g *projectionGraph) initializeRowsLocked(instance *projectionInstance) []projectionExpansion {
	var opening []projectionExpansion
	for _, entity := range instance.initial.Rows {
		member := newProjectionMember(entity.Record, querysubscription.RecordVersion{Revision: instance.initial.Revision, ChangeIndex: entity.ChangeIndex}, entity.Cursor, entity.Pending)
		instance.members[entity.Key] = member
		instance.order = append(instance.order, entity.Key)
		if !member.pending && len(instance.node.Children) > 0 {
			opening = append(opening, projectionExpansion{parent: instance, key: entity.Key, member: member})
		}
	}
	instance.initial.Rows = nil
	return opening
}

type instanceRetirement struct {
	subscription querysubscription.Subscription
	children     []string
}

// retireLocked detaches ownership once. Closing subscriptions and recursively
// retiring children is runtime work, performed outside the graph lock.
func (g *projectionGraph) retireLocked(id string) instanceRetirement {
	instance := g.instances[id]
	if instance == nil {
		return instanceRetirement{}
	}
	delete(g.instances, id)
	delete(g.failed, id)
	g.releaseInstanceSlotLocked(instance)
	retirement := instanceRetirement{subscription: instance.takeSubscriptionLocked()}
	for _, member := range instance.members {
		retirement.children = append(retirement.children, member.children...)
		for _, roots := range member.replacementRoots {
			retirement.children = append(retirement.children, roots...)
		}
	}
	return retirement
}

func newProjectionGraph() projectionGraph {
	return projectionGraph{instances: make(map[string]*projectionInstance), failed: make(map[string]*projectionInstance)}
}

// takeSubscriptionLocked transfers the sole Close responsibility and disables
// future window requests atomically. The caller holds Projection.mu and must
// close the returned handle outside that lock.
func (instance *projectionInstance) takeSubscriptionLocked() querysubscription.Subscription {
	handle := instance.subscription
	instance.subscription, instance.query = nil, nil
	return handle
}
