package queryregistry

import (
	"errors"
	"hash/maphash"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

// orderedMembers is the shared ordered membership of one query fingerprint.
// Records remain in entitycache; this structure stores only entity keys and
// their immutable composite sort keys. A privately seeded treap provides
// expected logarithmic updates and ordered bounded iteration without a second
// row copy, even when entity keys are controlled by clients.
type orderedMembers struct {
	plan     datastore.PreparedQuery
	root     *memberNode
	byEntity map[storage.EntityKey]datastore.SortKey
	seed     maphash.Seed
}

type memberNode struct {
	entity   storage.EntityKey
	key      datastore.SortKey
	priority uint64
	left     *memberNode
	right    *memberNode
	size     int
}

func newOrderedMembers(plan datastore.PreparedQuery) *orderedMembers {
	return &orderedMembers{plan: plan, byEntity: make(map[storage.EntityKey]datastore.SortKey), seed: maphash.MakeSeed()}
}

func (m *orderedMembers) upsert(entity storage.EntityKey, key datastore.SortKey) (bool, error) {
	previous, exists := m.byEntity[entity]
	root := m.root
	if exists {
		compared, err := m.plan.Compare(previous, key)
		if err != nil {
			return false, err
		}
		if compared == 0 {
			return false, nil
		}
		var removed bool
		root, removed, err = m.removeNode(root, previous, entity)
		if err != nil {
			return false, err
		}
		if !removed {
			return false, errors.New("ordered query membership is inconsistent")
		}
	}
	node := &memberNode{entity: entity, key: key, priority: memberPriority(m.seed, entity), size: 1}
	root, err := m.insert(root, node)
	if err != nil {
		return false, err
	}
	m.root = root
	m.byEntity[entity] = key
	return !exists, nil
}

func (m *orderedMembers) remove(entity storage.EntityKey) (bool, error) {
	key, exists := m.byEntity[entity]
	if !exists {
		return false, nil
	}
	root, removed, err := m.removeNode(m.root, key, entity)
	if err != nil {
		return false, err
	}
	if !removed {
		return false, errors.New("ordered query membership is inconsistent")
	}
	m.root = root
	delete(m.byEntity, entity)
	return true, nil
}

func (m *orderedMembers) insert(root, node *memberNode) (*memberNode, error) {
	if root == nil {
		return node, nil
	}
	compared, err := m.compareMember(node.key, node.entity, root.key, root.entity)
	if err != nil {
		return nil, err
	}
	if compared == 0 {
		return root, nil
	}
	if node.priority < root.priority || node.priority == root.priority && entityLess(node.entity, root.entity) {
		left, right, err := m.split(root, node.key, node.entity)
		if err != nil {
			return nil, err
		}
		node.left, node.right = left, right
		refreshMemberNode(node)
		return node, nil
	}
	copy := *root
	root = &copy
	if compared < 0 {
		root.left, err = m.insert(root.left, node)
	} else {
		root.right, err = m.insert(root.right, node)
	}
	refreshMemberNode(root)
	return root, err
}

func (m *orderedMembers) split(root *memberNode, key datastore.SortKey, entity storage.EntityKey) (*memberNode, *memberNode, error) {
	if root == nil {
		return nil, nil, nil
	}
	compared, err := m.compareMember(root.key, root.entity, key, entity)
	if err != nil {
		return nil, nil, err
	}
	copy := *root
	root = &copy
	if compared < 0 {
		var right *memberNode
		root.right, right, err = m.split(root.right, key, entity)
		refreshMemberNode(root)
		return root, right, err
	}
	var left *memberNode
	left, root.left, err = m.split(root.left, key, entity)
	refreshMemberNode(root)
	return left, root, err
}

func (m *orderedMembers) removeNode(root *memberNode, key datastore.SortKey, entity storage.EntityKey) (*memberNode, bool, error) {
	if root == nil {
		return nil, false, nil
	}
	compared, err := m.compareMember(key, entity, root.key, root.entity)
	if err != nil {
		return nil, false, err
	}
	if compared == 0 {
		merged, err := m.merge(root.left, root.right)
		return merged, true, err
	}
	copy := *root
	root = &copy
	var removed bool
	if compared < 0 {
		root.left, removed, err = m.removeNode(root.left, key, entity)
	} else {
		root.right, removed, err = m.removeNode(root.right, key, entity)
	}
	refreshMemberNode(root)
	return root, removed, err
}

func (m *orderedMembers) merge(left, right *memberNode) (*memberNode, error) {
	if left == nil {
		return right, nil
	}
	if right == nil {
		return left, nil
	}
	var err error
	if left.priority < right.priority || left.priority == right.priority && entityLess(left.entity, right.entity) {
		copy := *left
		left = &copy
		left.right, err = m.merge(left.right, right)
		refreshMemberNode(left)
		return left, err
	}
	copy := *right
	right = &copy
	right.left, err = m.merge(left, right.left)
	refreshMemberNode(right)
	return right, err
}

// position returns an entity's zero-based rank within (bounds.Left,
// bounds.Right]. Subtree cardinalities make the lookup expected O(log N)
// without materializing the window.
func (m *orderedMembers) position(bounds datastore.Bounds, entity storage.EntityKey) (int, bool, error) {
	key, present := m.byEntity[entity]
	if !present {
		return 0, false, nil
	}
	contained, err := querywindow.Contains(m.plan, key, []datastore.Bounds{bounds})
	if err != nil || !contained {
		return 0, false, err
	}
	rank, err := m.countMemberBefore(m.root, key, entity)
	if err != nil {
		return 0, false, err
	}
	before := 0
	switch bounds.Left.Kind {
	case datastore.BoundaryStart:
	case datastore.BoundaryCursor:
		before, err = m.countAtOrBefore(m.root, bounds.Left.Key)
	case datastore.BoundaryEnd:
		return 0, false, nil
	default:
		return 0, false, errors.New("unknown left boundary kind")
	}
	if err != nil {
		return 0, false, err
	}
	return rank - before, true, nil
}

// compareMember extends the query's public cursor order with the entity key.
// The extension is private to the membership tree: cursors and boundaries
// deliberately continue to treat equal requested sort keys as one bucket.
func (m *orderedMembers) compareMember(leftKey datastore.SortKey, leftEntity storage.EntityKey, rightKey datastore.SortKey, rightEntity storage.EntityKey) (int, error) {
	compared, err := m.plan.Compare(leftKey, rightKey)
	if err != nil || compared != 0 {
		return compared, err
	}
	if leftEntity == rightEntity {
		return 0, nil
	}
	if entityLess(leftEntity, rightEntity) {
		return -1, nil
	}
	return 1, nil
}

func (m *orderedMembers) countMemberBefore(root *memberNode, key datastore.SortKey, entity storage.EntityKey) (int, error) {
	if root == nil {
		return 0, nil
	}
	compared, err := m.compareMember(root.key, root.entity, key, entity)
	if err != nil {
		return 0, err
	}
	if compared >= 0 {
		return m.countMemberBefore(root.left, key, entity)
	}
	right, err := m.countMemberBefore(root.right, key, entity)
	return memberNodeSize(root.left) + 1 + right, err
}

func (m *orderedMembers) countAtOrBefore(root *memberNode, key datastore.SortKey) (int, error) {
	if root == nil {
		return 0, nil
	}
	compared, err := m.plan.Compare(root.key, key)
	if err != nil {
		return 0, err
	}
	if compared > 0 {
		return m.countAtOrBefore(root.left, key)
	}
	right, err := m.countAtOrBefore(root.right, key)
	return memberNodeSize(root.left) + 1 + right, err
}

func memberNodeSize(node *memberNode) int {
	if node == nil {
		return 0
	}
	return node.size
}

func refreshMemberNode(node *memberNode) {
	if node != nil {
		node.size = 1 + memberNodeSize(node.left) + memberNodeSize(node.right)
	}
}

func (m *orderedMembers) all() []storage.EntityKey {
	result := make([]storage.EntityKey, 0, len(m.byEntity))
	walkAll(m.root, &result)
	return result
}

func (m *orderedMembers) within(bounds []datastore.Bounds) ([]storage.EntityKey, error) {
	// A tree contains each entity once. Deduplication is only necessary when
	// combining multiple potentially overlapping intervals.
	if len(bounds) == 1 {
		var result []storage.EntityKey
		err := m.walkChecked(m.root, bounds[0], &result)
		return result, err
	}
	seen := make(map[storage.EntityKey]struct{})
	var result []storage.EntityKey
	for _, interval := range bounds {
		var keys []storage.EntityKey
		if err := m.walkChecked(m.root, interval, &keys); err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, key)
		}
	}
	return result, nil
}

func (m *orderedMembers) sortKeysWithin(bounds datastore.Bounds, limit int, reverse bool) ([]datastore.SortKey, error) {
	result := make([]datastore.SortKey, 0, limit)
	if err := m.walkSortKeys(m.root, bounds, limit, reverse, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (m *orderedMembers) walkSortKeys(root *memberNode, bounds datastore.Bounds, limit int, reverse bool, result *[]datastore.SortKey) error {
	if root == nil || limit > 0 && len(*result) >= limit {
		return nil
	}
	afterLeft, err := querywindow.AfterLeft(m.plan, root.key, bounds.Left)
	if err != nil {
		return err
	}
	atOrBeforeRight, err := querywindow.AtOrBeforeRight(m.plan, root.key, bounds.Right)
	if err != nil {
		return err
	}
	first, second := root.left, root.right
	if reverse {
		first, second = second, first
	}
	if (!reverse && afterLeft) || (reverse && atOrBeforeRight) {
		if err := m.walkSortKeys(first, bounds, limit, reverse, result); err != nil {
			return err
		}
	}
	if afterLeft && atOrBeforeRight && (limit <= 0 || len(*result) < limit) {
		*result = append(*result, root.key)
	}
	if (!reverse && atOrBeforeRight) || (reverse && afterLeft) {
		if err := m.walkSortKeys(second, bounds, limit, reverse, result); err != nil {
			return err
		}
	}
	return nil
}

func (m *orderedMembers) walkChecked(root *memberNode, bounds datastore.Bounds, result *[]storage.EntityKey) error {
	if root == nil {
		return nil
	}
	afterLeft, err := querywindow.AfterLeft(m.plan, root.key, bounds.Left)
	if err != nil {
		return err
	}
	atOrBeforeRight, err := querywindow.AtOrBeforeRight(m.plan, root.key, bounds.Right)
	if err != nil {
		return err
	}
	if afterLeft {
		if err := m.walkChecked(root.left, bounds, result); err != nil {
			return err
		}
	}
	if afterLeft && atOrBeforeRight {
		*result = append(*result, root.entity)
	}
	if atOrBeforeRight {
		if err := m.walkChecked(root.right, bounds, result); err != nil {
			return err
		}
	}
	return nil
}

func walkAll(root *memberNode, result *[]storage.EntityKey) {
	if root == nil {
		return
	}
	walkAll(root.left, result)
	*result = append(*result, root.entity)
	walkAll(root.right, result)
}

func entityLess(left, right storage.EntityKey) bool {
	if left.Relation.Source != right.Relation.Source {
		return left.Relation.Source < right.Relation.Source
	}
	if left.Relation.Namespace != right.Relation.Namespace {
		return left.Relation.Namespace < right.Relation.Namespace
	}
	if left.Relation.Name != right.Relation.Name {
		return left.Relation.Name < right.Relation.Name
	}
	if left.Relation.OpaqueID != right.Relation.OpaqueID {
		return left.Relation.OpaqueID < right.Relation.OpaqueID
	}
	return left.Value < right.Value
}

func memberPriority(seed maphash.Seed, key storage.EntityKey) uint64 {
	return seededPriority(seed,
		[]byte(key.Relation.Source),
		[]byte(key.Relation.Namespace),
		[]byte(key.Relation.Name),
		[]byte(key.Relation.OpaqueID),
		[]byte(key.Value),
	)
}
