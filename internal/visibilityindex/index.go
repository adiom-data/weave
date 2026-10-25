// Package visibilityindex maintains an ordered sequence of entity occurrences
// together with their connection-local visibility. It supports positional raw
// membership changes and visible rank queries without materializing either
// ordering.
package visibilityindex

import (
	"errors"
	"hash/maphash"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/treappriority"
)

// Index is an implicit treap. In-order traversal is raw query order; subtree
// cardinalities support positional edits and visible-rank queries in expected
// logarithmic time.
type Index struct {
	root  *node
	nodes map[storage.EntityKey]*node
	seed  maphash.Seed
}

type node struct {
	key          storage.EntityKey
	priority     uint64
	visible      bool
	size         int
	visibleCount int
	parent       *node
	left         *node
	right        *node
}

func New() *Index {
	return &Index{nodes: make(map[storage.EntityKey]*node), seed: maphash.MakeSeed()}
}

func (i *Index) Len() int { return nodeSize(i.root) }

func (i *Index) Contains(key storage.EntityKey) bool {
	_, ok := i.nodes[key]
	return ok
}

func (i *Index) Visible(key storage.EntityKey) (visible, present bool) {
	n := i.nodes[key]
	if n == nil {
		return false, false
	}
	return n.visible, true
}

// VisibleRank returns the zero-based position among visible occurrences.
func (i *Index) VisibleRank(key storage.EntityKey) (int, bool) {
	n := i.nodes[key]
	if n == nil || !n.visible {
		return 0, false
	}
	rank := nodeVisibleCount(n.left)
	for n.parent != nil {
		if n == n.parent.right {
			rank += nodeVisibleCount(n.parent.left)
			if n.parent.visible {
				rank++
			}
		}
		n = n.parent
	}
	return rank, true
}

func (i *Index) SetVisible(key storage.EntityKey, visible bool) (bool, error) {
	n := i.nodes[key]
	if n == nil {
		return false, errors.New("visibility index does not contain key")
	}
	if n.visible == visible {
		return false, nil
	}
	n.visible = visible
	for current := n; current != nil; current = current.parent {
		refresh(current)
	}
	return true, nil
}

func (i *Index) Append(key storage.EntityKey, visible bool) error {
	if _, exists := i.nodes[key]; exists {
		return errors.New("visibility index already contains key")
	}
	n := i.newNode(key, visible)
	i.root = merge(i.root, n)
	setParent(i.root, nil)
	i.nodes[key] = n
	return nil
}

func (i *Index) Remove(key storage.EntityKey) (bool, error) {
	if _, exists := i.nodes[key]; !exists {
		return false, nil
	}
	if _, err := i.remove(key); err != nil {
		return false, err
	}
	return true, nil
}

// ApplyMembership applies the shared query's raw membership delta. A key may
// occur in both Removed and Insertions to represent a move; its visibility is
// retained. Newly inserted keys start hidden until the caller evaluates them.
func (i *Index) ApplyMembership(delta querysubscription.MembershipDelta) error {
	removed := make(map[storage.EntityKey]struct{}, len(delta.Removed))
	for _, key := range delta.Removed {
		if _, duplicate := removed[key]; duplicate {
			return errors.New("visibility membership removes a key more than once")
		}
		if _, present := i.nodes[key]; !present {
			return errors.New("visibility membership removes an absent key")
		}
		removed[key] = struct{}{}
	}
	finalLength := i.Len() - len(removed) + len(delta.Insertions)
	inserted := make(map[storage.EntityKey]struct{}, len(delta.Insertions))
	previousIndex := -1
	for _, insertion := range delta.Insertions {
		if insertion.FinalIndex < 0 || insertion.FinalIndex >= finalLength {
			return errors.New("visibility membership insertion index is out of bounds")
		}
		if insertion.FinalIndex <= previousIndex {
			return errors.New("visibility membership insertions are not in strictly increasing final-index order")
		}
		if _, duplicate := inserted[insertion.Key]; duplicate {
			return errors.New("visibility membership inserts a key more than once")
		}
		if _, present := i.nodes[insertion.Key]; present {
			if _, moving := removed[insertion.Key]; !moving {
				return errors.New("visibility membership inserts an existing key")
			}
		}
		inserted[insertion.Key] = struct{}{}
		previousIndex = insertion.FinalIndex
	}

	detached := make(map[storage.EntityKey]*node, len(removed))
	for _, key := range delta.Removed {
		n, err := i.remove(key)
		if err != nil {
			return err
		}
		detached[key] = n
	}
	for _, insertion := range delta.Insertions {
		n := detached[insertion.Key]
		if n == nil {
			n = i.newNode(insertion.Key, false)
		}
		if err := i.insertAt(insertion.FinalIndex, n); err != nil {
			return err
		}
	}
	return nil
}

func (i *Index) Keys() []storage.EntityKey {
	result := make([]storage.EntityKey, 0, i.Len())
	walk(i.root, func(n *node) { result = append(result, n.key) })
	return result
}

func (i *Index) VisibleKeys() []storage.EntityKey {
	result := make([]storage.EntityKey, 0, nodeVisibleCount(i.root))
	walk(i.root, func(n *node) {
		if n.visible {
			result = append(result, n.key)
		}
	})
	return result
}

func (i *Index) newNode(key storage.EntityKey, visible bool) *node {
	priority := treappriority.Seeded(i.seed,
		[]byte(key.Relation.Source),
		[]byte(key.Relation.Namespace),
		[]byte(key.Relation.Name),
		[]byte(key.Relation.OpaqueID),
		[]byte(key.Value),
	)
	n := &node{key: key, priority: priority, visible: visible, size: 1}
	if visible {
		n.visibleCount = 1
	}
	return n
}

func (i *Index) remove(key storage.EntityKey) (*node, error) {
	n := i.nodes[key]
	if n == nil {
		return nil, errors.New("visibility index does not contain key")
	}
	rank := rawRank(n)
	left, rest := split(i.root, rank)
	middle, right := split(rest, 1)
	if middle == nil || middle.key != key || middle.left != nil || middle.right != nil {
		return nil, errors.New("visibility index is inconsistent")
	}
	i.root = merge(left, right)
	setParent(i.root, nil)
	delete(i.nodes, key)
	middle.parent = nil
	return middle, nil
}

func (i *Index) insertAt(index int, n *node) error {
	if index < 0 || index > i.Len() {
		return errors.New("visibility index insertion position is out of bounds")
	}
	if _, exists := i.nodes[n.key]; exists {
		return errors.New("visibility index already contains key")
	}
	n.left, n.right, n.parent = nil, nil, nil
	refresh(n)
	left, right := split(i.root, index)
	i.root = merge(merge(left, n), right)
	setParent(i.root, nil)
	i.nodes[n.key] = n
	return nil
}

func rawRank(n *node) int {
	rank := nodeSize(n.left)
	for n.parent != nil {
		if n == n.parent.right {
			rank += nodeSize(n.parent.left) + 1
		}
		n = n.parent
	}
	return rank
}

func split(root *node, count int) (*node, *node) {
	if root == nil {
		return nil, nil
	}
	if nodeSize(root.left) >= count {
		left, remainder := split(root.left, count)
		root.left = remainder
		setParent(remainder, root)
		refresh(root)
		setParent(left, nil)
		setParent(root, nil)
		return left, root
	}
	remainder, right := split(root.right, count-nodeSize(root.left)-1)
	root.right = remainder
	setParent(remainder, root)
	refresh(root)
	setParent(root, nil)
	setParent(right, nil)
	return root, right
}

func merge(left, right *node) *node {
	if left == nil {
		setParent(right, nil)
		return right
	}
	if right == nil {
		setParent(left, nil)
		return left
	}
	if left.priority < right.priority || left.priority == right.priority && keyLess(left.key, right.key) {
		left.right = merge(left.right, right)
		setParent(left.right, left)
		refresh(left)
		left.parent = nil
		return left
	}
	right.left = merge(left, right.left)
	setParent(right.left, right)
	refresh(right)
	right.parent = nil
	return right
}

func refresh(n *node) {
	if n == nil {
		return
	}
	n.size = nodeSize(n.left) + 1 + nodeSize(n.right)
	n.visibleCount = nodeVisibleCount(n.left) + nodeVisibleCount(n.right)
	if n.visible {
		n.visibleCount++
	}
}

func nodeSize(n *node) int {
	if n == nil {
		return 0
	}
	return n.size
}

func nodeVisibleCount(n *node) int {
	if n == nil {
		return 0
	}
	return n.visibleCount
}

func setParent(n, parent *node) {
	if n != nil {
		n.parent = parent
	}
}

func walk(n *node, visit func(*node)) {
	if n == nil {
		return
	}
	walk(n.left, visit)
	visit(n)
	walk(n.right, visit)
}

func keyLess(left, right storage.EntityKey) bool {
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
