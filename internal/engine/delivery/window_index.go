package delivery

import (
	"hash/maphash"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
)

// windowIndex is an interval treap for one query fingerprint. Its max-right
// augmentation supports point stabbing in O(log n + k), where k is the number
// of overlapping client windows.
type windowIndex struct {
	plan datastore.PreparedQuery
	root *windowNode
	seed maphash.Seed
}

type windowNode struct {
	id       string
	bounds   datastore.Bounds
	maxRight datastore.Boundary
	priority uint64
	left     *windowNode
	right    *windowNode
}

func newWindowIndex(plan datastore.PreparedQuery) *windowIndex {
	return &windowIndex{plan: plan, seed: maphash.MakeSeed()}
}

func (i *windowIndex) add(id string, bounds datastore.Bounds) error {
	node := &windowNode{id: id, bounds: bounds, maxRight: bounds.Right, priority: windowPriority(i.seed, id)}
	root, err := i.insert(i.root, node)
	if err != nil {
		return err
	}
	i.root = root
	return nil
}

func (i *windowIndex) replace(id string, old, next datastore.Bounds) error {
	root, err := i.remove(i.root, old.Left, id)
	if err != nil {
		return err
	}
	node := &windowNode{id: id, bounds: next, maxRight: next.Right, priority: windowPriority(i.seed, id)}
	root, err = i.insert(root, node)
	if err != nil {
		return err
	}
	i.root = root
	return nil
}

func (i *windowIndex) delete(id string, bounds datastore.Bounds) error {
	root, err := i.remove(i.root, bounds.Left, id)
	if err != nil {
		return err
	}
	i.root = root
	return nil
}

func (i *windowIndex) containing(key datastore.SortKey) ([]string, error) {
	var result []string
	if err := i.collect(i.root, key, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (i *windowIndex) collect(root *windowNode, key datastore.SortKey, result *[]string) error {
	if root == nil {
		return nil
	}
	if root.left != nil {
		compared, err := querywindow.CompareBoundary(i.plan, root.left.maxRight, datastore.Boundary{Kind: datastore.BoundaryCursor, Key: key})
		if err != nil {
			return err
		}
		if compared >= 0 {
			if err := i.collect(root.left, key, result); err != nil {
				return err
			}
		}
	}
	afterLeft, err := querywindow.AfterLeft(i.plan, key, root.bounds.Left)
	if err != nil {
		return err
	}
	if afterLeft {
		atOrBeforeRight, compareErr := querywindow.AtOrBeforeRight(i.plan, key, root.bounds.Right)
		if compareErr != nil {
			return compareErr
		}
		if atOrBeforeRight {
			*result = append(*result, root.id)
		}
		// Every interval in the right subtree begins at or after this one.
		// None can contain the point if this left boundary is already >= key.
		if err := i.collect(root.right, key, result); err != nil {
			return err
		}
	}
	return nil
}

func (i *windowIndex) insert(root, node *windowNode) (*windowNode, error) {
	if root == nil {
		return node, nil
	}
	compared, err := i.compareNode(node.bounds.Left, node.id, root.bounds.Left, root.id)
	if err != nil {
		return nil, err
	}
	copy := *root
	root = &copy
	if compared < 0 {
		root.left, err = i.insert(root.left, node)
		if err == nil && root.left.priority < root.priority {
			root, err = i.rotateRight(root)
		}
	} else {
		root.right, err = i.insert(root.right, node)
		if err == nil && root.right.priority < root.priority {
			root, err = i.rotateLeft(root)
		}
	}
	if err != nil {
		return nil, err
	}
	return i.recalculate(root)
}

func (i *windowIndex) remove(root *windowNode, left datastore.Boundary, id string) (*windowNode, error) {
	if root == nil {
		return nil, nil
	}
	compared, err := i.compareNode(left, id, root.bounds.Left, root.id)
	if err != nil {
		return nil, err
	}
	copy := *root
	root = &copy
	if compared < 0 {
		root.left, err = i.remove(root.left, left, id)
	} else if compared > 0 {
		root.right, err = i.remove(root.right, left, id)
	} else {
		return i.merge(root.left, root.right)
	}
	if err != nil {
		return nil, err
	}
	return i.recalculate(root)
}

func (i *windowIndex) merge(left, right *windowNode) (*windowNode, error) {
	if left == nil {
		return right, nil
	}
	if right == nil {
		return left, nil
	}
	if left.priority < right.priority {
		copy := *left
		left = &copy
		var err error
		left.right, err = i.merge(left.right, right)
		if err != nil {
			return nil, err
		}
		return i.recalculate(left)
	}
	copy := *right
	right = &copy
	var err error
	right.left, err = i.merge(left, right.left)
	if err != nil {
		return nil, err
	}
	return i.recalculate(right)
}

func (i *windowIndex) recalculate(node *windowNode) (*windowNode, error) {
	node.maxRight = node.bounds.Right
	for _, child := range []*windowNode{node.left, node.right} {
		if child == nil {
			continue
		}
		compared, err := querywindow.CompareBoundary(i.plan, node.maxRight, child.maxRight)
		if err != nil {
			return nil, err
		}
		if compared < 0 {
			node.maxRight = child.maxRight
		}
	}
	return node, nil
}

func (i *windowIndex) compareNode(left datastore.Boundary, leftID string, right datastore.Boundary, rightID string) (int, error) {
	compared, err := querywindow.CompareBoundary(i.plan, left, right)
	if err != nil || compared != 0 {
		return compared, err
	}
	if leftID < rightID {
		return -1, nil
	}
	if leftID > rightID {
		return 1, nil
	}
	return 0, nil
}

func (i *windowIndex) rotateRight(root *windowNode) (*windowNode, error) {
	left := root.left
	root.left = left.right
	var err error
	root, err = i.recalculate(root)
	if err != nil {
		return nil, err
	}
	left.right = root
	return i.recalculate(left)
}

func (i *windowIndex) rotateLeft(root *windowNode) (*windowNode, error) {
	right := root.right
	root.right = right.left
	var err error
	root, err = i.recalculate(root)
	if err != nil {
		return nil, err
	}
	right.left = root
	return i.recalculate(right)
}

func windowPriority(seed maphash.Seed, id string) uint64 {
	var hash maphash.Hash
	hash.SetSeed(seed)
	_, _ = hash.Write([]byte(id))
	return hash.Sum64()
}
