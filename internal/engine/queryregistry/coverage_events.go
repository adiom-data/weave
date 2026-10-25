package queryregistry

import (
	"errors"
	"hash/maphash"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
)

// coverageEvents is an incremental counted interval union. A window (L,R]
// contributes +1 immediately after L and -1 immediately after R. Prefix sums
// therefore answer point coverage, while bounded event walks identify only
// portions whose count changes to or from zero.
type coverageEvents struct {
	plan datastore.PreparedQuery
	root *coverageNode
	seed maphash.Seed
}

type coverageNode struct {
	boundary datastore.Boundary
	delta    int
	sum      int
	// minPrefix is the lowest relative running sum in this subtree's
	// in-order event sequence. It includes the empty prefix, so it is always
	// non-positive. Together with the coverage count on entry, it proves
	// whether coverage ever drops to zero without walking interior events.
	minPrefix int
	priority  uint64
	left      *coverageNode
	right     *coverageNode
}

type coverageEvent struct {
	boundary datastore.Boundary
	delta    int
}

func newCoverageEvents(plan datastore.PreparedQuery) *coverageEvents {
	return &coverageEvents{plan: plan, seed: maphash.MakeSeed()}
}

func (c *coverageEvents) clone() *coverageEvents {
	return &coverageEvents{plan: c.plan, root: c.root, seed: c.seed}
}

func (c *coverageEvents) add(bounds datastore.Bounds) error {
	return c.change(bounds, 1)
}

func (c *coverageEvents) remove(bounds datastore.Bounds) error {
	return c.change(bounds, -1)
}

func (c *coverageEvents) change(bounds datastore.Bounds, amount int) error {
	compared, err := querywindow.CompareBoundary(c.plan, bounds.Left, bounds.Right)
	if err != nil {
		return err
	}
	if compared > 0 {
		return errors.New("left boundary follows right boundary")
	}
	if compared == 0 {
		return nil
	}
	root, err := c.addDelta(c.root, bounds.Left, amount)
	if err != nil {
		return err
	}
	root, err = c.addDelta(root, bounds.Right, -amount)
	if err != nil {
		return err
	}
	c.root = root
	return nil
}

func (c *coverageEvents) contains(key datastore.SortKey) (bool, error) {
	cursor := datastore.Boundary{Kind: datastore.BoundaryCursor, Key: key}
	count, err := c.prefix(cursor, false)
	return count > 0, err
}

func (c *coverageEvents) covered(bounds datastore.Bounds) (bool, error) {
	compared, err := querywindow.CompareBoundary(c.plan, bounds.Left, bounds.Right)
	if err != nil {
		return false, err
	}
	if compared > 0 {
		return false, errors.New("left boundary follows right boundary")
	}
	if compared == 0 {
		return true, nil
	}
	count, err := c.prefix(bounds.Left, true)
	if err != nil || count <= 0 {
		return false, err
	}
	middle, err := c.betweenAggregate(bounds.Left, bounds.Right)
	if err != nil {
		return false, err
	}
	return count+coverageMinPrefix(middle) > 0, nil
}

// edgeInterval returns the maximal covered interval touching START or END.
// Aggregate prefix data lets it find the opposite edge without walking
// unrelated disjoint intervals.
func (c *coverageEvents) edgeInterval(fromEnd bool) (datastore.Bounds, bool, error) {
	start := datastore.Boundary{Kind: datastore.BoundaryStart}
	end := datastore.Boundary{Kind: datastore.BoundaryEnd}
	if !fromEnd {
		count, err := c.prefix(start, true)
		if err != nil || count <= 0 {
			return datastore.Bounds{}, false, err
		}
		_, afterStart, err := c.split(c.root, start, true)
		if err != nil {
			return datastore.Bounds{}, false, err
		}
		right, found := firstCoverageZero(afterStart, &count)
		if !found {
			return datastore.Bounds{}, false, errors.New("coverage reference count is unbalanced")
		}
		return datastore.Bounds{Left: start, Right: right}, true, nil
	}
	beforeEnd, err := c.prefix(end, false)
	if err != nil || beforeEnd <= 0 {
		return datastore.Bounds{}, false, err
	}
	throughEnd, _, err := c.split(c.root, end, true)
	if err != nil {
		return datastore.Bounds{}, false, err
	}
	count, err := c.prefix(end, true)
	if err != nil {
		return datastore.Bounds{}, false, err
	}
	left, found := firstCoverageZeroReverse(throughEnd, &count)
	if !found {
		return datastore.Bounds{}, false, errors.New("coverage reference count is unbalanced")
	}
	return datastore.Bounds{Left: left, Right: end}, true, nil
}

// adjacentInterval returns the maximal covered interval immediately after
// boundary, or immediately before it when reverse is true.
func (c *coverageEvents) adjacentInterval(boundary datastore.Boundary, reverse bool) (datastore.Bounds, bool, error) {
	if !reverse {
		count, err := c.prefix(boundary, true)
		if err != nil || count <= 0 {
			return datastore.Bounds{}, false, err
		}
		_, after, err := c.split(c.root, boundary, true)
		if err != nil {
			return datastore.Bounds{}, false, err
		}
		right, found := firstCoverageZero(after, &count)
		if !found {
			return datastore.Bounds{}, false, errors.New("coverage reference count is unbalanced")
		}
		return datastore.Bounds{Left: boundary, Right: right}, true, nil
	}
	count, err := c.prefix(boundary, false)
	if err != nil || count <= 0 {
		return datastore.Bounds{}, false, err
	}
	before, _, err := c.split(c.root, boundary, false)
	if err != nil {
		return datastore.Bounds{}, false, err
	}
	left, found := firstCoverageZeroReverse(before, &count)
	if !found {
		return datastore.Bounds{}, false, errors.New("coverage reference count is unbalanced")
	}
	return datastore.Bounds{Left: left, Right: boundary}, true, nil
}

func firstCoverageZero(root *coverageNode, count *int) (datastore.Boundary, bool) {
	if root == nil {
		return datastore.Boundary{}, false
	}
	if *count+root.minPrefix > 0 {
		*count += root.sum
		return datastore.Boundary{}, false
	}
	if boundary, found := firstCoverageZero(root.left, count); found {
		return boundary, true
	}
	*count += root.delta
	if *count == 0 {
		return root.boundary, true
	}
	return firstCoverageZero(root.right, count)
}

func firstCoverageZeroReverse(root *coverageNode, count *int) (datastore.Boundary, bool) {
	if root == nil {
		return datastore.Boundary{}, false
	}
	// Reversing and negating this subtree has minimum relative prefix
	// -(sum-minPrefix), the negative of its greatest suffix sum.
	if *count-(root.sum-root.minPrefix) > 0 {
		*count -= root.sum
		return datastore.Boundary{}, false
	}
	if boundary, found := firstCoverageZeroReverse(root.right, count); found {
		return boundary, true
	}
	*count -= root.delta
	if *count == 0 {
		return root.boundary, true
	}
	return firstCoverageZeroReverse(root.left, count)
}

// regionsWithCount returns pieces of bounds whose current coverage count is
// exactly target. Boundaries follow (left,right] semantics.
func (c *coverageEvents) regionsWithCount(bounds datastore.Bounds, target int) ([]datastore.Bounds, error) {
	compared, err := querywindow.CompareBoundary(c.plan, bounds.Left, bounds.Right)
	if err != nil || compared == 0 {
		return nil, err
	}
	if compared > 0 {
		return nil, errors.New("left boundary follows right boundary")
	}
	count, err := c.prefix(bounds.Left, true)
	if err != nil {
		return nil, err
	}
	middle, err := c.betweenAggregate(bounds.Left, bounds.Right)
	if err != nil {
		return nil, err
	}
	state := coverageRegionScan{count: count, target: target, cursor: bounds.Left}
	c.scanRegions(middle, &state)
	if state.count == target {
		state.result = append(state.result, datastore.Bounds{Left: state.cursor, Right: bounds.Right})
	}
	return state.result, nil
}

type coverageRegionScan struct {
	count  int
	target int
	cursor datastore.Boundary
	result []datastore.Bounds
}

// scanRegions emits only portions whose entry coverage equals target. When
// coverage enters a subtree above target and its cached minimum proves it
// never falls back to target, the whole subtree can be skipped in O(1).
// Window-removal callers use target=1 inside the removed window, where
// coverage cannot fall below one before the removal is applied.
func (c *coverageEvents) scanRegions(root *coverageNode, state *coverageRegionScan) {
	if root == nil {
		return
	}
	if state.count > state.target && state.count+root.minPrefix > state.target {
		state.count += root.sum
		return
	}
	c.scanRegions(root.left, state)
	if state.count == state.target {
		state.result = append(state.result, datastore.Bounds{Left: state.cursor, Right: root.boundary})
	}
	state.count += root.delta
	state.cursor = root.boundary
	c.scanRegions(root.right, state)
}

func (c *coverageEvents) intervals() ([]datastore.Bounds, error) {
	var events []coverageEvent
	c.walk(c.root, &events)
	count := 0
	var left datastore.Boundary
	var result []datastore.Bounds
	for _, event := range events {
		before := count
		count += event.delta
		if before == 0 && count > 0 {
			left = event.boundary
		}
		if before > 0 && count == 0 {
			result = append(result, datastore.Bounds{Left: left, Right: event.boundary})
		}
		if count < 0 {
			return nil, errors.New("coverage reference count is negative")
		}
	}
	if count != 0 {
		return nil, errors.New("coverage reference count is unbalanced")
	}
	return result, nil
}

func (c *coverageEvents) prefix(boundary datastore.Boundary, inclusive bool) (int, error) {
	result := 0
	root := c.root
	for root != nil {
		compared, err := querywindow.CompareBoundary(c.plan, root.boundary, boundary)
		if err != nil {
			return 0, err
		}
		if compared < 0 || inclusive && compared == 0 {
			result += coverageSum(root.left) + root.delta
			root = root.right
		} else {
			root = root.left
		}
	}
	return result, nil
}

// betweenAggregate returns a persistent subtree containing events strictly
// between left and right. Splitting copies only the two search paths, leaving
// c.root unchanged and allowing callers to use the subtree aggregates without
// visiting every interior boundary.
func (c *coverageEvents) betweenAggregate(left, right datastore.Boundary) (*coverageNode, error) {
	_, afterLeft, err := c.split(c.root, left, true)
	if err != nil {
		return nil, err
	}
	middle, _, err := c.split(afterLeft, right, false)
	return middle, err
}

// split partitions root into ordered trees. When includeEqualLeft is true,
// the left result contains keys <= boundary; otherwise it contains keys <
// boundary. The operation is persistent: only nodes on the split path are
// copied.
func (c *coverageEvents) split(root *coverageNode, boundary datastore.Boundary, includeEqualLeft bool) (*coverageNode, *coverageNode, error) {
	if root == nil {
		return nil, nil, nil
	}
	compared, err := querywindow.CompareBoundary(c.plan, root.boundary, boundary)
	if err != nil {
		return nil, nil, err
	}
	belongsLeft := compared < 0 || includeEqualLeft && compared == 0
	copy := *root
	if belongsLeft {
		var right *coverageNode
		copy.right, right, err = c.split(root.right, boundary, includeEqualLeft)
		if err != nil {
			return nil, nil, err
		}
		return recalculateCoverage(&copy), right, nil
	}
	var left *coverageNode
	left, copy.left, err = c.split(root.left, boundary, includeEqualLeft)
	if err != nil {
		return nil, nil, err
	}
	return left, recalculateCoverage(&copy), nil
}
func (c *coverageEvents) addDelta(root *coverageNode, boundary datastore.Boundary, delta int) (*coverageNode, error) {
	if root == nil {
		priority, err := c.priority(boundary)
		if err != nil {
			return nil, err
		}
		return recalculateCoverage(&coverageNode{boundary: boundary, delta: delta, priority: priority}), nil
	}
	compared, err := querywindow.CompareBoundary(c.plan, boundary, root.boundary)
	if err != nil {
		return nil, err
	}
	copy := *root
	root = &copy
	if compared == 0 {
		root.delta += delta
		if root.delta == 0 {
			return c.merge(root.left, root.right)
		}
		return recalculateCoverage(root), nil
	}
	if compared < 0 {
		root.left, err = c.addDelta(root.left, boundary, delta)
		if err == nil && root.left != nil && root.left.priority < root.priority {
			root = rotateCoverageRight(root)
		}
	} else {
		root.right, err = c.addDelta(root.right, boundary, delta)
		if err == nil && root.right != nil && root.right.priority < root.priority {
			root = rotateCoverageLeft(root)
		}
	}
	if err != nil {
		return nil, err
	}
	return recalculateCoverage(root), nil
}

func (c *coverageEvents) merge(left, right *coverageNode) (*coverageNode, error) {
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
		left.right, err = c.merge(left.right, right)
		return recalculateCoverage(left), err
	}
	copy := *right
	right = &copy
	var err error
	right.left, err = c.merge(left, right.left)
	return recalculateCoverage(right), err
}

func (c *coverageEvents) walk(root *coverageNode, result *[]coverageEvent) {
	if root == nil {
		return
	}
	c.walk(root.left, result)
	*result = append(*result, coverageEvent{boundary: root.boundary, delta: root.delta})
	c.walk(root.right, result)
}

func (c *coverageEvents) priority(boundary datastore.Boundary) (uint64, error) {
	parts := [][]byte{{byte(boundary.Kind)}}
	if boundary.Kind == datastore.BoundaryCursor {
		encoded, err := c.plan.EncodeKey(boundary.Key)
		if err != nil {
			return 0, err
		}
		parts = append(parts, encoded)
	}
	return seededPriority(c.seed, parts...), nil
}

func coverageSum(root *coverageNode) int {
	if root == nil {
		return 0
	}
	return root.sum
}

func coverageMinPrefix(root *coverageNode) int {
	if root == nil {
		return 0
	}
	return root.minPrefix
}

func recalculateCoverage(root *coverageNode) *coverageNode {
	if root != nil {
		leftSum := coverageSum(root.left)
		afterNode := leftSum + root.delta
		root.sum = afterNode + coverageSum(root.right)
		root.minPrefix = min(0,
			coverageMinPrefix(root.left),
			afterNode,
			afterNode+coverageMinPrefix(root.right),
		)
	}
	return root
}

func rotateCoverageRight(root *coverageNode) *coverageNode {
	left := root.left
	root.left = left.right
	recalculateCoverage(root)
	left.right = root
	return recalculateCoverage(left)
}

func rotateCoverageLeft(root *coverageNode) *coverageNode {
	right := root.right
	root.right = right.left
	recalculateCoverage(root)
	right.left = root
	return recalculateCoverage(right)
}
