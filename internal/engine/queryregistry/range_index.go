package queryregistry

import (
	"sort"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

// rangeIndex is a dynamic point-stabbing index. It rebuilds its immutable
// centered tree geometrically, scans only the small pending tail, and compacts
// removal tombstones geometrically. Point lookups remain O(log n + matches)
// outside the bounded pending tail.
type rangeIndex struct {
	compare func(storage.Value, storage.Value) (int, bool)
	entries []rangeEntry
	built   int
	root    *rangeNode
	active  map[datastore.QueryFingerprint]bool
	counts  map[datastore.QueryFingerprint]int
	stale   int
}

type rangeEntry struct {
	fingerprint datastore.QueryFingerprint
	interval    routing.RoutingRange
}

type rangeNode struct {
	center  *storage.Value
	all     []rangeEntry
	byLower []rangeEntry
	byUpper []rangeEntry
	left    *rangeNode
	right   *rangeNode
}

func newRangeIndex(compare func(storage.Value, storage.Value) (int, bool)) *rangeIndex {
	return &rangeIndex{compare: compare, active: make(map[datastore.QueryFingerprint]bool), counts: make(map[datastore.QueryFingerprint]int)}
}

func (i *rangeIndex) add(entry rangeEntry) bool {
	if !validRange(entry.interval, i.compare) {
		return false
	}
	wasActive := i.active[entry.fingerprint]
	previousCount := i.counts[entry.fingerprint]
	previousStale := i.stale
	if !wasActive {
		// Reacquiring an identical fingerprint makes its tombstoned intervals
		// live again until the next compaction.
		i.stale -= previousCount
	}
	i.entries = append(i.entries, entry)
	i.active[entry.fingerprint] = true
	i.counts[entry.fingerprint] = previousCount + 1
	pending := len(i.entries) - i.built
	threshold := i.built
	if threshold < 32 {
		threshold = 32
	}
	if i.built == 0 || pending >= threshold {
		if !i.rebuild() {
			i.entries = i.entries[:len(i.entries)-1]
			i.stale = previousStale
			if previousCount == 0 {
				delete(i.counts, entry.fingerprint)
			} else {
				i.counts[entry.fingerprint] = previousCount
			}
			if !wasActive {
				delete(i.active, entry.fingerprint)
			}
			return false
		}
	}
	return true
}

func (i *rangeIndex) remove(fingerprint datastore.QueryFingerprint) {
	if !i.active[fingerprint] {
		return
	}
	delete(i.active, fingerprint)
	i.stale += i.counts[fingerprint]
	// Query churn leaves tombstones in the immutable tree. Compact
	// geometrically so removals are amortized instead of rebuilding the whole
	// index for every unsubscribe.
	if i.stale*2 >= len(i.entries) {
		_ = i.rebuild()
	}
}

func (i *rangeIndex) empty() bool { return len(i.active) == 0 }

func (i *rangeIndex) candidates(point storage.Value) (map[datastore.QueryFingerprint]struct{}, bool) {
	result := make(map[datastore.QueryFingerprint]struct{})
	if !queryRangeTree(i.root, point, i.compare, result) {
		return nil, false
	}
	for _, entry := range i.entries[i.built:] {
		if !i.active[entry.fingerprint] {
			continue
		}
		contains, ok := rangeContains(entry.interval, point, i.compare)
		if !ok {
			return nil, false
		}
		if contains {
			result[entry.fingerprint] = struct{}{}
		}
	}
	for fingerprint := range result {
		if !i.active[fingerprint] {
			delete(result, fingerprint)
		}
	}
	return result, true
}

func (i *rangeIndex) rebuild() bool {
	active := make([]rangeEntry, 0, len(i.entries))
	counts := make(map[datastore.QueryFingerprint]int, len(i.active))
	for _, entry := range i.entries {
		if i.active[entry.fingerprint] {
			active = append(active, entry)
			counts[entry.fingerprint]++
		}
	}
	root, ok := buildRangeTree(active, i.compare)
	if !ok {
		return false
	}
	i.entries = active
	i.root = root
	i.built = len(i.entries)
	i.counts = counts
	i.stale = 0
	return true
}

func validRange(interval routing.RoutingRange, compare func(storage.Value, storage.Value) (int, bool)) bool {
	if interval.Domain == "" {
		return false
	}
	if interval.Lower == nil || interval.Upper == nil {
		return true
	}
	compared, ok := compare(*interval.Lower, *interval.Upper)
	return ok && (compared < 0 || compared == 0 && interval.LowerInclusive && interval.UpperInclusive)
}

func buildRangeTree(entries []rangeEntry, compare func(storage.Value, storage.Value) (int, bool)) (*rangeNode, bool) {
	if len(entries) == 0 {
		return nil, true
	}
	var endpoints []storage.Value
	for _, entry := range entries {
		if entry.interval.Lower != nil {
			endpoints = append(endpoints, *entry.interval.Lower)
		}
		if entry.interval.Upper != nil {
			endpoints = append(endpoints, *entry.interval.Upper)
		}
	}
	if len(endpoints) == 0 {
		return &rangeNode{all: append([]rangeEntry(nil), entries...)}, true
	}
	ok := true
	sort.SliceStable(endpoints, func(a, b int) bool {
		compared, comparedOK := compare(endpoints[a], endpoints[b])
		if !comparedOK {
			ok = false
			return false
		}
		return compared < 0
	})
	if !ok {
		return nil, false
	}
	center := endpoints[len(endpoints)/2]
	var left, right, overlap []rangeEntry
	for _, entry := range entries {
		before, comparisonOK := rangeEndsBefore(entry.interval, center, compare)
		if !comparisonOK {
			return nil, false
		}
		if before {
			left = append(left, entry)
			continue
		}
		after, comparisonOK := rangeStartsAfter(entry.interval, center, compare)
		if !comparisonOK {
			return nil, false
		}
		if after {
			right = append(right, entry)
		} else {
			overlap = append(overlap, entry)
		}
	}
	node := &rangeNode{center: &center, byLower: append([]rangeEntry(nil), overlap...), byUpper: append([]rangeEntry(nil), overlap...)}
	if !sortLower(node.byLower, compare) || !sortUpperDescending(node.byUpper, compare) {
		return nil, false
	}
	var childOK bool
	if node.left, childOK = buildRangeTree(left, compare); !childOK {
		return nil, false
	}
	if node.right, childOK = buildRangeTree(right, compare); !childOK {
		return nil, false
	}
	return node, true
}

func queryRangeTree(node *rangeNode, point storage.Value, compare func(storage.Value, storage.Value) (int, bool), result map[datastore.QueryFingerprint]struct{}) bool {
	if node == nil {
		return true
	}
	if node.center == nil {
		for _, entry := range node.all {
			result[entry.fingerprint] = struct{}{}
		}
		return true
	}
	compared, ok := compare(point, *node.center)
	if !ok {
		return false
	}
	if compared == 0 {
		for _, entry := range node.byLower {
			contains, valid := rangeContains(entry.interval, point, compare)
			if !valid {
				return false
			}
			if contains {
				result[entry.fingerprint] = struct{}{}
			}
		}
		return true
	}
	if compared < 0 {
		for _, entry := range node.byLower {
			contains, keepScanning, valid := lowerAllows(entry.interval, point, compare)
			if !valid {
				return false
			}
			if !keepScanning {
				break
			}
			if contains {
				result[entry.fingerprint] = struct{}{}
			}
		}
		return queryRangeTree(node.left, point, compare, result)
	}
	for _, entry := range node.byUpper {
		contains, keepScanning, valid := upperAllows(entry.interval, point, compare)
		if !valid {
			return false
		}
		if !keepScanning {
			break
		}
		if contains {
			result[entry.fingerprint] = struct{}{}
		}
	}
	return queryRangeTree(node.right, point, compare, result)
}

func rangeEndsBefore(interval routing.RoutingRange, point storage.Value, compare func(storage.Value, storage.Value) (int, bool)) (bool, bool) {
	if interval.Upper == nil {
		return false, true
	}
	compared, ok := compare(*interval.Upper, point)
	return compared < 0, ok
}

func rangeStartsAfter(interval routing.RoutingRange, point storage.Value, compare func(storage.Value, storage.Value) (int, bool)) (bool, bool) {
	if interval.Lower == nil {
		return false, true
	}
	compared, ok := compare(*interval.Lower, point)
	return compared > 0, ok
}

func rangeContains(interval routing.RoutingRange, point storage.Value, compare func(storage.Value, storage.Value) (int, bool)) (bool, bool) {
	if interval.Lower != nil {
		compared, ok := compare(point, *interval.Lower)
		if !ok {
			return false, false
		}
		if compared < 0 || compared == 0 && !interval.LowerInclusive {
			return false, true
		}
	}
	if interval.Upper != nil {
		compared, ok := compare(point, *interval.Upper)
		if !ok {
			return false, false
		}
		if compared > 0 || compared == 0 && !interval.UpperInclusive {
			return false, true
		}
	}
	return true, true
}

func lowerAllows(interval routing.RoutingRange, point storage.Value, compare func(storage.Value, storage.Value) (int, bool)) (contains, keepScanning, valid bool) {
	if interval.Lower == nil {
		return true, true, true
	}
	compared, ok := compare(*interval.Lower, point)
	if !ok {
		return false, false, false
	}
	if compared < 0 || compared == 0 && interval.LowerInclusive {
		return true, true, true
	}
	return false, compared == 0, true
}

func upperAllows(interval routing.RoutingRange, point storage.Value, compare func(storage.Value, storage.Value) (int, bool)) (contains, keepScanning, valid bool) {
	if interval.Upper == nil {
		return true, true, true
	}
	compared, ok := compare(*interval.Upper, point)
	if !ok {
		return false, false, false
	}
	if compared > 0 || compared == 0 && interval.UpperInclusive {
		return true, true, true
	}
	return false, compared == 0, true
}

func sortLower(entries []rangeEntry, compare func(storage.Value, storage.Value) (int, bool)) bool {
	ok := true
	sort.SliceStable(entries, func(i, j int) bool {
		left, right := entries[i].interval.Lower, entries[j].interval.Lower
		if left == nil {
			return right != nil
		}
		if right == nil {
			return false
		}
		compared, comparedOK := compare(*left, *right)
		if !comparedOK {
			ok = false
			return false
		}
		if compared == 0 {
			return entries[i].interval.LowerInclusive && !entries[j].interval.LowerInclusive
		}
		return compared < 0
	})
	return ok
}

func sortUpperDescending(entries []rangeEntry, compare func(storage.Value, storage.Value) (int, bool)) bool {
	ok := true
	sort.SliceStable(entries, func(i, j int) bool {
		left, right := entries[i].interval.Upper, entries[j].interval.Upper
		if left == nil {
			return right != nil
		}
		if right == nil {
			return false
		}
		compared, comparedOK := compare(*left, *right)
		if !comparedOK {
			ok = false
			return false
		}
		if compared == 0 {
			return entries[i].interval.UpperInclusive && !entries[j].interval.UpperInclusive
		}
		return compared > 0
	})
	return ok
}
