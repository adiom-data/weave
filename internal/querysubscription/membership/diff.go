// Package membership contains producer-side list delta algorithms.
package membership

import (
	"sort"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// DiffMembership returns a stable remove/insert representation by retaining a
// longest ordered subsequence.
func Diff(before, after []storage.EntityKey) querysubscription.MembershipDelta {
	oldIndex := make(map[storage.EntityKey]int, len(before))
	for index, key := range before {
		oldIndex[key] = index
	}
	sequence := make([]int, 0, len(after))
	sequenceKeys := make([]storage.EntityKey, 0, len(after))
	for _, key := range after {
		if index, ok := oldIndex[key]; ok {
			sequence = append(sequence, index)
			sequenceKeys = append(sequenceKeys, key)
		}
	}
	keep := longestIncreasingKeys(sequence, sequenceKeys)
	result := querysubscription.MembershipDelta{}
	for _, key := range before {
		if _, ok := keep[key]; !ok {
			result.Removed = append(result.Removed, key)
		}
	}
	for index, key := range after {
		if _, ok := keep[key]; !ok {
			result.Insertions = append(result.Insertions, querysubscription.Insertion{Key: key, FinalIndex: index})
		}
	}
	return result
}

func longestIncreasingKeys(values []int, keys []storage.EntityKey) map[storage.EntityKey]struct{} {
	keep := make(map[storage.EntityKey]struct{})
	if len(values) == 0 {
		return keep
	}
	tails := make([]int, 0, len(values))
	positions := make([]int, 0, len(values))
	previous := make([]int, len(values))
	for index, value := range values {
		position := sort.SearchInts(tails, value)
		if position == len(tails) {
			tails = append(tails, value)
			positions = append(positions, index)
		} else {
			tails[position] = value
			positions[position] = index
		}
		previous[index] = -1
		if position > 0 {
			previous[index] = positions[position-1]
		}
	}
	for index := positions[len(positions)-1]; index >= 0; index = previous[index] {
		keep[keys[index]] = struct{}{}
	}
	return keep
}
