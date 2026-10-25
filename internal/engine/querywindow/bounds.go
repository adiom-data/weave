// Package querywindow implements the datastore-neutral open-left,
// closed-right boundary rules shared by query caching, evaluation, delivery,
// and coordinator window management. Native key comparison remains delegated
// to datastore.PreparedQuery.
package querywindow

import (
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func CompareBoundary(plan datastore.PreparedQuery, left, right datastore.Boundary) (int, error) {
	if left.Kind != datastore.BoundaryCursor || right.Kind != datastore.BoundaryCursor {
		if left.Kind == right.Kind {
			return 0, nil
		}
		if left.Kind == datastore.BoundaryStart || right.Kind == datastore.BoundaryEnd {
			return -1, nil
		}
		return 1, nil
	}
	return plan.Compare(left.Key, right.Key)
}

func AfterLeft(plan datastore.PreparedQuery, key datastore.SortKey, left datastore.Boundary) (bool, error) {
	compared, err := CompareBoundary(plan, left, datastore.Boundary{Kind: datastore.BoundaryCursor, Key: key})
	return compared < 0, err
}

func AtOrBeforeRight(plan datastore.PreparedQuery, key datastore.SortKey, right datastore.Boundary) (bool, error) {
	compared, err := CompareBoundary(plan, datastore.Boundary{Kind: datastore.BoundaryCursor, Key: key}, right)
	return compared <= 0, err
}

func Contains(plan datastore.PreparedQuery, key datastore.SortKey, coverage []datastore.Bounds) (bool, error) {
	for _, bounds := range coverage {
		if bounds.Left.Kind == datastore.BoundaryEnd || bounds.Right.Kind == datastore.BoundaryStart {
			continue
		}
		afterLeft, err := AfterLeft(plan, key, bounds.Left)
		if err != nil {
			return false, err
		}
		atOrBeforeRight, err := AtOrBeforeRight(plan, key, bounds.Right)
		if err != nil {
			return false, err
		}
		if !afterLeft || !atOrBeforeRight {
			continue
		}
		return true, nil
	}
	return false, nil
}

func ContainsRecord(plan datastore.PreparedQuery, record storage.Record, bounds datastore.Bounds) (bool, error) {
	key, err := plan.Key(record)
	if err != nil {
		return false, err
	}
	return Contains(plan, key, []datastore.Bounds{bounds})
}

// Difference returns the at most two disjoint pieces of minuend not covered by
// subtrahend. Boundaries retain the service's (left, right] semantics.
func Difference(plan datastore.PreparedQuery, minuend, subtrahend datastore.Bounds) ([]datastore.Bounds, error) {
	valid, err := CompareBoundary(plan, minuend.Left, minuend.Right)
	if err != nil || valid == 0 {
		return nil, err
	}
	if valid > 0 {
		return nil, errors.New("left boundary follows right boundary")
	}
	valid, err = CompareBoundary(plan, subtrahend.Left, subtrahend.Right)
	if err != nil {
		return nil, err
	}
	if valid > 0 {
		return nil, errors.New("left boundary follows right boundary")
	}
	if valid == 0 {
		return []datastore.Bounds{minuend}, nil
	}
	var result []datastore.Bounds
	leftEnd := subtrahend.Left
	if compared, compareErr := CompareBoundary(plan, leftEnd, minuend.Right); compareErr != nil {
		return nil, compareErr
	} else if compared > 0 {
		leftEnd = minuend.Right
	}
	if compared, compareErr := CompareBoundary(plan, minuend.Left, leftEnd); compareErr != nil {
		return nil, compareErr
	} else if compared < 0 {
		result = append(result, datastore.Bounds{Left: minuend.Left, Right: leftEnd})
	}
	rightStart := subtrahend.Right
	if compared, compareErr := CompareBoundary(plan, rightStart, minuend.Left); compareErr != nil {
		return nil, compareErr
	} else if compared < 0 {
		rightStart = minuend.Left
	}
	if compared, compareErr := CompareBoundary(plan, rightStart, minuend.Right); compareErr != nil {
		return nil, compareErr
	} else if compared < 0 {
		result = append(result, datastore.Bounds{Left: rightStart, Right: minuend.Right})
	}
	return result, nil
}
