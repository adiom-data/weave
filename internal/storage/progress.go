package storage

import "errors"

// SourcePosition is the common ordering contract of checkpoints and fences.
// Native resume-token interpretation remains the adapter's responsibility.
type SourcePosition interface {
	Source() SourceID
	Order() uint64
}

// ComparePositions compares positions only within the same nonempty source.
// Zero is a valid coordinate; nil or foreign positions are errors, not bounds.
func ComparePositions(left, right SourcePosition) (int, error) {
	if left == nil || right == nil {
		return 0, errors.New("positions are required")
	}
	if left.Source() == "" || left.Source() != right.Source() {
		return 0, errors.New("positions belong to different sources")
	}
	switch {
	case left.Order() < right.Order():
		return -1, nil
	case left.Order() > right.Order():
		return 1, nil
	default:
		return 0, nil
	}
}

// CheckpointCovers is a pure comparison of an already-applied checkpoint and
// a snapshot fence. It never consults a feed, a queue, or adapter receive state.
func CheckpointCovers(checkpoint Checkpoint, fence Fence) (bool, error) {
	compared, err := ComparePositions(checkpoint, fence)
	return err == nil && compared >= 0, err
}
