package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/engine/windowloader"
)

func (c *Coordinator) QueueWindowUpdate(ctx context.Context, id string, changes []BoundaryChange, token string) (datastore.Bounds, error) {
	if token == "" {
		return datastore.Bounds{}, errors.New("window transition token is required")
	}
	return c.updateWindow(ctx, id, changes, token)
}

func (c *Coordinator) updateWindow(ctx context.Context, id string, changes []BoundaryChange, deliveryToken string) (datastore.Bounds, error) {
	if err := validateBoundaryChanges(changes, c.options.MaxWindowRows); err != nil {
		return datastore.Bounds{}, err
	}
	operation, err := c.beginWindowUpdate(ctx, id, changes)
	if err != nil {
		return datastore.Bounds{}, err
	}
	finished := false
	defer func() {
		if !finished {
			c.abortWindowUpdate(operation)
		}
	}()

	// Resolve every boundary change against the same pre-update window. The
	// request is atomic: changing one side must not alter the rows used to
	// resolve the other side, nor make the result depend on update ordering.
	active := operation.base
	original := active.Bounds
	bounds := original
	cached, err := c.planCachedBoundaries(ctx, operation, changes)
	if err != nil {
		return datastore.Bounds{}, err
	}
	type boundaryPlan struct {
		change         BoundaryChange
		cached         cachedBoundaryResolution
		remaining      int
		discoveryIndex int
	}
	plans := make([]boundaryPlan, len(changes))
	var requests []datastore.WindowRequest
	countChanges := 0
	needsDiscovery := false
	for index, change := range changes {
		switch change.Operation {
		case BoundaryContract:
			countChanges++
			needsDiscovery = needsDiscovery || cached[index].authoritative
		case BoundaryExtend:
			countChanges++
			atLimit := change.Side == BoundaryLeft && original.Left.Kind == datastore.BoundaryStart || change.Side == BoundaryRight && original.Right.Kind == datastore.BoundaryEnd
			needsDiscovery = needsDiscovery || !atLimit && (cached[index].authoritative || !cached[index].extension.Resolved)
		}
	}
	// If either side needs authoritative count resolution, resolve every count
	// in this atomic request from the same datastore snapshot. Mixing one cached
	// side with one discovered side would not describe any single membership
	// state. A one-sided request can still use partial cache coverage.
	batchAuthoritative := countChanges > 1 && needsDiscovery
	for index, change := range changes {
		plans[index] = boundaryPlan{change: change, cached: cached[index], discoveryIndex: -1}
		switch change.Operation {
		case BoundaryContract:
			if batchAuthoritative || cached[index].authoritative {
				plans[index].discoveryIndex = len(requests)
				requests = append(requests, datastore.WindowRequest{Bounds: original, Limit: change.Count + 1, Reverse: change.Side == BoundaryRight})
			}
		case BoundaryExtend:
			extension := cached[index].extension
			if !batchAuthoritative && !cached[index].authoritative && extension.Resolved {
				continue
			}
			remaining := change.Count
			if !batchAuthoritative && !cached[index].authoritative {
				remaining -= extension.CachedRows
			}
			plans[index].remaining = remaining
			reverse := change.Side == BoundaryLeft
			var request datastore.WindowRequest
			if reverse {
				if original.Left.Kind == datastore.BoundaryStart {
					continue
				}
				right := original.Left
				if !batchAuthoritative && !cached[index].authoritative && extension.Available {
					right = extension.Edge
				}
				request = datastore.WindowRequest{Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: right}, Limit: remaining + 1, Reverse: true}
			} else {
				if original.Right.Kind == datastore.BoundaryEnd {
					continue
				}
				left := original.Right
				if !batchAuthoritative && !cached[index].authoritative && extension.Available {
					left = extension.Edge
				}
				request = datastore.WindowRequest{Bounds: datastore.Bounds{Left: left, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Limit: remaining + 1}
			}
			plans[index].discoveryIndex = len(requests)
			requests = append(requests, request)
		}
	}
	var discoveries [][]windowloader.WindowDiscovery
	resolutionSource := windowResolvedFromCache
	if len(requests) > 0 {
		discoveries, err = c.loader.DiscoverWindows(ctx, active.Plan, requests)
		if err != nil {
			return datastore.Bounds{}, err
		}
		resolutionSource = windowResolvedFromDiscovery
		for _, discovered := range discoveries {
			if err := sortWindowDiscoveries(active.Plan, discovered); err != nil {
				return datastore.Bounds{}, err
			}
		}
	}
	for _, planned := range plans {
		change := planned.change
		switch change.Operation {
		case BoundaryMove:
			if change.Side == BoundaryLeft {
				bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: change.Key}
			} else {
				bounds.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: change.Key}
			}
		case BoundaryContract:
			keys := planned.cached.keys
			if planned.discoveryIndex >= 0 {
				discovered := discoveries[planned.discoveryIndex]
				keys = make([]datastore.SortKey, len(discovered))
				for index := range discovered {
					keys[index] = discovered[index].Key
				}
			}
			if change.Side == BoundaryLeft {
				if change.Count >= len(keys) {
					bounds.Left = original.Right
				} else {
					bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[change.Count-1]}
				}
			} else if change.Side == BoundaryRight {
				if change.Count >= len(keys) {
					bounds.Right = original.Left
				} else {
					index := change.Count
					if planned.discoveryIndex >= 0 {
						// Datastore discovery is normalized back into query order. Its
						// result contains only the greatest N+1 rows.
						index = len(keys) - change.Count - 1
					}
					bounds.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: keys[index]}
				}
			}
		case BoundaryExtend:
			reverse := change.Side == BoundaryLeft
			extension := planned.cached.extension
			if planned.discoveryIndex < 0 && !planned.cached.authoritative && extension.Resolved {
				if reverse {
					bounds.Left = extension.Boundary
				} else {
					bounds.Right = extension.Boundary
				}
				continue
			}
			if planned.discoveryIndex < 0 {
				continue
			}
			remaining := planned.remaining
			discovered := discoveries[planned.discoveryIndex]
			if reverse {
				if len(discovered) <= remaining {
					bounds.Left = datastore.Boundary{Kind: datastore.BoundaryStart}
				} else {
					bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[len(discovered)-remaining-1].Key}
				}
			} else if len(discovered) <= remaining {
				bounds.Right = datastore.Boundary{Kind: datastore.BoundaryEnd}
			} else {
				bounds.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[remaining-1].Key}
			}
		}
	}
	compared, err := querywindow.CompareBoundary(active.Plan, bounds.Left, bounds.Right)
	if err != nil {
		return datastore.Bounds{}, err
	}
	if compared > 0 {
		return datastore.Bounds{}, errors.New("left boundary follows right boundary")
	}
	resolved := resolvedWindow{bounds: bounds, source: resolutionSource}
	materialization, err := c.stageWindowUpdate(ctx, operation, resolved)
	if err != nil {
		return datastore.Bounds{}, err
	}
	if materialization != nil {
		materialization.reference = active.Reference
		materialized, readErr := c.readMaterialization(ctx, materialization)
		if readErr != nil {
			return datastore.Bounds{}, readErr
		}
		if _, activateErr := c.activateWindowRead(ctx, operation, materialized); activateErr != nil {
			return datastore.Bounds{}, activateErr
		}
	}
	if err := c.prefetchCurrentWindow(ctx, id, active.Reference); err != nil {
		return datastore.Bounds{}, err
	}
	installed, err := c.finishWindowUpdate(ctx, operation, deliveryToken)
	if err != nil {
		return datastore.Bounds{}, err
	}
	finished = true
	return installed, nil
}

func validateBoundaryChanges(changes []BoundaryChange, maxWindowRows int) error {
	if len(changes) == 0 {
		return errors.New("at least one boundary change is required")
	}
	seenSides := make(map[BoundarySide]struct{}, 2)
	for _, change := range changes {
		if change.Side != BoundaryLeft && change.Side != BoundaryRight {
			return errors.New("boundary side is required")
		}
		if _, duplicate := seenSides[change.Side]; duplicate {
			return errors.New("a window update may change each boundary at most once")
		}
		seenSides[change.Side] = struct{}{}
		switch change.Operation {
		case BoundaryMove:
			if change.Key == nil {
				return errors.New("MOVE requires a cursor")
			}
		case BoundaryExtend, BoundaryContract:
			if change.Count < 1 || change.Count > maxWindowRows {
				return fmt.Errorf("window update count must be between 1 and %d", maxWindowRows)
			}
		default:
			return errors.New("boundary operation is required")
		}
	}
	return nil
}
