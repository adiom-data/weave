package projectionserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

var errMissingVisibilityFence = errors.New("visibility evaluation requires a snapshot fence")
var errVisibilityResyncRequired = errors.New("access control requires subscription resynchronization")

// Direct query subscriptions have no partial-branch contract. Preserve their
// fail-fast behavior while projection publications can isolate Decision.Err.
func evaluateVisible(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	decisions, err := prepared.Evaluate(ctx, view, candidates)
	if err != nil {
		return nil, err
	}
	visible := make([]bool, len(decisions))
	for i, decision := range decisions {
		if decision.Err != nil {
			return nil, decision.Err
		}
		visible[i] = decision.Visible && !decision.Pending
	}
	return visible, nil
}

type visibilityFailure struct {
	externalID string
	projection bool
	cause      error
}

// filterVisibilityPart applies one subject-specific policy decision without
// mutating any other subscription. Evaluation and structural failures are
// scoped to that subscription; a failed projection must be closed because its
// connection-local visibility state may have partially incorporated the batch.
func filterVisibilityPart(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, raw deliveryPayload, subscriptions map[string]*sharedSubscription, projections map[string]*sharedProjection) (deliveryPayload, *visibilityFailure, error) {
	switch part := raw.(type) {
	case projectionBatchMessage:
		state := projections[part.externalID]
		if state == nil || (part.generation != "" && part.generation != state.generation) {
			return nil, nil, nil
		}
		filtered, err := state.visibility.filterTransaction(ctx, prepared, view, part.batch, part.invalidation)
		if err != nil {
			return nil, &visibilityFailure{externalID: part.externalID, projection: true, cause: fmt.Errorf("evaluate access control: %w", err)}, nil
		}
		part.batch = filtered
		return part, nil, nil
	case subscriptionBatchMessage:
		state := subscriptions[part.externalID]
		if state == nil || (part.generation != "" && part.generation != state.internalID) {
			return nil, nil, nil
		}
		filtered, err := state.visibility.filterTransaction(ctx, prepared, view, part)
		if err != nil {
			return nil, &visibilityFailure{externalID: part.externalID, cause: fmt.Errorf("evaluate access control: %w", err)}, nil
		}
		return filtered, nil, nil
	default:
		return nil, nil, errors.New("unknown assembled delivery payload")
	}
}

func synchronizeVisibility(ctx context.Context, controller accesscontrol.Controller, prepared accesscontrol.Prepared, position *querysubscription.Position) (accesscontrol.View, accesscontrol.Invalidation, error) {
	if position == nil {
		return nil, accesscontrol.Invalidation{}, errors.New("visibility evaluation requires a source checkpoint")
	}
	checkpoint, err := controller.Synchronize(ctx, prepared.ID(), position)
	if err != nil {
		return nil, accesscontrol.Invalidation{}, err
	}
	if checkpoint.View == nil {
		return nil, accesscontrol.Invalidation{}, errors.New("access controller returned a nil view")
	}
	if checkpoint.Position == nil || checkpoint.Position.Source() != position.Source() || checkpoint.Position.Order() != position.Order() {
		_ = checkpoint.View.Close()
		return nil, accesscontrol.Invalidation{}, errors.New("access controller returned a different checkpoint")
	}
	return checkpoint.View, checkpoint.Invalidation, nil
}

func acquireSnapshotVisibility(ctx context.Context, controller accesscontrol.Controller, fence *querysubscription.Position) (accesscontrol.View, error) {
	if fence == nil {
		return nil, errMissingVisibilityFence
	}
	checkpoint, err := controller.AcquireSnapshot(ctx, fence)
	if err != nil {
		return nil, err
	}
	if checkpoint.View == nil {
		return nil, errors.New("access controller returned a nil view")
	}
	if checkpoint.Fence == nil || checkpoint.Fence.Source() != fence.Source() || checkpoint.Fence.Order() != fence.Order() {
		_ = checkpoint.View.Close()
		return nil, errors.New("access controller returned a foreign snapshot fence")
	}
	return checkpoint.View, nil
}

// acquireVisibilityAt uses the checkpoint actually incorporated into a
// materialized result. The older snapshot fence is only a fallback for sources
// that can prove coverage without yielding a checkpoint to the coordinator.
func acquireVisibilityAt(ctx context.Context, controller accesscontrol.Controller, prepared accesscontrol.Prepared, checkpoint, fence *querysubscription.Position) (accesscontrol.View, error) {
	if checkpoint != nil {
		position, err := querysubscription.SnapshotPosition(checkpoint, fence)
		if err != nil {
			return nil, err
		}
		if querysubscription.SamePosition(position, checkpoint) {
			view, _, err := synchronizeVisibility(ctx, controller, prepared, checkpoint)
			return view, err
		}
	}
	return acquireSnapshotVisibility(ctx, controller, fence)
}

// Evidence can be reused within one publication attempt, but snapshot and
// committed views remain distinct even when their numeric positions coincide.
type visibilityPoint struct {
	source   storage.SourceID
	position uint64
	snapshot bool
}

func visibilityPointAt(checkpoint, fence *querysubscription.Position) (visibilityPoint, error) {
	if checkpoint == nil && fence == nil {
		return visibilityPoint{}, errMissingVisibilityFence
	}
	position, err := querysubscription.SnapshotPosition(checkpoint, fence)
	if err != nil {
		return visibilityPoint{}, err
	}
	return visibilityPoint{position.SourceID, position.Value, !querysubscription.SamePosition(position, checkpoint)}, nil
}
