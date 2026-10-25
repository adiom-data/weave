package projectionserver

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/storage"
)

// projectionPolicyOwnership retains identity-only ownership for unpublished
// current-image attempts. A rejected attempt may have started lazy fact loads;
// those belong to this projection even before its visibility gate adopts them.
type projectionPolicyOwnership struct {
	attempt map[string]map[storage.EntityKey]struct{}
}

func (o *projectionPolicyOwnership) capture(ctx context.Context, prepared accesscontrol.Prepared, retained *projectionVisibility, events []proj.ProjectionEvent) error {
	previous := make(map[string]map[storage.EntityKey]struct{})
	for id, keys := range o.attempt {
		previous[id] = make(map[storage.EntityKey]struct{}, len(keys))
		for key := range keys {
			previous[id][key] = struct{}{}
		}
	}
	for id, instance := range retained.instances {
		if previous[id] == nil {
			previous[id] = make(map[storage.EntityKey]struct{})
		}
		for key := range instance.rows {
			previous[id][key] = struct{}{}
		}
	}
	current, err := retainInitialOccurrences(ctx, prepared, previous, events)
	if err == nil {
		o.attempt = current
	}
	return err
}

func (o *projectionPolicyOwnership) release(ctx context.Context, prepared accesscontrol.Prepared) error {
	var err error
	for id := range o.attempt {
		err = errors.Join(err, prepared.Release(ctx, id, nil))
	}
	o.attempt = nil
	return err
}

func (s *sharedProjection) releaseVisibility(ctx context.Context, prepared accesscontrol.Prepared) error {
	return errors.Join(s.visibility.release(ctx, prepared), s.policyOwnership.release(ctx, prepared))
}
