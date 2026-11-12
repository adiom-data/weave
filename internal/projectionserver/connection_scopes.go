package projectionserver

import (
	"context"
	"errors"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type sharedSubscription struct {
	publication  publicationHold
	cancel       context.CancelFunc
	internalID   string
	queryID      string
	externalID   string
	subscription querysubscription.QuerySubscription
	visibility   *subscriptionVisibility
	sequence     uint64
}

type sharedProjection struct {
	policyOwnership projectionPolicyOwnership
	publication     publicationHold
	externalID      string
	generation      string
	projection      *proj.Projection
	cancel          context.CancelFunc
	visibility      *projectionVisibility
	selected        map[string]*fieldSelection
	sequence        uint64
}

func (p *sharedProjection) close() {
	p.publication.close()
	p.projection.Close()
	if p.cancel != nil {
		p.cancel()
	}
}

type pendingSharedSubscription struct {
	context    context.Context
	cancel     context.CancelFunc
	owns       func(string) bool
	projection *proj.Projection
	generation string
	admit      func() (proj.AdmissionWait, error)
	loaded     bool // The subscription call, not just its installer, has completed.
	retry      bool
	wait       <-chan struct{}
	policyOwns func(string) bool
	direct     *initialQueryImage
	requestID  string
}

func (session *connectionSession) closeDirect(state *sharedSubscription, cause error) error {
	state.publication.close()
	delete(session.subscriptions, state.externalID)
	_ = state.subscription.Close(context.WithoutCancel(session.ctx))
	if state.cancel != nil {
		state.cancel()
	}
	_ = session.prepared.Release(context.WithoutCancel(session.ctx), state.visibility.id, nil)
	if err := session.publisher.sendCleanup(state.externalID, &state.sequence); err != nil {
		return err
	}
	if !errors.Is(cause, querysubscription.ErrClosed) {
		state.sequence++
		if err := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "RESYNC_REQUIRED", Message: cause.Error()}}}); err != nil {
			return err
		}
	}
	return nil
}

func (session *connectionSession) closeProjection(state *sharedProjection, cause error) error {
	delete(session.projections, state.externalID)
	session.publisher.outputs.Drop(state.projection)
	state.close()
	_ = state.releaseVisibility(context.WithoutCancel(session.ctx), session.prepared)
	if err := session.publisher.sendCleanup(state.externalID, &state.sequence); err != nil {
		return err
	}
	if !errors.Is(cause, querysubscription.ErrClosed) {
		state.sequence++
		if err := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "RESYNC_REQUIRED", Message: cause.Error()}}}); err != nil {
			return err
		}
	}
	return nil
}
