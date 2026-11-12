package projectionserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// connectionSession is the single-consumer owner of one stream's scopes,
// request history, and resource lifetime. Workers only post completions to
// outbound; they never mutate the scope maps. Fields are fixed after initialize,
// apart from state changed by the connection consumer itself.
type connectionSession struct {
	service  *server
	stream   Stream
	ctx      context.Context
	cancel   context.CancelFunc
	route    *transactionmessages.Connection
	prefix   string
	queries  querysubscription.Feed
	prepared accesscontrol.Prepared

	// Only the connection consumer mutates scopes or advances publication.
	subscriptions                map[string]*sharedSubscription
	projections                  map[string]*sharedProjection
	pending                      map[string]*pendingSharedSubscription
	requests                     *requestLedger
	admissions                   *policyAdmissions
	publisher                    *connectionPublisher
	factActivity, policyActivity <-chan struct{}

	// Producers retain immutable resource handles and enqueue completion work.
	loaders  sync.WaitGroup
	outbound chan outboundMessage
}

func (session *connectionSession) initialize() error {
	s := session.service
	if s.queries == nil {
		return errors.New("query subscription client is required")
	}
	stream := session.stream
	parent := session.ctx
	if parent == nil {
		parent = stream.Context()
	}
	ctx, cancel := context.WithCancel(parent)
	prefix := strconv.FormatUint(streamSequence.Add(1), 10) + ":"
	claims, err := s.resolveClaims(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("resolve access-control claims: %w", err)
	}
	preparedID := accesscontrol.PreparedID(prefix)
	queries, err := s.queries.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: s.options.SubscriptionQueueLimit})
	if err != nil {
		cancel()
		return fmt.Errorf("open query feed: %w", err)
	}
	s, prepared, err := s.prepareAccess(ctx, queries, preparedID, claims)
	if err != nil {
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return fmt.Errorf("prepare access control: %w", err)
	}
	if prepared == nil {
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return errors.New("prepare access control: controller returned nil state")
	}
	if prepared.ID() != preparedID {
		_ = prepared.Close()
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return errors.New("prepare access control: controller returned a different prepared ID")
	}
	if session.route != nil {
		if err := stream.Send(&reactivev1.ServerMessage{Event: &reactivev1.ServerMessage_ConnectionOpened{ConnectionOpened: &reactivev1.ConnectionOpened{ConnectionContext: session.route.Context}}}); err != nil {
			_ = queries.Close(context.WithoutCancel(ctx))
			_ = prepared.Close()
			cancel()
			return err
		}
	}

	session.service, session.ctx, session.cancel = s, ctx, cancel
	session.prefix, session.queries, session.prepared = prefix, queries, prepared
	session.subscriptions = make(map[string]*sharedSubscription)
	session.projections = make(map[string]*sharedProjection)
	session.pending = make(map[string]*pendingSharedSubscription)
	session.requests = newRequestLedger(s.options.MaxRequestsPerConnection)
	session.outbound = make(chan outboundMessage, s.options.OutboundQueueLimit)
	session.publisher = &connectionPublisher{connection: session, model: newConnectionModel(), outputs: &proj.OutputScheduler[*sharedProjection]{}}
	session.admissions = &policyAdmissions{ctx: ctx, service: s, stream: stream, queries: queries, prepared: prepared, subscriptions: session.subscriptions, projections: session.projections, pending: session.pending, loaders: &session.loaders, model: session.publisher.model, outbound: session.outbound, releaseProjection: session.publisher.releaseProjection, cleanupFailedScope: session.publisher.cleanupFailedScope}
	if s.relationAccess != nil {
		session.publisher.policy = &policyPublications{ctx: ctx, service: s, prepared: prepared, model: session.publisher.model, subscriptions: session.subscriptions, projections: session.projections, stream: stream, outbound: session.outbound, loaders: &session.loaders, closeDirect: session.closeDirect, closeProjection: session.closeProjection}
		session.factActivity = s.relationAccess.session.FactChanges()
		session.policyActivity = s.relationAccess.session.PolicyChanges()
	}
	return nil
}

func (session *connectionSession) run() error {
	if err := session.initialize(); err != nil {
		return err
	}
	defer session.close()
	input := make(chan connectionInput)
	go receiveConnectionRequests(session.ctx, session.stream, input)
	if session.service.relationAccess != nil {
		go forwardConnectionPolicyProgress(session.ctx, session.service.relationAccess, session.outbound)
	}
	if session.route != nil {
		go forwardJoinedFeed(session.ctx, session.queries, session.route, session.outbound, session.service.options.MessageProgressInterval)
	} else {
		go forwardQueryFeed(session.ctx, session.queries, session.outbound)
	}
	for {
		select {
		case <-session.ctx.Done():
			return session.ctx.Err()
		case <-session.policyActivity:
			if err := session.refreshAndRetryPolicy(); err != nil {
				return err
			}
		case <-session.factActivity:
			if err := session.refreshAndRetryPolicy(); err != nil {
				return err
			}
		case item := <-session.outbound:
			if err := session.handleOutbound(item); err != nil {
				return err
			}
			if err := session.retryPolicy(); err != nil {
				return err
			}
		case item := <-input:
			if item.err != nil {
				if errors.Is(item.err, io.EOF) {
					return nil
				}
				return item.err
			}
			if err := session.drain(); err != nil {
				return err
			}
			if err := session.handleRequest(item.message); err != nil {
				return err
			}
			if err := session.drain(); err != nil {
				return err
			}
		}
	}
}

// Requests and terminal receive errors share one ordered input. The channel is
// not closed: termination is explicit, or the connection context is canceled.
type connectionInput struct {
	message *reactivev1.ClientMessage
	err     error
}

func receiveConnectionRequests(ctx context.Context, stream Stream, input chan<- connectionInput) {
	for {
		message, err := stream.Recv()
		select {
		case input <- connectionInput{message: message, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func forwardConnectionPolicyProgress(ctx context.Context, access *relationAccess, outbound chan<- outboundMessage) {
	if err := access.runProgress(ctx); err != nil && ctx.Err() == nil {
		select {
		case outbound <- queryEventMessage{err: err}:
		case <-ctx.Done():
		}
	}
}

func (session *connectionSession) close() {
	session.cancel()
	session.loaders.Wait()
	for {
		select {
		case item := <-session.outbound:
			if completion, ok := item.(completionMessage); ok && completion.abandon != nil {
				completion.abandon()
			}
		default:
			goto drained
		}
	}
drained:
	for _, state := range session.subscriptions {
		_ = state.subscription.Close(context.WithoutCancel(session.ctx))
		if state.cancel != nil {
			state.cancel()
		}
	}
	for _, state := range session.projections {
		state.close()
	}
	_ = session.queries.Close(context.WithoutCancel(session.ctx))
	_ = session.prepared.Close()
}

func (session *connectionSession) refreshPolicy() error {
	session.service.relationAccess.refresh()
	for _, loading := range session.pending {
		loading.retry = true
	}
	if session.publisher.policy.position == nil {
		return nil
	}
	// Local evidence can change at an unchanged source position. Reevaluate
	// there without inventing a feed revision or requesting another report.
	return session.publisher.emitDeliveries([]feedDelivery{{metadata: querysubscription.DeliveryMetadata{
		Kind: querysubscription.DeliveryHydration, Checkpoint: session.publisher.policy.position, Revision: session.publisher.policy.revision,
	}}})
}

func (session *connectionSession) retryPolicy() error {
	if session.service.relationAccess == nil {
		return nil
	}
	if err := session.admissions.retry(); err != nil {
		return err
	}
	return session.publisher.policy.retry()
}

func (session *connectionSession) refreshAndRetryPolicy() error {
	if err := session.refreshPolicy(); err != nil {
		return err
	}
	return session.retryPolicy()
}

func (session *connectionSession) drain() error {
	for {
		select {
		case item := <-session.outbound:
			if err := session.handleOutbound(item); err != nil {
				return err
			}
			if err := session.retryPolicy(); err != nil {
				return err
			}
		case <-session.policyActivity:
			if err := session.refreshAndRetryPolicy(); err != nil {
				return err
			}
		case <-session.factActivity:
			if err := session.refreshAndRetryPolicy(); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}
