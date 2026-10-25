package querygrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/querysubscription/referenceview"
	querysubscriptionv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

// Client implements querysubscription.Client over the internal gRPC service.
type client struct {
	remote  querysubscriptionv1.QuerySubscriptionsClient
	options TransportOptions
}

func NewClient(connection grpc.ClientConnInterface) querysubscription.Client {
	return NewClientWithOptions(connection, TransportOptions{})
}

func NewClientWithOptions(connection grpc.ClientConnInterface, options TransportOptions) querysubscription.Client {
	return &client{remote: querysubscriptionv1.NewQuerySubscriptionsClient(connection), options: options.withDefaults()}
}

type response struct {
	message        *querysubscriptionv1.ServerMessage
	subscriptionID string
	snapshot       *querysubscriptionv1.QuerySnapshot
	entitySnapshot *querysubscriptionv1.EntitySnapshot
	installation   bool
	err            error
}

func (c *client) OpenFeed(ctx context.Context, options querysubscription.FeedOptions) (querysubscription.Feed, error) {
	base, err := c.openFeed(ctx, options)
	if err != nil {
		return nil, err
	}
	return referenceview.WrapFeed(ctx, base, options), nil
}

func (c *client) openFeed(ctx context.Context, options querysubscription.FeedOptions) (querysubscription.Feed, error) {
	if options.BufferLimit <= 0 || uint64(options.BufferLimit) > math.MaxUint32 {
		return nil, errors.New("query feed buffer limit must be positive")
	}
	feedCtx, cancel := context.WithCancel(ctx)
	stream, err := c.remote.Open(feedCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	feed := &remoteFeed{
		ctx: feedCtx, cancel: cancel, stream: stream, bufferLimit: options.BufferLimit, options: c.options,
		pending: make(map[string]chan response), subscriptions: make(map[string]*remoteSubscription), used: make(map[string]struct{}), done: make(chan struct{}),
		controls: make(chan *querysubscriptionv1.ClientMessage, c.options.ControlQueueLimit),
		requests: make(chan outboundRequest),
		events:   feedbuffer.New(options.BufferLimit),
	}
	go feed.sendMessages()
	go feed.receive()
	result, err := feed.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_Initialize{Initialize: &querysubscriptionv1.Initialize{BufferLimit: uint32(options.BufferLimit), ProtocolVersion: ProtocolVersion}}})
	if err != nil {
		feed.fail(err)
		return nil, err
	}
	if result.message == nil || result.message.GetAcknowledged() == nil || result.message.GetAcknowledged().ProtocolVersion != ProtocolVersion {
		feed.fail(errors.New("query server returned an invalid open response"))
		return nil, errors.New("query server returned an invalid open response")
	}
	return feed, nil
}

type remoteFeed struct {
	ctx         context.Context
	cancel      context.CancelFunc
	stream      grpc.BidiStreamingClient[querysubscriptionv1.ClientMessage, querysubscriptionv1.ServerMessage]
	bufferLimit int
	options     TransportOptions
	sequence    atomic.Uint64
	controls    chan *querysubscriptionv1.ClientMessage
	requests    chan outboundRequest
	events      *feedbuffer.Buffer
	receiveMu   sync.Mutex

	closeMu       contextMutex
	mu            sync.Mutex
	closed        bool
	err           error
	pending       map[string]chan response
	subscriptions map[string]*remoteSubscription
	used          map[string]struct{}
	done          chan struct{}
	doneOnce      sync.Once
}

func (s *remoteFeed) SubscribeQuery(ctx context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	if err := querysubscription.ValidateQuerySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	if uint64(request.Window.Count) > math.MaxUint32 {
		return errors.New("query subscription initial window exceeds the transport limit")
	}
	query, err := queryToProto(request.Query)
	if err != nil {
		return err
	}
	reference, err := referenceToProto(request.Reference)
	if err != nil {
		return err
	}
	subscription := &remoteSubscription{id: request.ID, owner: s}
	s.mu.Lock()
	if s.closed {
		err := s.feedErrorLocked()
		s.mu.Unlock()
		return err
	}
	if _, exists := s.used[request.ID]; exists {
		s.mu.Unlock()
		return errors.New("subscription ID was already used in feed")
	}
	s.used[request.ID] = struct{}{}
	s.subscriptions[request.ID] = subscription
	s.mu.Unlock()
	message, err := s.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_SubscribeQuery{SubscribeQuery: &querysubscriptionv1.SubscribeQuery{
		SubscriptionId: request.ID, Query: query, Window: &querysubscriptionv1.InitialWindow{FromEnd: request.Window.FromEnd, Count: uint32(request.Window.Count)}, Reference: reference,
	}}})
	if err != nil {
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	installationSucceeded := false
	if message.installation {
		defer func() {
			if !installationSucceeded {
				s.discardStagedSubscription(request.ID)
			}
			s.finishInstallation()
		}()
	}
	snapshotMessage := message.snapshot
	if snapshotMessage == nil || message.subscriptionID != request.ID {
		err = errors.New("query server returned an invalid snapshot response")
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	snapshot, err := snapshotFromProto(snapshotMessage)
	if err != nil {
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	if installErr := install(subscription, snapshot); installErr != nil {
		closeErr := subscription.Close(context.WithoutCancel(ctx))
		// Even if remote cleanup fails, this feed must never expose effects for a
		// subscription whose snapshot was rejected by its consumer.
		s.removeSubscription(request.ID)
		return errors.Join(installErr, closeErr)
	}
	installationSucceeded = true
	return nil
}

func (s *remoteFeed) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	if err := querysubscription.ValidateEntitySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("entity snapshot installer is required")
	}
	query, err := queryToProto(request.Query)
	if err != nil {
		return err
	}
	subscription := &remoteSubscription{id: request.ID, owner: s}
	s.mu.Lock()
	if s.closed {
		err := s.feedErrorLocked()
		s.mu.Unlock()
		return err
	}
	if _, exists := s.used[request.ID]; exists {
		s.mu.Unlock()
		return errors.New("subscription ID was already used in feed")
	}
	s.used[request.ID] = struct{}{}
	s.subscriptions[request.ID] = subscription
	s.mu.Unlock()
	message, err := s.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_SubscribeEntity{SubscribeEntity: &querysubscriptionv1.SubscribeEntity{
		SubscriptionId: request.ID, Query: query, FetchFrom: referenceSourceToProto(request.FetchFrom),
	}}})
	if err != nil {
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	installationSucceeded := false
	if message.installation {
		defer func() {
			if !installationSucceeded {
				s.discardStagedSubscription(request.ID)
			}
			s.finishInstallation()
		}()
	}
	if message.entitySnapshot == nil || message.subscriptionID != request.ID {
		err = errors.New("query server returned an invalid entity snapshot response")
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	snapshot, err := entitySnapshotFromProto(message.entitySnapshot)
	if err != nil {
		s.removeSubscription(request.ID)
		s.unsubscribeWithoutWaiting(request.ID)
		return err
	}
	if installErr := install(&remoteEntitySubscription{subscription: subscription}, snapshot); installErr != nil {
		closeErr := subscription.Close(context.WithoutCancel(ctx))
		s.removeSubscription(request.ID)
		return errors.Join(installErr, closeErr)
	}
	installationSucceeded = true
	return nil
}

func (s *remoteFeed) Next(ctx context.Context) (querysubscription.Event, error) {
	s.receiveMu.Lock()
	defer s.receiveMu.Unlock()
	return s.events.Next(ctx)
}

func (s *remoteFeed) RequestProgress(ctx context.Context) error {
	result, err := s.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_RequestProgress{
		RequestProgress: &querysubscriptionv1.RequestProgress{},
	}})
	if err != nil {
		return err
	}
	if result.message == nil || result.message.GetAcknowledged() == nil {
		return errors.New("query server returned an invalid progress acknowledgment")
	}
	return nil
}

func (s *remoteFeed) Close(ctx context.Context) error {
	if err := s.closeMu.Lock(ctx); err != nil {
		return err
	}
	defer s.closeMu.Unlock()
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}
	result, err := s.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_CloseFeed{CloseFeed: &querysubscriptionv1.CloseFeed{}}})
	if err != nil {
		if errors.Is(err, querysubscription.ErrClosed) {
			s.fail(querysubscription.ErrClosed)
			return nil
		}
		return err
	}
	if result.message == nil || result.message.GetAcknowledged() == nil {
		return errors.New("query server returned an invalid close acknowledgement")
	}
	s.fail(querysubscription.ErrClosed)
	return nil
}

func (s *remoteFeed) request(ctx context.Context, message *querysubscriptionv1.ClientMessage) (response, error) {
	if err := ctx.Err(); err != nil {
		return response{}, err
	}
	requestID := "request-" + strconv.FormatUint(s.sequence.Add(1), 10)
	message.RequestId = requestID
	// Only subscribe cancellation retires membership. A canceled window or
	// progress request must leave its existing subscription intact.
	subscriptionID := message.GetSubscribeQuery().GetSubscriptionId()
	if subscriptionID == "" {
		subscriptionID = message.GetSubscribeEntity().GetSubscriptionId()
	}
	wait := make(chan response, 1)
	s.mu.Lock()
	if s.closed {
		err := s.feedErrorLocked()
		s.mu.Unlock()
		return response{}, err
	}
	s.pending[requestID] = wait
	s.mu.Unlock()
	// The single writer owns transport flow control. Request contexts bound
	// both waiting for that writer and waiting for a response; a blocked Send
	// must never trap the caller past its deadline.
	select {
	case s.requests <- outboundRequest{ctx: ctx, message: message}:
	case <-ctx.Done():
		s.abandonRequest(requestID, wait, subscriptionID)
		return response{}, ctx.Err()
	case <-s.done:
		s.abandonRequest(requestID, wait, subscriptionID)
		s.mu.Lock()
		err := s.feedErrorLocked()
		s.mu.Unlock()
		return response{}, err
	}
	select {
	case result := <-wait:
		return result, result.err
	case <-ctx.Done():
		s.abandonRequest(requestID, wait, subscriptionID)
		s.cancelRequestWithoutWaiting(requestID)
		return response{}, ctx.Err()
	case <-s.done:
		select {
		case result := <-wait:
			return result, result.err
		default:
		}
		s.mu.Lock()
		err := s.feedErrorLocked()
		s.mu.Unlock()
		return response{}, err
	}
}

func (s *remoteFeed) receive() {
	querySnapshotPayloads := make(map[string]*payloadAssembly)
	deliveryPayloads := make(map[string]*payloadAssembly)
	for {
		message, err := s.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) && s.ctx.Err() != nil {
				err = querysubscription.ErrClosed
			}
			s.fail(err)
			return
		}
		if chunk := message.GetPayloadChunk(); chunk != nil {
			complete, data, payloadErr := s.acceptPayload(message.RequestId, chunk, querySnapshotPayloads, deliveryPayloads)
			if payloadErr != nil {
				s.fail(payloadErr)
				return
			}
			if !complete {
				continue
			}
			switch chunk.Kind {
			case querysubscriptionv1.PayloadChunk_ENTITY_INSTALLED:
				var value querysubscriptionv1.EntityInstallation
				if err := proto.Unmarshal(data, &value); err != nil {
					s.fail(err)
					return
				}
				entity, err := entityInstallationFromProto(&value)
				if err != nil {
					s.fail(err)
					return
				}
				if message.RequestId != "" {
					if entity.Target.Reference != nil || entity.Target.SubscriptionID != chunk.SubscriptionId {
						s.fail(errors.New("entity subscription response has an inconsistent target"))
						return
					}
					s.routeEntitySnapshot(message.RequestId, chunk.SubscriptionId, value.Snapshot)
					continue
				}
				if err := s.stageOrOffer(querysubscription.Event{Entity: &entity}); err != nil {
					s.fail(err)
					return
				}
			case querysubscriptionv1.PayloadChunk_QUERY_SNAPSHOT:
				var snapshot querysubscriptionv1.QuerySnapshot
				if err := proto.Unmarshal(data, &snapshot); err != nil {
					s.fail(fmt.Errorf("decode snapshot payload: %w", err))
					return
				}
				s.routeSnapshot(message.RequestId, chunk.SubscriptionId, &snapshot)
			case querysubscriptionv1.PayloadChunk_DELIVERY:
				var envelope querysubscriptionv1.DeliveryEnvelope
				if err := proto.Unmarshal(data, &envelope); err != nil {
					s.fail(fmt.Errorf("decode delivery payload: %w", err))
					return
				}
				converted, err := envelopeFromProto(&envelope)
				if err != nil {
					s.fail(err)
					return
				}
				if err := s.offerDelivery(&converted); err != nil {
					s.fail(err)
					return
				}
			default:
				s.fail(errors.New("query server returned an unknown payload kind"))
				return
			}
			continue
		}
		if message.RequestId != "" {
			s.routeResponse(message)
			continue
		}
		if progress := message.GetProgress(); progress != nil {
			event := querysubscription.Event{Progress: &querysubscription.Progress{
				Position: positionFromProto(progress.Position), Revision: progress.Revision, PreviousRevision: progress.PreviousRevision,
			}}
			if err := s.stageOrOffer(event); err != nil {
				s.fail(err)
				return
			}
			continue
		}
		if wireErr := message.GetError(); wireErr != nil && wireErr.SubscriptionId != "" {
			cause := remoteError(wireErr)
			s.removeSubscription(wireErr.SubscriptionId)
			if err := s.stageOrOffer(querysubscription.Event{Failure: &querysubscription.SubscriptionFailure{SubscriptionID: wireErr.SubscriptionId, Cause: cause}}); err != nil {
				s.fail(err)
				return
			}
			continue
		}
		s.fail(errors.New("query server returned an unrecognized message"))
		return
	}
}

func (s *remoteFeed) stageOrOffer(event querysubscription.Event) error { return s.events.Offer(event) }
func (s *remoteFeed) beginInstallation()                               { s.events.BeginInstallation() }
func (s *remoteFeed) finishInstallation() {
	if err := s.events.FinishInstallation(); err != nil {
		s.fail(err)
	}
}
func (s *remoteFeed) discardStagedSubscription(id string) { s.events.DiscardSubscription(id) }

// Filtering and staging share the membership lock: retirement must not discard
// buffered effects only to have a previously filtered envelope append them again.
func (s *remoteFeed) offerDelivery(envelope *querysubscription.DeliveryEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	retained := envelope.Effects[:0]
	for _, effect := range envelope.Effects {
		if _, known := s.used[effect.SubscriptionID]; !known {
			return fmt.Errorf("query server delivered an unknown subscription %q", effect.SubscriptionID)
		}
		if s.subscriptions[effect.SubscriptionID] != nil {
			retained = append(retained, effect)
		}
	}
	envelope.Effects = retained
	entities := envelope.Entities[:0]
	for _, entity := range envelope.Entities {
		id := entity.Target.SubscriptionID
		if _, known := s.used[id]; !known {
			return fmt.Errorf("query server delivered an unknown entity owner %q", id)
		}
		if s.subscriptions[id] != nil {
			entities = append(entities, entity)
		}
	}
	envelope.Entities = entities
	return s.stageOrOffer(querysubscription.Event{Delivery: envelope})
}

func (s *remoteFeed) routeSnapshot(requestID, subscriptionID string, snapshot *querysubscriptionv1.QuerySnapshot) {
	s.completeRequest(requestID, response{subscriptionID: subscriptionID, snapshot: snapshot, installation: true})
}

func (s *remoteFeed) routeEntitySnapshot(requestID, subscriptionID string, snapshot *querysubscriptionv1.EntitySnapshot) {
	s.completeRequest(requestID, response{subscriptionID: subscriptionID, entitySnapshot: snapshot, installation: true})
}

func (s *remoteFeed) routeResponse(message *querysubscriptionv1.ServerMessage) {
	result := response{message: message}
	if wireErr := message.GetError(); wireErr != nil {
		result = response{err: remoteError(wireErr)}
	}
	s.completeRequest(message.RequestId, result)
}

// completeRequest transfers response ownership atomically with removing the
// pending entry. The one-slot mailbox is empty while its entry is pending, so
// publishing never blocks. Cancellation can either remove that entry first or
// reclaim the fully published response, never miss an in-flight installation.
func (s *remoteFeed) completeRequest(requestID string, result response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wait := s.pending[requestID]
	if wait == nil {
		return
	}
	if result.installation {
		s.beginInstallation()
	}
	wait <- result
	delete(s.pending, requestID)
}

func (s *remoteFeed) acceptPayload(requestID string, chunk *querysubscriptionv1.PayloadChunk, snapshots, batches map[string]*payloadAssembly) (bool, []byte, error) {
	var assemblies map[string]*payloadAssembly
	var key string
	switch chunk.Kind {
	case querysubscriptionv1.PayloadChunk_QUERY_SNAPSHOT:
		if requestID == "" || chunk.SubscriptionId == "" {
			return false, nil, errors.New("snapshot payload requires request and subscription IDs")
		}
		assemblies, key = snapshots, requestID
	case querysubscriptionv1.PayloadChunk_ENTITY_INSTALLED:
		if requestID != "" {
			if chunk.SubscriptionId == "" {
				return false, nil, errors.New("entity response requires a subscription ID")
			}
			assemblies, key = snapshots, requestID
		} else {
			if chunk.SubscriptionId != "" {
				return false, nil, errors.New("unsolicited entity payload has a subscription ID outside its target")
			}
			assemblies, key = batches, strconv.FormatUint(chunk.PayloadId, 10)
		}
	case querysubscriptionv1.PayloadChunk_DELIVERY:
		if requestID != "" || chunk.SubscriptionId != "" {
			return false, nil, errors.New("delivery payload has invalid request or subscription ID")
		}
		assemblies, key = batches, strconv.FormatUint(chunk.PayloadId, 10)
	default:
		return false, nil, errors.New("unknown payload kind")
	}
	assembly := assemblies[key]
	if assembly == nil {
		if len(snapshots)+len(batches) >= s.options.MaxInFlightPayloads {
			return false, nil, errors.New("too many in-flight query payloads")
		}
		var err error
		assembly, err = newPayloadAssembly(chunk, s.options.MaxPayloadBytes)
		if err != nil {
			return false, nil, err
		}
		assemblies[key] = assembly
	}
	complete, err := assembly.append(chunk)
	if err != nil {
		delete(assemblies, key)
		return false, nil, err
	}
	if !complete {
		return false, nil, nil
	}
	delete(assemblies, key)
	return true, assembly.data, nil
}

func (s *remoteFeed) unsubscribeWithoutWaiting(id string) {
	message := &querysubscriptionv1.ClientMessage{RequestId: "request-" + strconv.FormatUint(s.sequence.Add(1), 10), Operation: &querysubscriptionv1.ClientMessage_Unsubscribe{Unsubscribe: &querysubscriptionv1.Unsubscribe{SubscriptionId: id}}}
	s.enqueueControl(message)
}

func (s *remoteFeed) cancelRequestWithoutWaiting(target string) {
	message := &querysubscriptionv1.ClientMessage{RequestId: "request-" + strconv.FormatUint(s.sequence.Add(1), 10), Operation: &querysubscriptionv1.ClientMessage_CancelRequest{CancelRequest: &querysubscriptionv1.CancelRequest{TargetRequestId: target}}}
	s.enqueueControl(message)
}

func (s *remoteFeed) enqueueControl(message *querysubscriptionv1.ClientMessage) {
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case s.controls <- message:
	case <-s.done:
	default:
		s.fail(querysubscription.ErrBackpressure)
	}
}

type outboundRequest struct {
	ctx     context.Context
	message *querysubscriptionv1.ClientMessage
}

func (s *remoteFeed) sendMessages() {
	for {
		var message *querysubscriptionv1.ClientMessage
		select {
		case request := <-s.requests:
			if request.ctx.Err() != nil {
				continue
			}
			message = request.message
		case message = <-s.controls:
		case <-s.done:
			return
		}
		if s.ctx.Err() != nil {
			return
		}
		if err := s.stream.Send(message); err != nil {
			s.fail(err)
			return
		}
	}
}

// abandonRequest owns cleanup if cancellation wins the caller's select, even
// when response routing already published a snapshot to its mailbox.
func (s *remoteFeed) abandonRequest(id string, wait chan response, subscriptionID string) {
	s.mu.Lock()
	delete(s.pending, id)
	subscription := s.subscriptions[subscriptionID]
	if subscriptionID != "" {
		delete(s.subscriptions, subscriptionID)
	}
	var abandoned response
	select {
	case abandoned = <-wait:
	default:
	}
	s.mu.Unlock()
	if subscription != nil {
		subscription.markClosed()
	}
	if subscriptionID != "" {
		// Remove rejected effects before releasing the installation pause.
		// Membership retires under the same lock as the pending response so
		// an ignored late snapshot cannot expose effects before its caller
		// returns through Subscribe's ordinary cleanup path.
		s.discardStagedSubscription(subscriptionID)
	}
	if abandoned.installation {
		s.finishInstallation()
	}
}

func (s *remoteFeed) removeSubscription(id string) {
	s.mu.Lock()
	subscription := s.subscriptions[id]
	delete(s.subscriptions, id)
	s.mu.Unlock()
	if subscription != nil {
		subscription.markClosed()
	}
}

func (s *remoteFeed) fail(reason error) {
	if reason == nil {
		reason = querysubscription.ErrClosed
	}
	if status.Code(reason) == codes.ResourceExhausted && !errors.Is(reason, querysubscription.ErrBackpressure) {
		reason = errors.Join(querysubscription.ErrBackpressure, reason)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = reason
	pending := s.pending
	s.pending = make(map[string]chan response)
	subscriptions := s.subscriptions
	s.subscriptions = make(map[string]*remoteSubscription)
	s.mu.Unlock()
	s.cancel()
	// Cancellation closes the whole stream. CloseSend is unnecessary and
	// would race the writer unless teardown waited for a blocked Send.
	for _, wait := range pending {
		wait <- response{err: reason}
	}
	for _, subscription := range subscriptions {
		subscription.markClosed()
	}
	s.events.Close(reason)
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *remoteFeed) feedErrorLocked() error {
	if s.err != nil {
		return s.err
	}
	return querysubscription.ErrClosed
}

type remoteSubscription struct {
	id    string
	owner *remoteFeed

	closeMu contextMutex
	mu      sync.Mutex
	closed  bool
}

type remoteEntitySubscription struct {
	subscription *remoteSubscription
}

func (s *remoteEntitySubscription) ID() string                      { return s.subscription.ID() }
func (s *remoteEntitySubscription) Close(ctx context.Context) error { return s.subscription.Close(ctx) }

func (s *remoteSubscription) ID() string { return s.id }

func (s *remoteSubscription) UpdateWindow(ctx context.Context, changes []querysubscription.BoundaryChange, token string) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return querysubscription.ErrClosed
	}
	converted := make([]*querysubscriptionv1.BoundaryChange, 0, len(changes))
	for _, change := range changes {
		wire, err := changeToProto(change)
		if err != nil {
			return err
		}
		converted = append(converted, wire)
	}
	result, err := s.owner.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_UpdateWindow{UpdateWindow: &querysubscriptionv1.UpdateWindow{SubscriptionId: s.id, Changes: converted, Token: token}}})
	if err != nil {
		return err
	}
	if result.message == nil || result.message.GetAcknowledged() == nil {
		return errors.New("query server returned an invalid window acknowledgement")
	}
	return nil
}

func (s *remoteSubscription) Close(ctx context.Context) error {
	if err := s.closeMu.Lock(ctx); err != nil {
		return err
	}
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	result, err := s.owner.request(ctx, &querysubscriptionv1.ClientMessage{Operation: &querysubscriptionv1.ClientMessage_Unsubscribe{Unsubscribe: &querysubscriptionv1.Unsubscribe{SubscriptionId: s.id}}})
	if err != nil {
		if errors.Is(err, querysubscription.ErrClosed) {
			s.owner.removeSubscription(s.id)
			return nil
		}
		return err
	}
	if result.message == nil || result.message.GetAcknowledged() == nil {
		return errors.New("query server returned an invalid unsubscribe acknowledgement")
	}
	s.owner.removeSubscription(s.id)
	return nil
}

func (s *remoteSubscription) markClosed() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func remoteError(wire *querysubscriptionv1.Error) error {
	if wire == nil {
		return errors.New("remote query error")
	}
	var sentinel error
	switch wire.Code {
	case querysubscriptionv1.Error_BACKPRESSURE:
		sentinel = querysubscription.ErrBackpressure
	case querysubscriptionv1.Error_CLOSED:
		sentinel = querysubscription.ErrClosed
	}
	if sentinel != nil {
		return fmt.Errorf("%w: %s", sentinel, wire.Message)
	}
	return errors.New(wire.Message)
}

var _ querysubscription.Client = (*client)(nil)
var _ querysubscription.Feed = (*remoteFeed)(nil)
var _ querysubscription.Subscription = (*remoteSubscription)(nil)
