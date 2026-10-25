package projectionserver

import (
	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// outboundMessage is the closed set of messages produced by background work
// and consumed by one connection session. Concrete variants prevent payload,
// failure, and completion states from being combined accidentally.
type outboundMessage interface {
	isOutboundMessage()
}

type queryEventMessage struct {
	event    querysubscription.Event
	err      error
	messages []*reactivev1.TransactionMessage
}

func (queryEventMessage) isOutboundMessage() {}

type transactionMessage struct {
	transaction *transactionmessages.Transaction
}

func (transactionMessage) isOutboundMessage() {}

type deliveryPayload interface {
	outboundMessage
	isDeliveryPayload()
}

type subscriptionBatchMessage struct {
	externalID string
	generation string
	// Preserve the complete source context through filtering and output. The
	// row batch alone cannot identify the freshness of a window snapshot.
	delivery     querysubscription.DeliveryMetadata
	batch        querysubscription.Batch
	invalidation accesscontrol.Invalidation
	// Row image versions stay unchanged when only authorization changes.
	versions map[storage.EntityKey]querysubscription.RecordVersion
}

func (subscriptionBatchMessage) isOutboundMessage() {}
func (subscriptionBatchMessage) isDeliveryPayload() {}

type projectionBatchMessage struct {
	externalID   string
	generation   string
	batch        proj.ProjectionBatch
	invalidation accesscontrol.Invalidation
}

func (projectionBatchMessage) isOutboundMessage() {}
func (projectionBatchMessage) isDeliveryPayload() {}

type projectionClosedMessage struct {
	externalID string
	generation string
	cause      error
}

func (projectionClosedMessage) isOutboundMessage() {}

type windowFailedMessage struct {
	externalID string
	generation string
	requestID  string
	cause      error
}

func (windowFailedMessage) isOutboundMessage() {}

type completionMessage struct {
	run      func() error
	abandon  func()
	finished chan<- error
}

func (completionMessage) isOutboundMessage() {}
