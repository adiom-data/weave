// Package postgres adapts PostgreSQL-native behavior to the storage contracts.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pglogrepl"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/valueencoding"
)

const SourceID storage.SourceID = "postgres"

type WALPosition struct {
	LSN pglogrepl.LSN
}

func (p WALPosition) String() string         { return p.LSN.String() }
func (p WALPosition) StableID() string       { return p.LSN.String() }
func (WALPosition) Source() storage.SourceID { return SourceID }
func (p WALPosition) Order() uint64          { return uint64(p.LSN) }

func (WALPosition) StorageFence()      {}
func (WALPosition) StorageCheckpoint() {}

type feedSource struct {
	broker  *cdc.Broker
	adapter *postgresTypeAdapter
}

func newFeedSource(broker *cdc.Broker, adapter *postgresTypeAdapter) *feedSource {
	return &feedSource{broker: broker, adapter: adapter}
}

func (s *feedSource) OpenFeed(ctx context.Context) (datastore.ChangeFeed, error) {
	feed, err := s.openReplicationFeed(ctx)
	if err != nil {
		return nil, err
	}
	return &changeFeed{replicationFeed: feed, adapter: s.adapter}, nil
}

func (s *feedSource) OpenMessageFeed(ctx context.Context) (storage.MessageFeed, error) {
	feed, err := s.openReplicationFeed(ctx)
	if err != nil {
		return nil, err
	}
	return &messageFeed{replicationFeed: feed}, nil
}

func (s *feedSource) openReplicationFeed(ctx context.Context) (*replicationFeed, error) {
	if err := s.broker.WaitReady(ctx); err != nil {
		return nil, fmt.Errorf("wait for PostgreSQL replication slot: %w", err)
	}
	subscription, err := s.broker.Subscribe()
	if err != nil {
		return nil, err
	}
	return &replicationFeed{broker: s.broker, subscription: subscription}, nil
}

type replicationFeed struct {
	broker       *cdc.Broker
	subscription cdc.Subscription
}

type changeFeed struct {
	*replicationFeed
	adapter *postgresTypeAdapter
	last    pglogrepl.LSN
}

func (f *replicationFeed) receive(ctx context.Context) (cdc.StreamEvent, error) {
	select {
	case event, ok := <-f.subscription.Events:
		if !ok {
			// Overflow closes Events after queueing its terminal error. Preserve
			// that cause instead of randomly returning EOF from the closed channel.
			select {
			case err := <-f.subscription.Err:
				return cdc.StreamEvent{}, feedError(err)
			default:
				return cdc.StreamEvent{}, io.EOF
			}
		}
		return event, nil
	case err, ok := <-f.subscription.Err:
		if !ok {
			return cdc.StreamEvent{}, io.EOF
		}
		return cdc.StreamEvent{}, feedError(err)
	case <-ctx.Done():
		return cdc.StreamEvent{}, ctx.Err()
	}
}

func (f *changeFeed) Receive(ctx context.Context) (datastore.ChangeEvent, error) {
	event, err := f.receive(ctx)
	if err != nil {
		return datastore.ChangeEvent{}, err
	}
	f.last = event.Position
	position := WALPosition{LSN: event.Position}
	if event.Transaction == nil {
		return datastore.ProgressEvent(position), nil
	}
	transaction := event.Transaction
	changes := make([]datastore.Change, 0, len(transaction.Changes))
	for _, change := range transaction.Changes {
		changes = append(changes, newPostgresChange(f.adapter, change))
	}
	return datastore.TransactionEvent(datastore.CommittedChanges{Changes: changes, Messages: postgresMessages(transaction.Messages), Checkpoint: position, Atomic: true}), nil
}

// messageFeed never visits the row-change slice. Row-only commits are just
// position-bearing units, independent of how many rows that transaction wrote.
type messageFeed struct{ *replicationFeed }

func (f *messageFeed) Receive(ctx context.Context) (storage.MessageEvent, error) {
	event, err := f.receive(ctx)
	if err != nil {
		return storage.MessageEvent{}, err
	}
	result := storage.MessageEvent{Checkpoint: WALPosition{LSN: event.Position}}
	if event.Transaction != nil {
		result.Messages = postgresMessages(event.Transaction.Messages)
	}
	return result, nil
}

func postgresMessages(messages []cdc.Message) []storage.TransactionMessage {
	if len(messages) == 0 {
		return nil
	}
	result := make([]storage.TransactionMessage, len(messages))
	for index, message := range messages {
		result[index] = storage.TransactionMessage{Kind: message.Prefix, Payload: message.Content}
	}
	return result
}

func feedError(err error) error {
	if err == nil {
		return io.EOF
	}
	if errors.Is(err, cdc.ErrSlowConsumer) {
		return fmt.Errorf("%w: %w", storage.ErrResumeRequired, err)
	}
	return err
}

func (f *changeFeed) Covers(fence storage.Fence) bool {
	position, ok := fence.(WALPosition)
	return ok && f.last >= position.LSN
}

// Ack advances PostgreSQL feedback only after the coordinator has applied and
// published the complete transaction. A process crash before this call causes
// the persistent slot to replay the transaction after restart.
func (f *changeFeed) Ack(_ context.Context, checkpoint storage.Checkpoint) error {
	position, ok := checkpoint.(WALPosition)
	if !ok {
		return errors.New("PostgreSQL feed received a foreign checkpoint")
	}
	f.broker.Acknowledge(position.LSN)
	return nil
}

func (f *replicationFeed) Close() error {
	f.subscription.Close()
	return nil
}

type postgresChange struct {
	adapter *postgresTypeAdapter
	change  cdc.Change
}

func newPostgresChange(adapter *postgresTypeAdapter, change cdc.Change) postgresChange {
	return postgresChange{adapter: adapter, change: change}
}

func (c postgresChange) canonicalIdentity(encoded, version string) (storage.EntityKey, error) {
	identity, err := canonicalPostgresIdentity(c.adapter, encoded)
	if err != nil {
		return storage.EntityKey{}, fmt.Errorf("canonicalize %s identity for %s.%s: %w", version, c.change.Schema, c.change.Table, err)
	}
	return storage.EntityKey{Relation: c.Relation(), Value: identity}, nil
}

func (c postgresChange) afterKey() (storage.EntityKey, error) {
	afterIdentity := c.change.NewIdentity
	if afterIdentity == "" {
		afterIdentity = c.change.Identity
	}
	return c.canonicalIdentity(afterIdentity, "new")
}

func canonicalPostgresIdentity(adapter *postgresTypeAdapter, encoded string) (string, error) {
	if encoded == "" {
		return "", errors.New("identity is missing")
	}
	parts, err := entitykey.Decode(encoded)
	if err != nil {
		return "", err
	}
	for index := range parts {
		value, supported, err := adapter.decodeCell(cdc.Cell{TypeOID: parts[index].TypeOID, Text: parts[index].Value})
		if err != nil {
			return "", fmt.Errorf("field %q: %w", parts[index].Name, err)
		}
		if !supported || value.State != storage.ValuePresent || value.Type == storage.TypeList || value.Type == storage.TypeObject {
			return "", fmt.Errorf("field %q uses an unsupported primary-key type", parts[index].Name)
		}
		encoded, err := valueencoding.ScalarBytes(value)
		if err != nil {
			return "", fmt.Errorf("field %q: %w", parts[index].Name, err)
		}
		parts[index].Value = encoded
	}
	return entitykey.CanonicalEncode(parts), nil
}

func (c postgresChange) Kind() datastore.ChangeKind {
	switch c.change.Operation {
	case "insert":
		return datastore.ChangeInsert
	case "update":
		return datastore.ChangeUpdate
	case "delete":
		return datastore.ChangeDelete
	case "truncate":
		return datastore.ChangeClearRelation
	default:
		return datastore.ChangeInvalidate
	}
}

func (c postgresChange) Relation() storage.RelationKey {
	return storage.RelationKey{Source: SourceID, Namespace: c.change.Schema, Name: c.change.Table, OpaqueID: fmt.Sprint(c.change.RelationID)}
}

func (c postgresChange) BeforeKey() (storage.EntityKey, error) {
	return c.canonicalIdentity(c.change.Identity, "old")
}

func (c postgresChange) AfterKey() (storage.EntityKey, error) {
	if c.Kind() == datastore.ChangeDelete {
		return c.BeforeKey()
	}
	return c.afterKey()
}

func (c postgresChange) Apply(previous storage.Record) (datastore.AppliedChange, error) {
	switch c.Kind() {
	case datastore.ChangeDelete:
		key, err := c.BeforeKey()
		if err != nil {
			return datastore.AppliedChange{}, err
		}
		return datastore.AppliedChange{Kind: datastore.AppliedDeleted, Key: key}, nil
	case datastore.ChangeClearRelation, datastore.ChangeInvalidate:
		return datastore.AppliedChange{}, errorsForRelationChange(c.Kind())
	}
	key, err := c.afterKey()
	if err != nil {
		return datastore.AppliedChange{}, err
	}
	result := datastore.AppliedChange{Kind: datastore.AppliedComplete, Key: key}
	// A PostgreSQL new tuple is authoritative about the relation's current
	// column set. Start fresh so a dropped/renamed field cannot survive merely
	// because it existed in the cached tuple. Only an explicit unchanged-TOAST
	// marker inherits the corresponding value from the previous record.
	record := make(storage.Record, len(c.change.New))
	for name, cell := range c.change.New {
		value, supported, err := c.adapter.decodeCell(cell)
		if err != nil {
			return datastore.AppliedChange{}, fmt.Errorf("canonicalize changed field %q: %w", name, err)
		}
		if !supported {
			continue
		}
		if value.State == storage.ValueUnavailable {
			if cached, ok := previous[name]; ok {
				record[name] = storage.CloneValue(cached)
				continue
			}
			result.Kind = datastore.AppliedPartial
		}
		record[name] = value
	}
	for _, value := range record {
		if value.State == storage.ValueUnavailable {
			result.Kind = datastore.AppliedPartial
			break
		}
	}
	result.Record = record
	return result, nil
}

func errorsForRelationChange(kind datastore.ChangeKind) error {
	if kind == datastore.ChangeInvalidate {
		return datastore.ErrInvalidated
	}
	return errors.New("relation-wide change must be applied by the engine")
}
