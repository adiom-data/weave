package cdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/messagebudget"
)

type Replicator struct {
	connectionConfig   *pgconn.Config
	publication        string
	slot               string
	broker             *Broker
	logger             *slog.Logger
	messageLimits      messagebudget.Limits
	messageLogInterval time.Duration
	lastMessageWarning time.Time
	replication        ReplicationOptions
	// relations retains the latest decoder metadata across replication retries.
	// A subsequent RelationMessage replaces it without invalidating cached rows.
	relations map[uint32]*pglogrepl.RelationMessage
}

type fatalReplicationError struct{ err error }

func (e fatalReplicationError) Error() string { return e.err.Error() }
func (e fatalReplicationError) Unwrap() error { return e.err }

func fatalReplication(err error) error {
	if err == nil {
		return nil
	}
	return fatalReplicationError{err: err}
}

// classifyStartupError separates configuration faults, which cannot be fixed
// by reconnecting with the same process configuration, from availability
// faults such as a refused connection or an active slot. Fatal errors reach
// Broker.WaitReady so the server fails startup instead of waiting forever.
func classifyStartupError(operation string, err error) error {
	if err == nil {
		return nil
	}
	wrapped := fmt.Errorf("%s: %w", operation, err)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return wrapped
	}
	switch postgresError.Code {
	case "0A000", // feature_not_supported
		"22023",          // invalid_parameter_value
		"28000", "28P01", // invalid authorization / password
		"3D000",          // invalid_catalog_name
		"42501",          // insufficient_privilege
		"42601", "42602", // syntax_error / invalid_name
		"42704": // undefined_object (including a missing publication)
		return fatalReplication(wrapped)
	default:
		return wrapped
	}
}

const BarrierPrefix = "weave_barrier"

// pendingTransaction belongs to one replication connection. Discarding it on
// reconnect (or replacing it at Begin) discards all uncommitted metadata too.
type pendingTransaction struct {
	id            uint32
	changes       []Change
	messages      []Message
	messageBudget messagebudget.Budget
}

func (t *pendingTransaction) begin(id uint32) {
	limits := t.messageBudget.Limits
	if limits == (messagebudget.Limits{}) {
		limits = messagebudget.Defaults()
	}
	*t = pendingTransaction{id: id, messageBudget: messagebudget.Budget{Limits: limits}}
}

func (t *pendingTransaction) message(m *pglogrepl.LogicalDecodingMessage) {
	if m.Prefix == BarrierPrefix {
		return // reserved progress-only metadata; the commit advances the feed
	}
	if m.Transactional {
		switch t.messageBudget.Add(len(m.Prefix), len(m.Content)) {
		case messagebudget.Keep:
			t.messages = append(t.messages, Message{Prefix: m.Prefix, Content: bytes.Clone(m.Content)})
		case messagebudget.Empty:
			t.messages = append(t.messages, Message{})
		}
	}
}

func (t *pendingTransaction) commit(ctx context.Context, broker *Broker, m *pglogrepl.CommitMessage) error {
	defer t.begin(0)
	if len(t.changes) > 0 || len(t.messages) > 0 {
		return broker.Publish(ctx, Transaction{ID: t.id, CommitLSN: m.CommitLSN, EndLSN: m.TransactionEndLSN, Changes: t.changes, Messages: t.messages})
	}
	position := m.TransactionEndLSN
	if position == 0 {
		position = m.CommitLSN
	}
	broker.Advance(position)
	return nil
}

type ReplicatorOptions struct {
	MessageLimits      messagebudget.Limits
	MessageLogInterval time.Duration
	Replication        ReplicationOptions
}

// NewReplicator borrows a parsed, pool-option-free SQL connection configuration
// and retains its own copy. Replication and slot inspection derive independent
// connections from that same base; no raw connection strings are reparsed.
func NewReplicator(config *pgconn.Config, publication, slot string, broker *Broker, logger *slog.Logger, options ReplicatorOptions) (*Replicator, error) {
	if config == nil {
		return nil, errors.New("parsed PostgreSQL connection configuration is required")
	}
	if options.MessageLimits == (messagebudget.Limits{}) {
		options.MessageLimits = messagebudget.Defaults()
	}
	if options.MessageLogInterval == 0 {
		options.MessageLogInterval = DefaultReplicatorOptions().MessageLogInterval
	}
	options.Replication = options.Replication.WithDefaults()
	if err := options.Replication.Validate(); err != nil {
		return nil, err
	}
	if err := options.MessageLimits.Validate(); err != nil {
		return nil, err
	}
	if options.MessageLogInterval <= 0 {
		return nil, errors.New("message log interval must be positive")
	}
	base := config.Copy()
	if base.RuntimeParams == nil {
		base.RuntimeParams = make(map[string]string)
	}
	// pgoutput's text output must be independent of role/session defaults.
	base.RuntimeParams["DateStyle"] = "ISO, YMD"
	base.RuntimeParams["TimeZone"] = "UTC"
	base.RuntimeParams["bytea_output"] = "hex"
	delete(base.RuntimeParams, "replication")
	return &Replicator{connectionConfig: base, publication: publication, slot: slot, broker: broker, logger: logger, messageLimits: options.MessageLimits, messageLogInterval: options.MessageLogInterval, replication: options.Replication, relations: make(map[uint32]*pglogrepl.RelationMessage)}, nil
}

func (r *Replicator) replicationConfig() *pgconn.Config {
	config := r.connectionConfig.Copy()
	config.RuntimeParams["replication"] = "database"
	return config
}

func (r *Replicator) Run(ctx context.Context) {
	backoff := r.replication.RetryMinDelay
	for ctx.Err() == nil {
		if err := r.consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			var fatal fatalReplicationError
			if errors.As(err, &fatal) {
				r.logger.Error("logical replication stopped permanently", "error", err)
				r.broker.Fail(err)
				return
			}
			r.logger.Error("logical replication stopped; retrying", "error", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = r.replication.nextRetryDelay(backoff)
	}
}

func (r *Replicator) consume(ctx context.Context) error {
	conn, err := pgconn.ConnectConfig(ctx, r.replicationConfig())
	if err != nil {
		return classifyStartupError("connect", err)
	}
	defer conn.Close(context.Background())
	startLSN, err := r.ensureSlot(ctx, conn)
	if err != nil {
		return classifyStartupError("prepare replication slot", err)
	}
	args := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", r.publication), "messages 'true'"}
	if err := pglogrepl.StartReplication(ctx, conn, r.slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: args}); err != nil {
		return classifyStartupError("start replication", err)
	}
	r.broker.MarkReady(startLSN)
	r.logger.Info("logical replication started", "publication", r.publication, "slot", r.slot)

	relations := r.relations
	transaction := pendingTransaction{messageBudget: messagebudget.Budget{Limits: r.messageLimits}}
	deadline := time.Now().Add(r.replication.StatusInterval)
	for {
		// pgconn can classify a canceled ReceiveMessage as a timeout. Checking
		// the parent before retrying prevents an endless timeout/retry spin on
		// shutdown, including when the replication stream is otherwise idle.
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			acknowledged := r.broker.Acknowledged()
			if acknowledged < startLSN {
				acknowledged = startLSN
			}
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: acknowledged}); err != nil {
				return err
			}
			deadline = time.Now().Add(r.replication.StatusInterval)
		}
		receiveCtx, cancel := context.WithDeadline(ctx, deadline)
		message, receiveErr := conn.ReceiveMessage(receiveCtx)
		cancel()
		if receiveErr != nil {
			if pgconn.Timeout(receiveErr) {
				continue
			}
			return receiveErr
		}
		copyData, ok := message.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return err
			}
			// ServerWALEnd is the end of physical WAL on the server, not proof
			// that pgoutput has delivered every logical message through that
			// position. Keepalives drive protocol replies only; ordered logical
			// barriers below are the source of progress-only checkpoints.
			if keepalive.ReplyRequested {
				deadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return err
			}
			logical, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fatalReplication(fmt.Errorf("parse logical WAL message: %w", err))
			}
			switch m := logical.(type) {
			case *pglogrepl.RelationMessage:
				// Relation metadata is decoder state, not a demand for a table-wide
				// resnapshot. Existing cached rows may retain an older shape until
				// touched; queries prepared against the new schema establish their
				// own fingerprint and authoritative coverage.
				relations[m.RelationID] = m
			case *pglogrepl.BeginMessage:
				transaction.begin(m.Xid)
			case *pglogrepl.InsertMessage:
				change, err := decodeInsert(relations[m.RelationID], m)
				if err != nil {
					return fatalReplication(fmt.Errorf("decode insert: %w", err))
				}
				transaction.changes = append(transaction.changes, change)
			case *pglogrepl.UpdateMessage:
				change, err := decodeUpdate(relations[m.RelationID], m)
				if err != nil {
					return fatalReplication(fmt.Errorf("decode update: %w", err))
				}
				transaction.changes = append(transaction.changes, change)
			case *pglogrepl.DeleteMessage:
				change, err := decodeDelete(relations[m.RelationID], m)
				if err != nil {
					return fatalReplication(fmt.Errorf("decode delete: %w", err))
				}
				transaction.changes = append(transaction.changes, change)
			case *pglogrepl.TruncateMessage:
				decoded, err := decodeTruncate(relations, m)
				if err != nil {
					return fatalReplication(fmt.Errorf("decode truncate: %w", err))
				}
				transaction.changes = append(transaction.changes, decoded...)
			case *pglogrepl.LogicalDecodingMessage:
				if !m.Transactional && m.Prefix == BarrierPrefix {
					r.broker.Advance(m.LSN)
				} else {
					transaction.message(m)
				}
			case *pglogrepl.CommitMessage:
				if transaction.messageBudget.Dropped > 0 && time.Since(r.lastMessageWarning) >= r.messageLogInterval {
					r.logger.Warn("transaction metadata replaced with empty placeholders", "omitted", transaction.messageBudget.Dropped, "transaction", transaction.id)
					r.lastMessageWarning = time.Now()
				}
				if err := transaction.commit(ctx, r.broker, m); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Replicator) ensureSlot(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.LSN, error) {
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err == nil {
		return pglogrepl.ParseLSN(result.ConsistentPoint)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" {
		queryConn, connectErr := pgconn.ConnectConfig(ctx, r.connectionConfig.Copy())
		if connectErr != nil {
			return 0, fmt.Errorf("connect to inspect replication slot: %w", connectErr)
		}
		defer queryConn.Close(context.Background())
		query := queryConn.ExecParams(ctx, `
			SELECT confirmed_flush_lsn::text
			FROM pg_replication_slots
			WHERE slot_name = $1 AND plugin = 'pgoutput' AND database = current_database()`,
			[][]byte{[]byte(r.slot)}, nil, nil, nil).Read()
		if query.Err != nil {
			return 0, fmt.Errorf("read replication slot position: %w", query.Err)
		}
		if len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] == nil {
			return 0, fatalReplication(fmt.Errorf("replication slot %q is not a pgoutput slot for this database", r.slot))
		}
		lsn, parseErr := pglogrepl.ParseLSN(string(query.Rows[0][0]))
		if parseErr != nil {
			return 0, fatalReplication(fmt.Errorf("parse replication slot position: %w", parseErr))
		}
		return lsn, nil
	}
	return 0, err
}

func decodeInsert(relation *pglogrepl.RelationMessage, message *pglogrepl.InsertMessage) (Change, error) {
	row, err := decodeTuple(relation, message.Tuple)
	if err != nil {
		return Change{}, err
	}
	rowIdentity := identity(relation, row)
	return Change{Operation: "insert", RelationID: relation.RelationID, Schema: relation.Namespace, Table: relation.RelationName, Identity: rowIdentity, NewIdentity: rowIdentity, New: row}, nil
}

func decodeUpdate(relation *pglogrepl.RelationMessage, message *pglogrepl.UpdateMessage) (Change, error) {
	newRow, err := decodeTuple(relation, message.NewTuple)
	if err != nil {
		return Change{}, err
	}
	oldRow, err := decodeTuple(relation, message.OldTuple)
	if err != nil {
		return Change{}, err
	}
	id := identity(relation, oldRow)
	if id == "" {
		id = identity(relation, newRow)
	}
	return Change{Operation: "update", RelationID: relation.RelationID, Schema: relation.Namespace, Table: relation.RelationName, Identity: id, NewIdentity: identity(relation, newRow), Old: oldRow, New: newRow}, nil
}

func decodeDelete(relation *pglogrepl.RelationMessage, message *pglogrepl.DeleteMessage) (Change, error) {
	oldRow, err := decodeTuple(relation, message.OldTuple)
	if err != nil {
		return Change{}, err
	}
	return Change{Operation: "delete", RelationID: relation.RelationID, Schema: relation.Namespace, Table: relation.RelationName, Identity: identity(relation, oldRow), Old: oldRow}, nil
}

func decodeTruncate(relations map[uint32]*pglogrepl.RelationMessage, message *pglogrepl.TruncateMessage) ([]Change, error) {
	changes := make([]Change, 0, len(message.RelationIDs))
	for _, relationID := range message.RelationIDs {
		relation := relations[relationID]
		if relation == nil {
			return nil, fmt.Errorf("relation metadata missing for relation %d", relationID)
		}
		changes = append(changes, Change{
			Operation:  "truncate",
			RelationID: relationID,
			Schema:     relation.Namespace,
			Table:      relation.RelationName,
		})
	}
	return changes, nil
}

func decodeTuple(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (Row, error) {
	if relation == nil {
		return nil, errors.New("relation metadata missing")
	}
	if tuple == nil {
		return nil, nil
	}
	if len(tuple.Columns) != len(relation.Columns) {
		return nil, errors.New("tuple column count mismatch")
	}
	row := make(Row, len(tuple.Columns))
	for i, value := range tuple.Columns {
		column := relation.Columns[i]
		cell := Cell{TypeOID: column.DataType}
		switch value.DataType {
		case pglogrepl.TupleDataTypeNull:
			cell.Null = true
		case pglogrepl.TupleDataTypeToast:
			cell.Missing = true
		case pglogrepl.TupleDataTypeText:
			cell.Text = append([]byte(nil), value.Data...)
		default:
			return nil, fmt.Errorf("unsupported binary value for %s", column.Name)
		}
		row[column.Name] = cell
	}
	return row, nil
}

func identity(relation *pglogrepl.RelationMessage, row Row) string {
	if row == nil {
		return ""
	}
	var parts []entitykey.Part
	for _, column := range relation.Columns {
		if column.Flags&1 == 0 {
			continue
		}
		cell, ok := row[column.Name]
		if !ok || cell.Missing || cell.Null {
			return ""
		}
		parts = append(parts, entitykey.Part{Name: column.Name, TypeOID: column.DataType, Value: cell.Text})
	}
	if len(parts) == 0 {
		return ""
	}
	return entitykey.CanonicalEncode(parts)
}
