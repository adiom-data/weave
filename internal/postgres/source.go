package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/valueencoding"
)

// PostgresDataSource exposes PostgreSQL planning, snapshot, and change-feed
// behavior through the source-neutral engine contract.
type PostgresDataSource struct {
	db          *pgxpool.Pool
	feed        *feedSource
	adapter     *postgresTypeAdapter
	allow       map[string]struct{}
	publication string
	queryLimits QueryLimits
	metadata    metadataCache
}

type DataSourceOptions struct {
	AllowedTables []string
	// Publication enables strict validation that subscribed tables and every
	// table column participate in an unfiltered full-operation publication.
	Publication string
	QueryLimits QueryLimits
}

// QueryLimits bounds adapter planning work. They are configurable policy,
// unlike snapshot window size, which is owned by the engine issuing reads.
type QueryLimits struct {
	MaxOrderTerms          int
	MaxPredicateNodes      int
	MaxPredicateDepth      int
	MaxPredicateValueBytes int
}

func (limits QueryLimits) withDefaults() QueryLimits {
	d := DefaultQueryLimits()
	if limits.MaxOrderTerms == 0 {
		limits.MaxOrderTerms = d.MaxOrderTerms
	}
	if limits.MaxPredicateNodes == 0 {
		limits.MaxPredicateNodes = d.MaxPredicateNodes
	}
	if limits.MaxPredicateDepth == 0 {
		limits.MaxPredicateDepth = d.MaxPredicateDepth
	}
	if limits.MaxPredicateValueBytes == 0 {
		limits.MaxPredicateValueBytes = d.MaxPredicateValueBytes
	}
	return limits
}

// Validate checks configured values after applying the same defaults used by
// query preparation. Backend assembly can reject invalid limits before startup.
func (limits QueryLimits) validate() error {
	limits = limits.withDefaults()
	if limits.MaxOrderTerms < 1 || limits.MaxPredicateNodes < 1 || limits.MaxPredicateDepth < 1 || limits.MaxPredicateValueBytes < 1 {
		return errors.New("PostgreSQL query limits must be positive")
	}
	return nil
}

func NewPostgresDataSourceWithOptions(db *pgxpool.Pool, broker *cdc.Broker, options DataSourceOptions) *PostgresDataSource {
	options.QueryLimits = options.QueryLimits.withDefaults()
	adapter := newPostgresTypeAdapter()
	allow := make(map[string]struct{}, len(options.AllowedTables))
	for _, table := range options.AllowedTables {
		if table = strings.TrimSpace(table); table != "" {
			allow[table] = struct{}{}
		}
	}
	return &PostgresDataSource{
		db: db, feed: newFeedSource(broker, adapter), adapter: adapter, allow: allow,
		publication: strings.TrimSpace(options.Publication), queryLimits: options.QueryLimits,
	}
}

func (s *PostgresDataSource) ID() storage.SourceID { return SourceID }

func (s *PostgresDataSource) PrepareQuery(ctx context.Context, spec storage.QuerySpec) (datastore.PreparedQuery, error) {
	if err := s.queryLimits.validate(); err != nil {
		return nil, err
	}
	if err := validateRelationName(spec.Relation); err != nil {
		return nil, err
	}
	qualified := spec.Relation.Namespace + "." + spec.Relation.Name
	if _, ok := s.allow[qualified]; len(s.allow) > 0 && !ok {
		return nil, fmt.Errorf("table %s is not permitted", qualified)
	}
	metadata, err := s.metadata.get(ctx, spec.Relation, func(ctx context.Context) (*plan, error) {
		p, err := loadTablePlan(ctx, s.db, s.adapter, spec.Relation)
		if err != nil {
			return nil, err
		}
		if s.publication != "" {
			if err := validatePublication(ctx, s.db, s.publication, p); err != nil {
				return nil, err
			}
		}
		return p, nil
	})
	if err != nil {
		return nil, err
	}
	compiled, err := compileCompletePlan(metadata, spec, s.queryLimits)
	if err != nil {
		return nil, err
	}
	prepared := &postgresPrepared{plan: compiled}
	return prepared, nil
}

func validatePublication(ctx context.Context, db *pgxpool.Pool, publication string, p *plan) error {
	var insert, update, deleteRows, truncateRows, viaRoot bool
	err := db.QueryRow(ctx, `
		SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
		FROM pg_publication
		WHERE pubname = $1`, publication).Scan(&insert, &update, &deleteRows, &truncateRows, &viaRoot)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("publication %q does not exist", publication)
	}
	if err != nil {
		return fmt.Errorf("inspect publication %q: %w", publication, err)
	}
	if !insert || !update || !deleteRows || !truncateRows {
		return fmt.Errorf("publication %q must publish inserts, updates, deletes, and truncates", publication)
	}
	if viaRoot {
		return fmt.Errorf("publication %q uses publish_via_partition_root, which is not supported", publication)
	}
	var columns []string
	var rowFilter *string
	err = db.QueryRow(ctx, `
		SELECT attnames, rowfilter
		FROM pg_publication_tables
		WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
		publication, p.schema, p.table).Scan(&columns, &rowFilter)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("table %s.%s is not in publication %q", p.schema, p.table, publication)
	}
	if err != nil {
		return fmt.Errorf("inspect table %s.%s in publication %q: %w", p.schema, p.table, publication, err)
	}
	if rowFilter != nil {
		return fmt.Errorf("publication %q has a row filter for table %s.%s", publication, p.schema, p.table)
	}
	published := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		published[column] = struct{}{}
	}
	for column := range p.columns {
		if _, ok := published[column]; !ok {
			return fmt.Errorf("publication %q omits column %s.%s.%s", publication, p.schema, p.table, column)
		}
	}
	return nil
}

func (s *PostgresDataSource) BeginSnapshot(ctx context.Context, query datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	prepared, ok := nativePrepared(query)
	if !ok || prepared == nil || prepared.plan == nil {
		return nil, errors.New("PostgreSQL snapshot requires a PostgreSQL prepared query")
	}
	relation := prepared.Relation()
	if relation.Source != s.ID() {
		return nil, fmt.Errorf("relation belongs to source %q", relation.Source)
	}
	if _, ok := s.allow[relation.Namespace+"."+relation.Name]; len(s.allow) > 0 && !ok {
		return nil, fmt.Errorf("table %s.%s is not permitted", relation.Namespace, relation.Name)
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	return &postgresSnapshot{source: s, tx: tx, relation: relation, plan: prepared.plan}, nil
}

func (s *PostgresDataSource) OpenFeed(ctx context.Context) (datastore.ChangeFeed, error) {
	return s.feed.OpenFeed(ctx)
}

func (s *PostgresDataSource) OpenMessageFeed(ctx context.Context) (storage.MessageFeed, error) {
	return s.feed.OpenMessageFeed(ctx)
}

func (s *PostgresDataSource) RequestProgress(ctx context.Context, fence storage.Fence) error {
	if fence == nil || fence.Source() != SourceID {
		return errors.New("PostgreSQL progress received a foreign position")
	}
	position := pglogrepl.LSN(fence.Order())
	// A committed, synchronously flushed message works on every supported
	// PostgreSQL version (the nontransactional flush argument requires PG17).
	// The decoder treats this reserved prefix as progress, not application data.
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin progress barrier: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	// Wait for local WAL durability, not synchronous standby acknowledgments.
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = local"); err != nil {
		return fmt.Errorf("configure progress barrier flush: %w", err)
	}
	var raw string
	if err := tx.QueryRow(ctx,
		`SELECT pg_logical_emit_message(true, $1, '')::text`, cdc.BarrierPrefix,
	).Scan(&raw); err != nil {
		return fmt.Errorf("emit logical progress barrier: %w", err)
	}
	barrier, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		return fmt.Errorf("parse logical progress barrier: %w", err)
	}
	if barrier < position {
		return fmt.Errorf("logical progress barrier %s precedes requested fence %s", barrier, position)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("flush progress barrier: %w", err)
	}
	return nil
}

type postgresPrepared struct{ plan *plan }

type postgresSortKey struct{ values []*string }

func (postgresSortKey) StorageSortKey() {}

func (p *postgresPrepared) Fingerprint() datastore.QueryFingerprint {
	return datastore.QueryFingerprint(p.plan.fingerprint)
}

func (p *postgresPrepared) Relation() storage.RelationKey {
	return storage.RelationKey{
		Source: SourceID, Namespace: p.plan.schema, Name: p.plan.table,
		OpaqueID: strconv.FormatUint(uint64(p.plan.relationID), 10),
	}
}

func (p *postgresPrepared) Query() storage.QuerySpec { return cloneQuerySpec(p.plan.query) }

func (p *postgresPrepared) EntityKey(record storage.Record) (storage.EntityKey, error) {
	parts := make([]entitykey.Part, 0, len(p.plan.primary))
	for _, name := range p.plan.primary {
		value, ok := record[name]
		if !ok || value.State != storage.ValuePresent {
			return storage.EntityKey{}, fmt.Errorf("record lacks primary-key field %q", name)
		}
		encoded, err := valueencoding.ScalarBytes(value)
		if err != nil {
			return storage.EntityKey{}, fmt.Errorf("encode primary-key field %q: %w", name, err)
		}
		parts = append(parts, entitykey.Part{Name: name, TypeOID: p.plan.columns[name].typeOID, Value: encoded})
	}
	return storage.EntityKey{Relation: p.Relation(), Value: entitykey.CanonicalEncode(parts)}, nil
}

func (p *postgresPrepared) Matches(record storage.Record) (bool, error) {
	return storage.EvaluatePredicate(p.plan.query.Filter, record)
}

func (p *postgresPrepared) Key(record storage.Record) (datastore.SortKey, error) {
	values := make([]*string, 0, len(p.plan.order))
	for _, term := range p.plan.order {
		value, ok := storage.Lookup(record, term.Field)
		if !ok || value.State == storage.ValueMissing || value.State == storage.ValueUnavailable {
			return nil, fmt.Errorf("%w: record lacks ordering field %s", storage.ErrIncompleteRecord, term.Field.Display())
		}
		if value.State == storage.ValueNull {
			values = append(values, nil)
			continue
		}
		native, err := p.plan.adapter.encodeScalar(p.plan.columns[term.Field.Root()].typeOID, value)
		if err != nil {
			return nil, fmt.Errorf("encode ordering field %s: %w", term.Field.Display(), err)
		}
		text, ok := native.(string)
		if raw, binary := native.([]byte); binary {
			text, ok = `\x`+fmt.Sprintf("%x", raw), true
		}
		if !ok {
			return nil, fmt.Errorf("ordering field %s has an unsupported PostgreSQL key", term.Field.Display())
		}
		values = append(values, &text)
	}
	return postgresSortKey{values: values}, nil
}

func (p *postgresPrepared) Compare(a, b datastore.SortKey) (int, error) {
	left, leftOK := a.(postgresSortKey)
	right, rightOK := b.(postgresSortKey)
	if !leftOK || !rightOK {
		return 0, errors.New("sort key belongs to another source or query")
	}
	return p.plan.compareKeys(left.values, right.values)
}

func (p *postgresPrepared) EncodeKey(key datastore.SortKey) ([]byte, error) {
	value, ok := key.(postgresSortKey)
	if !ok {
		return nil, errors.New("sort key belongs to another source")
	}
	return p.plan.encodeCursor(value.values)
}

func (p *postgresPrepared) DecodeKey(cursor []byte) (datastore.SortKey, error) {
	values, err := p.plan.decodeCursor(cursor)
	if err != nil {
		return nil, err
	}
	return postgresSortKey{values: values}, nil
}

type postgresSnapshot struct {
	source   *PostgresDataSource
	tx       pgx.Tx
	relation storage.RelationKey
	plan     *plan
	done     bool
}

func (s *postgresSnapshot) Relation() storage.RelationKey { return s.relation }

func (s *postgresSnapshot) FetchEntity(ctx context.Context, key storage.EntityKey) (datastore.AuthoritativeRecord, error) {
	if key.Relation != s.relation {
		return datastore.AuthoritativeRecord{}, errors.New("entity is outside this relation snapshot")
	}
	row, err := s.plan.refetch(ctx, s.tx, key.Value)
	if err != nil {
		return datastore.AuthoritativeRecord{}, err
	}
	if row == nil {
		return datastore.AuthoritativeRecord{}, datastore.ErrNotFound
	}
	record, err := postgresRecord(s.plan.adapter, row.values)
	if err != nil {
		return datastore.AuthoritativeRecord{}, err
	}
	return datastore.AuthoritativeRecord{Record: record}, nil
}

func (s *postgresSnapshot) LoadWindowKeys(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.SortKey, error) {
	prepared, ok := nativePrepared(query)
	if !ok || prepared.Relation() != s.relation {
		return nil, errors.New("query is outside this relation snapshot")
	}
	if request.Limit < 1 {
		return nil, errors.New("window limit must be positive")
	}
	left, err := postgresBoundary(request.Bounds.Left)
	if err != nil {
		return nil, err
	}
	right, err := postgresBoundary(request.Bounds.Right)
	if err != nil {
		return nil, err
	}
	keys, err := prepared.plan.loadWindowKeys(ctx, s.tx, left, right, request.Reverse, request.Limit)
	if err != nil {
		return nil, err
	}
	result := make([]datastore.SortKey, 0, len(keys))
	for _, key := range keys {
		result = append(result, *key)
	}
	return result, nil
}

func (s *postgresSnapshot) LoadWindow(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.AuthoritativeRecord, error) {
	prepared, ok := nativePrepared(query)
	if !ok || prepared.Relation() != s.relation {
		return nil, errors.New("query is outside this relation snapshot")
	}
	if request.Limit < 1 {
		return nil, errors.New("window limit must be positive")
	}
	left, err := postgresBoundary(request.Bounds.Left)
	if err != nil {
		return nil, err
	}
	right, err := postgresBoundary(request.Bounds.Right)
	if err != nil {
		return nil, err
	}
	rows, err := prepared.plan.loadWindow(ctx, s.tx, left, right, request.Reverse, request.Limit)
	if err != nil {
		return nil, err
	}
	result := make([]datastore.AuthoritativeRecord, 0, len(rows))
	for _, row := range rows {
		record, err := postgresRecord(s.plan.adapter, row.values)
		if err != nil {
			return nil, err
		}
		result = append(result, datastore.AuthoritativeRecord{Record: record})
	}
	return result, nil
}

func (s *postgresSnapshot) Finish(ctx context.Context) (storage.Fence, error) {
	if s.done {
		return nil, errors.New("snapshot is already closed")
	}
	if err := s.tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.done = true
	var raw string
	// This is the snapshot fence, not proof that logical decoding has reached
	// it. The engine first lets normal CDC cover it and requests an in-band
	// logical barrier only if the feed remains quiet.
	if err := s.source.db.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&raw); err != nil {
		return nil, err
	}
	lsn, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		return nil, err
	}
	return WALPosition{LSN: lsn}, nil
}

func (s *postgresSnapshot) Abort(ctx context.Context) error {
	if s.done {
		return nil
	}
	s.done = true
	err := s.tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}

func postgresBoundary(value datastore.Boundary) (boundary, error) {
	switch value.Kind {
	case datastore.BoundaryStart:
		return boundary{kind: boundaryStart}, nil
	case datastore.BoundaryEnd:
		return boundary{kind: boundaryEnd}, nil
	case datastore.BoundaryCursor:
		key, ok := value.Key.(postgresSortKey)
		if !ok {
			return boundary{}, errors.New("cursor belongs to another source")
		}
		return boundary{kind: boundaryCursor, values: key.values}, nil
	default:
		return boundary{}, errors.New("unknown boundary kind")
	}
}

func postgresRecord(adapter *postgresTypeAdapter, row cdc.Row) (storage.Record, error) {
	record := make(storage.Record, len(row))
	for name, cell := range row {
		value, supported, err := adapter.decodeCell(cell)
		if err != nil {
			return nil, fmt.Errorf("canonicalize field %q: %w", name, err)
		}
		if !supported {
			continue
		}
		record[name] = value
	}
	return record, nil
}

var _ datastore.DataSource = (*PostgresDataSource)(nil)
var _ datastore.ProgressRequester = (*PostgresDataSource)(nil)
var _ datastore.PreparedQuery = (*postgresPrepared)(nil)
var _ datastore.SnapshotSession = (*postgresSnapshot)(nil)
