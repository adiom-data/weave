package windowloader

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

// WindowLoader separates snapshot IO from publication. Callers must consume
// the source feed through PendingWindow.Fence before Activate; this ensures no
// snapshot row becomes visible as current until all intervening committed
// changes have had a chance to supersede it.
type WindowLoader struct {
	source   snapshotSource
	entities windowEntityState
	queries  windowQueryState
}

type snapshotSource interface {
	ID() storage.SourceID
	BeginSnapshot(context.Context, datastore.PreparedQuery) (datastore.SnapshotSession, error)
}

// These consumer-owned interfaces keep snapshot loading independent from the
// coordinator's broader cache and query-registry capabilities.
type windowEntityState interface {
	Get(storage.EntityKey) (entitycache.EntitySnapshot, bool)
	StageSnapshotBatch([]entitycache.SnapshotLoadRequest) []entitycache.Load
	Complete(entitycache.Load, storage.Record) error
	Fail(entitycache.Load, error) error
	LoadError(entitycache.Load) error
	MarkUnreferenced(storage.EntityKey)
}

type windowQueryState interface {
	CoversKey(datastore.QueryFingerprint, datastore.SortKey) (bool, error)
	AddMember(datastore.QueryFingerprint, storage.EntityKey) (entitycache.EntitySnapshot, bool, error)
}

type pendingEntity struct {
	load         entitycache.Load
	record       storage.Record
	forceRefresh bool
	// absent is a point-read absence baseline, not a complete row onto which
	// an otherwise unresolved partial insert can safely be merged.
	absent bool
}

type validatedWindowRecord struct {
	record storage.Record
	entity storage.EntityKey
	sort   datastore.SortKey
}

type loadIdentity struct {
	key        storage.EntityKey
	generation uint64
}

type PendingWindow struct {
	Fence storage.Fence
	// References are speculative joined images. Only the coordinator's
	// reference-read admission may use them; Activate handles base rows only.
	References []datastore.ReferenceWindowRow

	entities windowEntityState
	queries  windowQueryState
	query    datastore.PreparedQuery
	pending  []pendingEntity
	used     bool
}

// QueryEvaluationError identifies a failure in one prepared query's local
// matching or ordering behavior. Callers can isolate that query without
// treating the authoritative snapshot or entity cache as corrupt.
type QueryEvaluationError struct{ Err error }

func (e *QueryEvaluationError) Error() string { return e.Err.Error() }
func (e *QueryEvaluationError) Unwrap() error { return e.Err }

func (w *PendingWindow) Len() int                       { return len(w.pending) }
func (w *PendingWindow) Query() datastore.PreparedQuery { return w.query }

func (w *PendingWindow) LastKey() (datastore.SortKey, bool, error) {
	if len(w.pending) == 0 {
		return nil, false, nil
	}
	key, err := w.query.Key(w.pending[len(w.pending)-1].record)
	return key, true, err
}

// WindowDiscovery is the key-only result of a first-pass window scan.
type WindowDiscovery struct {
	Key datastore.SortKey
}

// WaitForExternalLoads waits for coalesced loads owned by another snapshot.
// activationOrder must be the same order in which the caller will activate the
// windows. Loads owned by this sequence are deliberately skipped because an
// earlier window in the sequence will complete them during activation.
func WaitForExternalLoads(ctx context.Context, activationOrder []*PendingWindow) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		owned := make(map[loadIdentity]int)
		for windowIndex, window := range activationOrder {
			for _, entity := range window.pending {
				if entity.load.Owned() {
					owned[loadIdentity{key: entity.load.Key(), generation: entity.load.Generation()}] = windowIndex
				}
			}
		}
		failed := make([][]int, len(activationOrder))
		hasFailures := false
		for windowIndex, window := range activationOrder {
			for index := range window.pending {
				entity := &window.pending[index]
				if entity.load.Owned() {
					continue
				}
				if ownerIndex, internal := owned[loadIdentity{key: entity.load.Key(), generation: entity.load.Generation()}]; internal {
					if ownerIndex > windowIndex {
						return errors.New("snapshot load owner follows its waiter in activation order")
					}
					continue
				}
				select {
				case <-entity.load.Done():
					if err := window.entities.LoadError(entity.load); err != nil {
						failed[windowIndex] = append(failed[windowIndex], index)
						hasFailures = true
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if !hasFailures {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Preserve window order so an internal dependency always points toward a
		// window that activation will reconcile first.
		for windowIndex, indexes := range failed {
			if len(indexes) == 0 {
				continue
			}
			if err := activationOrder[windowIndex].retryLoads(indexes); err != nil {
				return err
			}
		}
	}
}

func (w *PendingWindow) retryLoads(indexes []int) error {
	if w.used || len(indexes) == 0 {
		return errors.New("cannot retry an activated pending window")
	}
	requests := make([]entitycache.SnapshotLoadRequest, len(indexes))
	for offset, index := range indexes {
		if index < 0 || index >= len(w.pending) {
			return errors.New("pending load index is out of range")
		}
		pending := w.pending[index]
		requests[offset] = entitycache.SnapshotLoadRequest{Key: pending.load.Key(), Record: pending.record, ForceRefresh: pending.forceRefresh}
	}
	loads := w.entities.StageSnapshotBatch(requests)
	if len(loads) != len(indexes) {
		return errors.New("entity cache returned the wrong snapshot load count")
	}
	for offset, index := range indexes {
		w.pending[index].load = loads[offset]
	}
	return nil
}

func NewWindowLoader(source snapshotSource, entities windowEntityState, queries windowQueryState) *WindowLoader {
	return &WindowLoader{source: source, entities: entities, queries: queries}
}

// ReadRanges fully materializes disjoint bounds through one snapshot, paging
// each range independently. The returned fence applies to the combined result.
func (l *WindowLoader) ReadRanges(ctx context.Context, query datastore.PreparedQuery, bounds []datastore.Bounds, pageSize int) (*PendingWindow, error) {
	return l.readRanges(ctx, query, bounds, pageSize, nil)
}

func (l *WindowLoader) ReadRangesWithReference(ctx context.Context, query datastore.PreparedQuery, bounds []datastore.Bounds, pageSize int, reference datastore.ReferenceFetch) (*PendingWindow, error) {
	return l.readRanges(ctx, query, bounds, pageSize, &reference)
}

func (l *WindowLoader) readRanges(ctx context.Context, query datastore.PreparedQuery, bounds []datastore.Bounds, pageSize int, reference *datastore.ReferenceFetch) (*PendingWindow, error) {
	if pageSize < 1 {
		return nil, errors.New("snapshot page size must be positive")
	}
	requests := make([]datastore.WindowRequest, 0, len(bounds))
	for _, interval := range bounds {
		requests = append(requests, datastore.WindowRequest{Bounds: interval, Limit: pageSize})
	}
	return l.readWithReference(ctx, query, requests, false, true, reference)
}

func (l *WindowLoader) ReadRefresh(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) (*PendingWindow, error) {
	return l.read(ctx, query, []datastore.WindowRequest{request}, true, false)
}

// DiscoverWindows resolves several cursor requests through one consistent
// source snapshot. The result remains unfenced and may only be used to choose
// boundaries; callers must materialize through a fenced read before publishing.
func (l *WindowLoader) DiscoverWindows(ctx context.Context, query datastore.PreparedQuery, requests []datastore.WindowRequest) ([][]WindowDiscovery, error) {
	if query.Relation().Source != l.source.ID() {
		return nil, errors.New("query belongs to another source")
	}
	if len(requests) == 0 {
		return nil, errors.New("at least one discovery request is required")
	}
	for _, request := range requests {
		if request.Limit < 1 {
			return nil, errors.New("window limit must be positive")
		}
	}
	snapshot, err := l.source.BeginSnapshot(ctx, query)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("source returned a nil discovery snapshot")
	}
	if snapshot.Relation() != query.Relation() {
		_ = snapshot.Abort(context.WithoutCancel(ctx))
		return nil, errors.New("source returned a discovery snapshot for another relation")
	}
	defer snapshot.Abort(context.WithoutCancel(ctx))
	result := make([][]WindowDiscovery, 0, len(requests))
	for _, request := range requests {
		keys, err := snapshot.LoadWindowKeys(ctx, query, request)
		if err != nil {
			return nil, err
		}
		if len(keys) > request.Limit {
			return nil, errors.New("source returned more keys than the window limit")
		}
		if err := validateWindowKeys(query, request.Bounds, keys); err != nil {
			return nil, err
		}
		window := make([]WindowDiscovery, 0, len(keys))
		for _, key := range keys {
			window = append(window, WindowDiscovery{Key: key})
		}
		result = append(result, window)
	}
	return result, nil
}

func (l *WindowLoader) read(ctx context.Context, query datastore.PreparedQuery, requests []datastore.WindowRequest, refresh, exhaust bool) (*PendingWindow, error) {
	return l.readWithReference(ctx, query, requests, refresh, exhaust, nil)
}

func (l *WindowLoader) readWithReference(ctx context.Context, query datastore.PreparedQuery, requests []datastore.WindowRequest, refresh, exhaust bool, reference *datastore.ReferenceFetch) (*PendingWindow, error) {
	if query.Relation().Source != l.source.ID() {
		return nil, errors.New("query belongs to another source")
	}
	if len(requests) == 0 {
		return nil, errors.New("at least one window request is required")
	}
	for _, request := range requests {
		if request.Limit < 1 {
			return nil, errors.New("window limit must be positive")
		}
	}
	snapshot, err := l.source.BeginSnapshot(ctx, query)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, errors.New("source returned a nil snapshot")
	}
	if snapshot.Relation() != query.Relation() {
		_ = snapshot.Abort(context.WithoutCancel(ctx))
		return nil, errors.New("source returned a snapshot for another relation")
	}
	finished := false
	handedOff := false
	var pending []pendingEntity
	defer func() {
		if !finished {
			_ = snapshot.Abort(context.WithoutCancel(ctx))
		}
		if !handedOff {
			for _, entity := range pending {
				if entity.load.Owned() {
					_ = l.entities.Fail(entity.load, context.Canceled)
				}
			}
		}
	}()
	seen := make(map[storage.EntityKey]struct{})
	var rows []validatedWindowRecord
	var references []datastore.ReferenceWindowRow
	for _, request := range requests {
		if exhaust && request.Reverse {
			return nil, errors.New("range materialization requires forward reads")
		}
		for {
			var authoritative []datastore.AuthoritativeRecord
			var loadErr error
			if reader, ok := snapshot.(datastore.ReferenceWindowReader); reference != nil && ok {
				var joined []datastore.ReferenceWindowRow
				joined, loadErr = reader.LoadWindowWithReference(ctx, query, request, *reference)
				for _, row := range joined {
					authoritative = append(authoritative, row.Row)
				}
				references = append(references, joined...)
			} else {
				authoritative, loadErr = snapshot.LoadWindow(ctx, query, request)
			}
			if loadErr != nil {
				return nil, loadErr
			}
			if len(authoritative) > request.Limit {
				return nil, errors.New("source returned more rows than the window limit")
			}
			records, unwrapErr := unwrapAuthoritative(authoritative)
			if unwrapErr != nil {
				return nil, unwrapErr
			}
			validated, validateErr := validateWindowRecords(query, request.Bounds, records)
			if validateErr != nil {
				return nil, validateErr
			}
			for _, row := range validated {
				if _, duplicate := seen[row.entity]; duplicate {
					return nil, errors.New("source returned a duplicate entity while materializing ranges")
				}
				seen[row.entity] = struct{}{}
				rows = append(rows, row)
			}
			if !exhaust || len(records) < request.Limit {
				break
			}
			last := validated[len(validated)-1].sort
			if request.Bounds.Left.Kind == datastore.BoundaryCursor {
				advanced, compareErr := query.Compare(request.Bounds.Left.Key, last)
				if compareErr != nil {
					return nil, compareErr
				}
				if advanced >= 0 {
					return nil, errors.New("source window page did not advance")
				}
			}
			if request.Bounds.Right.Kind == datastore.BoundaryCursor {
				reached, compareErr := query.Compare(last, request.Bounds.Right.Key)
				if compareErr != nil {
					return nil, compareErr
				}
				if reached > 0 {
					return nil, errors.New("source returned a row beyond the window boundary")
				}
				if reached == 0 {
					break
				}
			}
			request.Bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: last}
		}
	}
	fence, err := snapshot.Finish(ctx)
	if err != nil {
		return nil, err
	}
	finished = true
	if fence == nil || fence.Source() != l.source.ID() {
		return nil, errors.New("snapshot returned a foreign or nil fence")
	}
	pending, err = l.stageRows(rows, refresh)
	if err != nil {
		return nil, err
	}
	handedOff = true
	return &PendingWindow{Fence: fence, References: references, entities: l.entities, queries: l.queries, query: query, pending: pending}, nil
}

func (l *WindowLoader) stageRows(rows []validatedWindowRecord, refresh bool) ([]pendingEntity, error) {
	pending := make([]pendingEntity, 0, len(rows))
	requests := make([]entitycache.SnapshotLoadRequest, len(rows))
	for index, row := range rows {
		requests[index] = entitycache.SnapshotLoadRequest{Key: row.entity, Record: row.record, ForceRefresh: refresh}
	}
	loads := l.entities.StageSnapshotBatch(requests)
	if len(loads) != len(rows) {
		return nil, errors.New("entity cache returned the wrong snapshot load count")
	}
	for index, row := range rows {
		staged := pendingEntity{load: loads[index], record: storage.CloneRecord(row.record), forceRefresh: refresh}
		pending = append(pending, staged)
	}
	return pending, nil
}

func validateWindowRecords(query datastore.PreparedQuery, bounds datastore.Bounds, records []storage.Record) ([]validatedWindowRecord, error) {
	result := make([]validatedWindowRecord, 0, len(records))
	keys := make([]datastore.SortKey, 0, len(records))
	for _, record := range records {
		matches, err := query.Matches(record)
		if err != nil {
			return nil, &QueryEvaluationError{Err: err}
		}
		if !matches {
			return nil, errors.New("source returned a record that does not match the prepared query")
		}
		sortKey, err := query.Key(record)
		if err != nil {
			return nil, &QueryEvaluationError{Err: err}
		}
		entity, err := query.EntityKey(record)
		if err != nil {
			return nil, err
		}
		if entity.Relation != query.Relation() {
			return nil, errors.New("query produced a foreign entity key")
		}
		result = append(result, validatedWindowRecord{record: record, entity: entity, sort: sortKey})
		keys = append(keys, sortKey)
	}
	if err := validateWindowKeys(query, bounds, keys); err != nil {
		return nil, err
	}
	return result, nil
}

func validateWindowKeys(query datastore.PreparedQuery, bounds datastore.Bounds, keys []datastore.SortKey) error {
	var previous datastore.SortKey
	for _, key := range keys {
		afterLeft, err := querywindow.AfterLeft(query, key, bounds.Left)
		if err != nil {
			return err
		}
		atOrBeforeRight, err := querywindow.AtOrBeforeRight(query, key, bounds.Right)
		if err != nil {
			return err
		}
		if !afterLeft || !atOrBeforeRight {
			return errors.New("source returned a key outside the requested window")
		}
		if previous != nil {
			compared, err := query.Compare(previous, key)
			if err != nil {
				return err
			}
			if compared > 0 {
				return errors.New("source returned keys outside query order")
			}
		}
		previous = key
	}
	return nil
}

func unwrapAuthoritative(records []datastore.AuthoritativeRecord) ([]storage.Record, error) {
	result := make([]storage.Record, len(records))
	for i, record := range records {
		if err := record.Validate(); err != nil {
			return nil, err
		}
		result[i] = record.Record
	}
	return result, nil
}

// Activate is idempotence-protected. It returns the current rows for the
// requested window after reconciliation, not necessarily the older rows read
// by the snapshot.
func (w *PendingWindow) Activate(ctx context.Context) ([]entitycache.EntitySnapshot, error) {
	if err := w.Reconcile(ctx); err != nil {
		return nil, err
	}
	result := make([]entitycache.EntitySnapshot, 0, len(w.pending))
	for _, pending := range w.pending {
		entity, ok := w.entities.Get(pending.load.Key())
		if !ok || (entity.State != entitycache.EntityCurrent && entity.State != entitycache.EntityUnreferenced) {
			continue
		}
		matches, err := w.query.Matches(entity.Record)
		if err != nil {
			return nil, &QueryEvaluationError{Err: err}
		}
		if !matches {
			continue
		}
		contained := true
		if _, identity := w.query.(datastore.IdentityQuery); !identity {
			key, err := w.query.Key(entity.Record)
			if err != nil {
				return nil, &QueryEvaluationError{Err: err}
			}
			contained, err = w.queries.CoversKey(w.query.Fingerprint(), key)
			if err != nil {
				return nil, &QueryEvaluationError{Err: err}
			}
		}
		if !contained {
			continue
		}
		retained, added, err := w.queries.AddMember(w.query.Fingerprint(), entity.Key)
		if err != nil {
			return nil, err
		}
		if added {
			entity = retained
		}
		result = append(result, entity)
	}
	return result, nil
}

// Reconcile installs the authoritative entity images without changing query
// membership. Relation recovery uses this when a query was isolated while its
// shared snapshot read was in flight, so overlapping healthy queries can still
// consume coalesced entity loads.
func (w *PendingWindow) Reconcile(ctx context.Context) (err error) {
	if w.used {
		return errors.New("pending window was already activated")
	}
	w.used = true
	completed := false
	defer func() {
		if completed {
			return
		}
		for _, pending := range w.pending {
			if pending.load.Owned() {
				_ = w.entities.Fail(pending.load, err)
			}
		}
	}()
	for _, pending := range w.pending {
		if pending.load.Owned() {
			if pending.absent {
				current, exists := w.entities.Get(pending.load.Key())
				if exists && current.Generation == pending.load.Generation() && current.State == entitycache.EntityLoading && len(current.Record) > 0 {
					// There is no full baseline for this newly observed partial
					// entity. Fail this initialization rather than publish a
					// seemingly complete row with unresolved fields omitted.
					return storage.ErrResumeRequired
				}
			}
			err := w.entities.Complete(pending.load, pending.record)
			if err != nil && !errors.Is(err, entitycache.ErrStaleLoad) {
				return err
			}
			continue
		}
		select {
		case <-pending.load.Done():
			if err := w.entities.LoadError(pending.load); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	completed = true
	w.markUnreferenced()
	return nil
}

func (w *PendingWindow) markUnreferenced() {
	for _, pending := range w.pending {
		w.entities.MarkUnreferenced(pending.load.Key())
	}
}

// Cancel releases loads owned by this pending window. It is safe after another
// CDC path has superseded a load; those entries are left untouched.
func (w *PendingWindow) Cancel(err error) {
	if w.used {
		return
	}
	w.used = true
	if err == nil {
		err = context.Canceled
	}
	for _, pending := range w.pending {
		if pending.load.Owned() {
			_ = w.entities.Fail(pending.load, err)
		}
	}
}
