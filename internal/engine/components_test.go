package engine

// Package-level tests predate the implementation split. These aliases keep
// those white-box tests concise without re-exporting implementation types from
// the production engine API.

import (
	"bytes"
	"strings"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
	routing     routing.RoutingPlan
	matches     func(storage.Record) bool
	matchError  error
	matchErr    func() error
	keyError    error
}

func (p fakePlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p fakePlan) Relation() storage.RelationKey           { return p.relation }
func (p fakePlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (p fakePlan) Matches(record storage.Record) (bool, error) {
	if p.matchErr != nil {
		if err := p.matchErr(); err != nil {
			return false, err
		}
	}
	if p.matchError != nil {
		return false, p.matchError
	}
	if p.matches != nil {
		return p.matches(record), nil
	}
	return true, nil
}
func (p fakePlan) Key(record storage.Record) (datastore.SortKey, error) {
	if p.keyError != nil {
		return nil, p.keyError
	}
	return datastore.EncodedSortKey(record["id"].Data), nil
}
func (p fakePlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (p fakePlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (p fakePlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), encoded...)), nil
}
func (p fakePlan) Query() storage.QuerySpec {
	query := storage.QuerySpec{}
	for _, exact := range p.routing.Exact {
		value := string(exact.Token)
		if _, suffix, found := strings.Cut(value, "="); found {
			value = suffix
		}
		query.Filter = &storage.Predicate{Comparison: &storage.Comparison{Field: exact.Field, Operator: storage.ComparisonEqual, Value: storage.Literal{Data: []byte(value)}}}
	}
	return query
}

type fakeChange struct {
	kind      datastore.ChangeKind
	relation  storage.RelationKey
	before    storage.EntityKey
	after     storage.EntityKey
	record    storage.Record
	complete  bool
	beforeErr error
	applyErr  error
}

func (c fakeChange) Kind() datastore.ChangeKind    { return c.kind }
func (c fakeChange) Relation() storage.RelationKey { return c.relation }
func (c fakeChange) BeforeKey() (storage.EntityKey, error) {
	return c.before, c.beforeErr
}
func (c fakeChange) Apply(storage.Record) (datastore.AppliedChange, error) {
	if c.applyErr != nil {
		return datastore.AppliedChange{}, c.applyErr
	}
	if c.kind == datastore.ChangeDelete {
		return datastore.AppliedChange{Kind: datastore.AppliedDeleted, Key: c.before}, nil
	}
	kind := datastore.AppliedPartial
	if c.complete {
		kind = datastore.AppliedComplete
	}
	return datastore.AppliedChange{Kind: kind, Key: c.after, Record: c.record}, nil
}

type EntitySnapshot = entitycache.EntitySnapshot
type Load = entitycache.Load
type EntityCache = entitycache.EntityCache

const (
	EntityLoading      = entitycache.EntityLoading
	EntityReconciling  = entitycache.EntityReconciling
	EntityCurrent      = entitycache.EntityCurrent
	EntityUnreferenced = entitycache.EntityUnreferenced
)

func NewEntityCache() *EntityCache { return entitycache.NewEntityCache() }

type QueryDescriptor = queryregistry.QueryDescriptor
type QueryRegistry = queryregistry.QueryRegistry

func NewQueryRegistry(entities *EntityCache) *QueryRegistry {
	return queryregistry.NewQueryRegistry(entities)
}

type QueryChange = changeprocessor.QueryChange
type Processor = changeprocessor.Processor

const (
	QueryAdded   = changeprocessor.QueryAdded
	QueryUpdated = changeprocessor.QueryUpdated
	QueryRemoved = changeprocessor.QueryRemoved
)

func NewProcessor(source datastore.DataSource, entities changeprocessor.EntityStore, queries changeprocessor.QueryIndex) *Processor {
	return changeprocessor.NewProcessor(source.ID(), entities, queries)
}

type Subscription = delivery.Subscription
type SubscriptionBatch = delivery.SubscriptionBatch
type SubscriptionEvent = delivery.SubscriptionEvent
type SubscriptionHub = delivery.SubscriptionHub

const DeliveryCommit = delivery.DeliveryCommit

var ErrClientQueueFull = delivery.ErrClientQueueFull

func NewClientQueue[T any](capacity int) *delivery.ClientQueue[T] {
	return delivery.NewClientQueue[T](capacity)
}

func NewSubscriptionHub(order delivery.WindowOrder) *SubscriptionHub {
	return delivery.NewSubscriptionHub(order)
}

func inCoverage(plan datastore.PreparedQuery, key datastore.SortKey, bounds []datastore.Bounds) bool {
	contained, _ := querywindow.Contains(plan, key, bounds)
	return contained
}

func compareBoundary(plan datastore.PreparedQuery, left, right datastore.Boundary) int {
	compared, _ := querywindow.CompareBoundary(plan, left, right)
	return compared
}
