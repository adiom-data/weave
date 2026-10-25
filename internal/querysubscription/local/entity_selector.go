package local

import (
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type entitySelection interface {
	apply(delivery.SubscriptionBatch) (querysubscription.Batch, error)
}

func newEntitySelection(plan datastore.PreparedQuery, initial []entitycache.EntitySnapshot) entitySelection {
	if identity, ok := plan.(datastore.IdentityQuery); ok {
		return &identitySelector{key: identity.LookupKey(), present: len(initial) > 0}
	}
	selector := &entitySelector{rows: make(map[storage.EntityKey]querysubscription.Row, len(initial))}
	for _, row := range initial {
		selector.order = append(selector.order, row.Key)
		selector.rows[row.Key] = querysubscription.Row{Key: row.Key, Record: storage.CloneRecord(row.Record), Pending: row.State != entitycache.EntityCurrent}
	}
	return selector
}

// identitySelector needs only presence: the datastore has proven a unique
// identity, so no alternate candidate can ever require promotion. Payloads
// arrive in each batch and are copied only for its outgoing result.
type identitySelector struct {
	key     storage.EntityKey
	present bool
}

func (s *identitySelector) apply(native delivery.SubscriptionBatch) (querysubscription.Batch, error) {
	before := s.present
	if native.Clear {
		s.present = false
	}
	var last querysubscription.Row
	lastIndex := native.ClearChangeIndex
	for _, event := range native.Events {
		if event.Key != s.key {
			return querysubscription.Batch{}, errors.New("identity selector received a foreign entity")
		}
		if _, err := changeKind(event.Kind); err != nil {
			return querysubscription.Batch{}, err
		}
		lastIndex = max(lastIndex, event.ChangeIndex)
		s.present = event.Kind != changeprocessor.QueryRemoved
		last = querysubscription.Row{Key: event.Key, Record: event.Record, Pending: event.Pending, ChangeIndex: event.ChangeIndex}
	}
	result := querysubscription.Batch{Clear: native.Clear, ClearChangeIndex: native.ClearChangeIndex}
	if native.Clear || before != s.present {
		if before && !s.present && !native.Clear {
			result.Changes = append(result.Changes, querysubscription.Change{Kind: querysubscription.Removed, Row: querysubscription.Row{Key: s.key, ChangeIndex: lastIndex}})
		}
		if s.present {
			last.Record = storage.CloneRecord(last.Record)
			if !native.Clear && last.ChangeIndex == 0 {
				last.ChangeIndex = lastIndex
			}
			result.Changes = append(result.Changes, querysubscription.Change{Kind: querysubscription.Added, Row: last})
		}
		return result, nil
	}
	if !s.present {
		return result, nil
	}
	for _, event := range native.Events {
		kind, _ := changeKind(event.Kind) // validated before changing state
		result.Changes = append(result.Changes, querysubscription.Change{Kind: kind, Row: querysubscription.Row{Key: event.Key, Record: storage.CloneRecord(event.Record), Pending: event.Pending, ChangeIndex: event.ChangeIndex}})
	}
	return result, nil
}

// entitySelector reduces ordered query candidates to a zero-or-one entity
// result. It is owned by one subscription and used only by its feed assembler;
// it has no lifecycle, transport, or coordinator responsibilities.
type entitySelector struct {
	order []storage.EntityKey
	rows  map[storage.EntityKey]querysubscription.Row
}

func (s *entitySelector) apply(native delivery.SubscriptionBatch) (querysubscription.Batch, error) {
	before, hadBefore := firstEntity(s.order)
	if native.Clear {
		s.order = nil
		s.rows = make(map[storage.EntityKey]querysubscription.Row)
	} else if s.rows == nil {
		s.rows = make(map[storage.EntityKey]querysubscription.Row)
	}
	lastIndex := native.ClearChangeIndex
	indices := make(map[storage.EntityKey]uint32, len(native.Events))
	for _, event := range native.Events {
		if event.ChangeIndex > lastIndex {
			lastIndex = event.ChangeIndex
		}
		indices[event.Key] = event.ChangeIndex
		if event.Kind == changeprocessor.QueryRemoved {
			delete(s.rows, event.Key)
			continue
		}
		s.rows[event.Key] = querysubscription.Row{
			Key: event.Key, Record: storage.CloneRecord(event.Record), Pending: event.Pending, ChangeIndex: event.ChangeIndex,
		}
	}
	membership := querysubscription.MembershipDelta{Removed: append([]storage.EntityKey(nil), native.Membership.Removed...)}
	for _, insertion := range native.Membership.Insertions {
		membership.Insertions = append(membership.Insertions, querysubscription.Insertion{Key: insertion.Key, FinalIndex: insertion.FinalIndex})
	}
	order, err := querysubscription.ApplyMembership(s.order, membership)
	if err != nil {
		return querysubscription.Batch{}, fmt.Errorf("apply entity candidate membership: %w", err)
	}
	s.order = order
	after, hasAfter := firstEntity(s.order)
	result := querysubscription.Batch{Clear: native.Clear, ClearChangeIndex: native.ClearChangeIndex}
	if native.Clear {
		if hasAfter {
			row, ok := s.rows[after]
			if !ok {
				return querysubscription.Batch{}, errors.New("selected entity candidate has no record")
			}
			row.ChangeIndex = indices[after]
			result.Changes = append(result.Changes, querysubscription.Change{Kind: querysubscription.Added, Row: row})
		}
		return result, nil
	}
	if hadBefore != hasAfter || (hadBefore && before != after) {
		if hadBefore {
			index := indices[before]
			if index == 0 {
				index = lastIndex
			}
			result.Changes = append(result.Changes, querysubscription.Change{Kind: querysubscription.Removed, Row: querysubscription.Row{Key: before, ChangeIndex: index}})
		}
		if hasAfter {
			row, ok := s.rows[after]
			if !ok {
				return querysubscription.Batch{}, errors.New("selected entity candidate has no record")
			}
			if row.ChangeIndex == 0 {
				row.ChangeIndex = lastIndex
			}
			result.Changes = append(result.Changes, querysubscription.Change{Kind: querysubscription.Added, Row: row})
		}
		return result, nil
	}
	if !hasAfter {
		return result, nil
	}
	for _, event := range native.Events {
		if event.Key != after {
			continue
		}
		kind, err := changeKind(event.Kind)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		result.Changes = append(result.Changes, querysubscription.Change{Kind: kind, Row: querysubscription.Row{
			Key: event.Key, Record: storage.CloneRecord(event.Record), Pending: event.Pending, ChangeIndex: event.ChangeIndex,
		}})
	}
	return result, nil
}

func firstEntity(order []storage.EntityKey) (storage.EntityKey, bool) {
	if len(order) == 0 {
		return storage.EntityKey{}, false
	}
	return order[0], true
}
