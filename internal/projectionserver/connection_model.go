package projectionserver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/selection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type modelQuery struct {
	scope        string
	node         string
	publicBounds *querysubscription.Bounds
	selected     *fieldSelection
	members      map[storage.EntityKey]struct{}
	ordered      []storage.EntityKey
	version      uint64
}

type modelOccurrence struct {
	scope              string
	node               string
	parentOccurrenceID string
	parentEntityID     string
	entityID           string
	queryID            string
	selected           *fieldSelection
}

type connectionModel struct {
	computed         map[string]map[string]storage.Record
	queries          map[string]*modelQuery
	occurrences      map[string]modelOccurrence
	entityRefs       map[string]int
	entityKeys       map[string]storage.EntityKey
	entities         map[string]*modelEntity
	entitySelections map[string]*selection.Union
	batchDepth       int
	dirtyEntities    map[string]struct{}
}

func newConnectionModel() *connectionModel {
	return &connectionModel{
		computed: make(map[string]map[string]storage.Record),
		queries:  make(map[string]*modelQuery), occurrences: make(map[string]modelOccurrence),
		entityRefs: make(map[string]int), entityKeys: make(map[string]storage.EntityKey),
		entities:         make(map[string]*modelEntity),
		entitySelections: make(map[string]*selection.Union),
		dirtyEntities:    make(map[string]struct{}),
	}
}

// assembleBatch joins changes from multiple scopes into one publication. Public
// model operations nest inside it, so transient zero references cannot discard
// freshness state until the complete outgoing batch has been assembled.
func (m *connectionModel) assembleBatch(result *reactivev1.ModelBatch, apply func() error) (err error) {
	m.beginBatch()
	defer m.finishBatch(result, &err)
	return apply()
}

func (m *connectionModel) beginBatch() { m.batchDepth++ }

func (m *connectionModel) finishBatch(result *reactivev1.ModelBatch, cause *error) {
	m.batchDepth--
	if m.batchDepth != 0 {
		return
	}
	// On failure, keep dirty state for scope cleanup (or connection teardown).
	// No public output state is advanced by a partially renderable batch.
	if *cause != nil {
		return
	}
	publications := make(map[string]*entityPublication)
	for id := range m.dirtyEntities {
		if m.entityRefs[id] > 0 {
			if entity := m.entities[id]; entity != nil {
				publication, err := entity.prepare(id, func(record storage.Record) storage.Record { return m.selectRecordView(id, record) })
				if err != nil {
					*cause = err
					return
				}
				if publication != nil {
					publications[id] = publication
				}
			}
		}
	}
	for id := range m.dirtyEntities {
		if m.entityRefs[id] > 0 {
			if publication := publications[id]; publication != nil {
				entity := m.entities[id]
				entity.output, entity.outputRecord = publication.output, publication.record
				result.Entities = append(result.Entities, publication.event)
			}
			continue
		}
		entity := m.entities[id]
		if entity != nil && entity.output != entityUnpublished {
			result.Entities = append(result.Entities, &reactivev1.EntityEvent{EntityId: id, Event: &reactivev1.EntityEvent_Released{Released: &reactivev1.EntityReleased{}}})
		}
		delete(m.entityRefs, id)
		delete(m.entityKeys, id)
		delete(m.entities, id)
		delete(m.entitySelections, id)
	}
	clear(m.dirtyEntities)
}

func publicEntityID(key storage.EntityKey) string {
	buffer := []byte{1}
	for _, value := range []string{string(key.Relation.Source), key.Relation.Namespace, key.Relation.Name, key.Relation.OpaqueID, key.Value} {
		buffer = binary.AppendUvarint(buffer, uint64(len(value)))
		buffer = append(buffer, value...)
	}
	sum := sha256.Sum256(buffer)
	return "ent_" + base64.RawURLEncoding.EncodeToString(sum[:])
}

func (m *connectionModel) openSubscription(scope, queryID, node string, snapshot querysubscription.QuerySnapshot, selected []storage.FieldPath) (_ *reactivev1.ModelBatch, err error) {
	return m.openSubscriptionVersions(scope, queryID, node, snapshot, selected, nil)
}

// Retained initial materializations carry each row's own version. Advancing
// their coverage checkpoint must not relabel old payloads as newer records.
func (m *connectionModel) openSubscriptionVersions(scope, queryID, node string, snapshot querysubscription.QuerySnapshot, selected []storage.FieldPath, versions map[storage.EntityKey]querysubscription.RecordVersion) (_ *reactivev1.ModelBatch, err error) {
	bounds := snapshot.Bounds
	query := &modelQuery{scope: scope, node: node, publicBounds: &bounds, selected: selectColumns(selected), members: make(map[storage.EntityKey]struct{})}
	m.queries[queryID] = query
	result := &reactivev1.ModelBatch{Checkpoint: checkpointString(snapshot.Checkpoint)}
	m.beginBatch()
	defer m.finishBatch(result, &err)
	for _, row := range snapshot.Rows {
		if _, duplicate := query.members[row.Key]; duplicate {
			return nil, errors.New("query snapshot contains a duplicate row")
		}
		query.members[row.Key] = struct{}{}
		query.ordered = append(query.ordered, row.Key)
		m.retain(row.Key, query.selected)
		version, retained := versions[row.Key]
		if !retained {
			version = querysubscription.RecordVersion{Revision: snapshot.Revision, ChangeIndex: row.ChangeIndex}
		}
		m.acceptEntity(version, row.Key, row.Record, row.Pending)
	}
	if err := m.resetQuery(result, queryID, query); err != nil {
		return nil, err
	}
	return result, nil
}

func (m *connectionModel) applySubscriptionBatch(versions map[storage.EntityKey]querysubscription.RecordVersion, queryID string, delivery querysubscription.DeliveryMetadata, batch querysubscription.Batch) (_ *reactivev1.ModelBatch, err error) {
	query := m.queries[queryID]
	if query == nil {
		return &reactivev1.ModelBatch{}, nil
	}
	result := &reactivev1.ModelBatch{Checkpoint: checkpointString(delivery.Checkpoint)}
	m.beginBatch()
	defer m.finishBatch(result, &err)
	if batch.Clear {
		for key := range query.members {
			m.release(key, query.selected)
		}
		clear(query.members)
		query.ordered = nil
	}
	for _, change := range batch.Changes {
		switch change.Kind {
		case querysubscription.Added, querysubscription.Updated:
			if _, exists := query.members[change.Key]; !exists {
				query.members[change.Key] = struct{}{}
				m.retain(change.Key, query.selected)
			}
			version, retained := versions[change.Key]
			if !retained {
				version = querysubscription.RecordVersion{Revision: delivery.Revision, ChangeIndex: change.ChangeIndex}
			}
			m.acceptEntity(version, change.Key, change.Record, change.Pending)
		case querysubscription.Removed:
			if _, exists := query.members[change.Key]; exists {
				delete(query.members, change.Key)
				m.release(change.Key, query.selected)
			}
		default:
			return nil, errors.New("unknown query change kind")
		}
	}
	order, err := querysubscription.ApplyMembership(query.ordered, batch.Membership)
	if err != nil {
		return nil, fmt.Errorf("apply query membership delta: %w", err)
	}
	query.ordered = order
	for _, key := range query.ordered {
		if _, exists := query.members[key]; !exists {
			return nil, errors.New("query membership contains a row without data")
		}
	}
	if len(query.ordered) != len(query.members) {
		return nil, errors.New("query rows and ordered membership disagree")
	}
	if batch.Window != nil {
		bounds := batch.Window.Bounds
		query.publicBounds = &bounds
	}
	if batch.Clear {
		if err := m.resetQuery(result, queryID, query); err != nil {
			return nil, err
		}
	} else if len(batch.Membership.Removed) > 0 || len(batch.Membership.Insertions) > 0 {
		m.appendMembershipDelta(result, queryID, query, batch.Membership)
	}
	if batch.Window != nil {
		window, err := protoQueryWindow(batch.Window.Bounds)
		if err != nil {
			return nil, err
		}
		result.Queries = append(result.Queries, &reactivev1.QueryEvent{
			QuerySubscriptionId: queryID, NodeId: query.node,
			Event: &reactivev1.QueryEvent_WindowChanged{WindowChanged: &reactivev1.QueryWindowChanged{Left: window.Left, Right: window.Right}},
		})
	}
	return result, nil
}

func (m *connectionModel) appendMembershipDelta(result *reactivev1.ModelBatch, queryID string, query *modelQuery, delta querysubscription.MembershipDelta) {
	removed := make([]string, len(delta.Removed))
	for i, key := range delta.Removed {
		removed[i] = publicEntityID(key)
	}
	insertions := make([]*reactivev1.QueryInsertion, len(delta.Insertions))
	for i, insertion := range delta.Insertions {
		insertions[i] = &reactivev1.QueryInsertion{EntityId: publicEntityID(insertion.Key), FinalIndex: uint32(insertion.FinalIndex)}
	}
	base := query.version
	query.version++
	result.Queries = append(result.Queries, &reactivev1.QueryEvent{
		QuerySubscriptionId: queryID, NodeId: query.node,
		Event: &reactivev1.QueryEvent_Delta{Delta: &reactivev1.QueryDelta{BaseVersion: base, ResultVersion: query.version, RemovedEntityIds: removed, Insertions: insertions}},
	})
}

func (m *connectionModel) applyProjectionWindow(queryID string, bounds querysubscription.Bounds) (*reactivev1.ModelBatch, error) {
	query := m.queries[queryID]
	if query == nil {
		return nil, fmt.Errorf("query subscription %q does not exist", queryID)
	}
	query.publicBounds = &bounds
	window, err := protoQueryWindow(bounds)
	if err != nil {
		return nil, err
	}
	return &reactivev1.ModelBatch{Queries: []*reactivev1.QueryEvent{{
		QuerySubscriptionId: queryID,
		NodeId:              query.node,
		Event:               &reactivev1.QueryEvent_WindowChanged{WindowChanged: &reactivev1.QueryWindowChanged{Left: window.Left, Right: window.Right}},
	}}}, nil
}

func (m *connectionModel) applyProjection(scope string, batch proj.ProjectionBatch, selections ...map[string]*fieldSelection) (_ *reactivev1.ModelBatch, err error) {
	result := &reactivev1.ModelBatch{Checkpoint: checkpointString(batch.Delivery.Checkpoint)}
	m.beginBatch()
	defer m.finishBatch(result, &err)
	selectedByNode := optionalProjectionSelections(selections)
	newQueries := make(map[string]struct{})
	for _, event := range batch.Events {
		parentID := ""
		if event.ParentEntityKey.Relation.Source != "" {
			parentID = publicEntityID(event.ParentEntityKey)
		}
		switch event.Type {
		case proj.ProjectionInstanceOpened:
			if event.EntityNode {
				m.occurrences[event.InstanceID] = modelOccurrence{scope: scope, node: event.Node, parentOccurrenceID: event.ParentInstanceID, parentEntityID: parentID, selected: selectedByNode[event.Node]}
				continue
			}
			bounds := event.Bounds
			m.queries[event.InstanceID] = &modelQuery{scope: scope, node: event.Node, publicBounds: &bounds, selected: selectedByNode[event.Node], members: make(map[storage.EntityKey]struct{})}
			m.occurrences[event.InstanceID] = modelOccurrence{scope: scope, node: event.Node, parentOccurrenceID: event.ParentInstanceID, parentEntityID: parentID, queryID: event.InstanceID}
			result.Projections = append(result.Projections, projectionAttach(event.InstanceID, m.occurrences[event.InstanceID]))
			newQueries[event.InstanceID] = struct{}{}
		case proj.ProjectionRowChanged:
			occurrence, exists := m.occurrences[event.InstanceID]
			if !exists {
				return nil, fmt.Errorf("projection row belongs to unknown instance %q", event.InstanceID)
			}
			if event.Kind == querysubscription.Removed {
				id := publicEntityID(event.Key)
				if event.EntityNode && occurrence.entityID != "" {
					id = occurrence.entityID
				}
				m.releaseComputed(result, event.InstanceID, id)
			} else {
				if event.EntityNode && occurrence.entityID != "" && occurrence.entityID != publicEntityID(event.Key) {
					m.releaseComputed(result, event.InstanceID, occurrence.entityID)
				}
				if err := m.acceptComputed(result, event.InstanceID, publicEntityID(event.Key), event.Computed); err != nil {
					return nil, err
				}
			}
			if event.EntityNode {
				if event.Kind == querysubscription.Removed {
					if occurrence.entityID != "" {
						result.Projections = append(result.Projections, projectionDetach(event.InstanceID, occurrence))
						m.releaseID(occurrence.entityID, occurrence.selected)
						occurrence.entityID = ""
						m.occurrences[event.InstanceID] = occurrence
					}
					continue
				}
				entityID := publicEntityID(event.Key)
				if occurrence.entityID != "" && occurrence.entityID != entityID {
					result.Projections = append(result.Projections, projectionDetach(event.InstanceID, occurrence))
					m.releaseID(occurrence.entityID, occurrence.selected)
					occurrence.entityID = ""
				}
				if occurrence.entityID == "" {
					m.retain(event.Key, occurrence.selected)
					occurrence.entityID = entityID
					m.occurrences[event.InstanceID] = occurrence
					result.Projections = append(result.Projections, projectionAttach(event.InstanceID, occurrence))
				}
				m.acceptEntity(event.Version, event.Key, event.Record, event.Pending)
				continue
			}
			query := m.queries[event.InstanceID]
			if query == nil {
				return nil, fmt.Errorf("projection instance %q is not a query", event.InstanceID)
			}
			switch event.Kind {
			case querysubscription.Added:
				_, exists := query.members[event.Key]
				if !exists {
					m.retain(event.Key, query.selected)
					if _, orderedByDelta := batch.Membership[event.InstanceID]; !orderedByDelta {
						query.ordered = append(query.ordered, event.Key)
					}
				}
				query.members[event.Key] = struct{}{}
				m.acceptEntity(event.Version, event.Key, event.Record, event.Pending)
			case querysubscription.Updated:
				_, exists := query.members[event.Key]
				if !exists {
					m.retain(event.Key, query.selected)
					if _, orderedByDelta := batch.Membership[event.InstanceID]; !orderedByDelta {
						query.ordered = append(query.ordered, event.Key)
					}
				}
				query.members[event.Key] = struct{}{}
				m.acceptEntity(event.Version, event.Key, event.Record, event.Pending)
			case querysubscription.Removed:
				if _, exists := query.members[event.Key]; exists {
					delete(query.members, event.Key)
					m.release(event.Key, query.selected)
				}
			}
		case proj.ProjectionInstanceClosed:
			m.closeComputed(result, event.InstanceID)
			occurrence, exists := m.occurrences[event.InstanceID]
			if !exists {
				continue
			}
			if occurrence.queryID != "" {
				m.closeQuery(result, occurrence.queryID)
			} else if occurrence.entityID != "" {
				m.releaseID(occurrence.entityID, occurrence.selected)
			}
			result.Projections = append(result.Projections, projectionDetach(event.InstanceID, occurrence))
			delete(m.occurrences, event.InstanceID)
		}
	}
	for queryID, delta := range batch.Membership {
		query := m.queries[queryID]
		if query == nil {
			continue
		}
		order, err := querysubscription.ApplyMembership(query.ordered, delta)
		if err != nil {
			return nil, fmt.Errorf("apply projection membership delta: %w", err)
		}
		query.ordered = order
		if _, opened := newQueries[queryID]; !opened && (len(delta.Removed) > 0 || len(delta.Insertions) > 0) {
			m.appendMembershipDelta(result, queryID, query, delta)
		}
	}
	for queryID := range newQueries {
		if query := m.queries[queryID]; query != nil {
			if err := m.resetQuery(result, queryID, query); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (m *connectionModel) closeScope(scope string) (_ *reactivev1.ModelBatch, err error) {
	result := &reactivev1.ModelBatch{}
	m.beginBatch()
	defer m.finishBatch(result, &err)
	for id, occurrence := range m.occurrences {
		if occurrence.scope != scope {
			continue
		}
		m.closeComputed(result, id)
		if occurrence.queryID != "" {
			m.closeQuery(result, occurrence.queryID)
		} else if occurrence.entityID != "" {
			m.releaseID(occurrence.entityID, occurrence.selected)
		}
		result.Projections = append(result.Projections, projectionDetach(id, occurrence))
		delete(m.occurrences, id)
	}
	for id, query := range m.queries {
		if query.scope == scope {
			m.closeQuery(result, id)
		}
	}
	return result, nil
}

func (m *connectionModel) closeQuery(result *reactivev1.ModelBatch, queryID string) {
	query := m.queries[queryID]
	if query == nil {
		return
	}
	for key := range query.members {
		m.release(key, query.selected)
	}
	result.Queries = append(result.Queries, &reactivev1.QueryEvent{QuerySubscriptionId: queryID, NodeId: query.node, Event: &reactivev1.QueryEvent_Closed{Closed: &reactivev1.QueryClosed{}}})
	delete(m.queries, queryID)
}

func (m *connectionModel) resetQuery(result *reactivev1.ModelBatch, queryID string, query *modelQuery) error {
	ids := m.orderedQueryIDs(query)
	window, err := modelWindow(query)
	if err != nil {
		return err
	}
	query.version++
	result.Queries = append(result.Queries, &reactivev1.QueryEvent{QuerySubscriptionId: queryID, NodeId: query.node, Event: &reactivev1.QueryEvent_Reset_{Reset_: &reactivev1.QueryReset{Left: window.Left, Right: window.Right, EntityIds: ids, Version: query.version}}})
	return nil
}

func modelWindow(query *modelQuery) (wireBounds, error) {
	if query.publicBounds == nil {
		return wireBounds{}, errors.New("query subscription has no public bounds")
	}
	return protoQueryWindow(*query.publicBounds)
}

func (m *connectionModel) orderedQueryIDs(query *modelQuery) []string {
	ids := make([]string, 0, len(query.ordered))
	for _, key := range query.ordered {
		ids = append(ids, publicEntityID(key))
	}
	return ids
}

func (m *connectionModel) retain(key storage.EntityKey, selected *fieldSelection) string {
	id := publicEntityID(key)
	m.entityRefs[id]++
	m.entityKeys[id] = key
	m.addSelection(id, selected, 1)
	m.dirtyEntities[id] = struct{}{}
	return id
}

func (m *connectionModel) release(key storage.EntityKey, selected *fieldSelection) {
	m.releaseID(publicEntityID(key), selected)
}

func (m *connectionModel) releaseID(id string, selected *fieldSelection) {
	if m.entityRefs[id] == 0 {
		return
	}
	m.entityRefs[id]--
	m.addSelection(id, selected, -1)
	m.dirtyEntities[id] = struct{}{}
}

func (m *connectionModel) acceptEntity(version querysubscription.RecordVersion, key storage.EntityKey, record storage.Record, pending bool) {
	id := publicEntityID(key)
	entity := m.entities[id]
	if entity == nil {
		entity = &modelEntity{}
		m.entities[id] = entity
	}
	entity.accept(version, record, pending)
	// Even a rejected older image may accompany a new field selection. Output
	// derives from current accepted state, not from that incoming image.
	m.dirtyEntities[id] = struct{}{}
}

func optionalProjectionSelections(selections []map[string]*fieldSelection) map[string]*fieldSelection {
	if len(selections) == 0 {
		return nil
	}
	return selections[0]
}

func (m *connectionModel) addSelection(id string, selected *fieldSelection, delta int) {
	union := m.entitySelections[id]
	if union == nil {
		union = &selection.Union{}
		m.entitySelections[id] = union
	}
	union.Add(storedSelection(selected), delta)
}

// selectRecordView owns its outer map and any selection-path scaffolding, but
// borrows field values from the accepted image. It is read-only until prepare
// determines that a publication is necessary and takes ownership of the values.
func (m *connectionModel) selectRecordView(id string, record storage.Record) storage.Record {
	return m.entitySelections[id].View(record)
}

func protoAllValues(record storage.Record) (map[string]*reactivev1.Value, error) {
	selected := make([]string, 0, len(record))
	for name := range record {
		selected = append(selected, name)
	}
	sort.Strings(selected)
	return protoValues(record, selected)
}

func projectionAttach(occurrenceID string, occurrence modelOccurrence) *reactivev1.ProjectionReference {
	target := &reactivev1.ProjectionTarget{}
	if occurrence.entityID != "" {
		target.Target = &reactivev1.ProjectionTarget_EntityId{EntityId: occurrence.entityID}
	} else {
		target.Target = &reactivev1.ProjectionTarget_QuerySubscriptionId{QuerySubscriptionId: occurrence.queryID}
	}
	return &reactivev1.ProjectionReference{OccurrenceId: occurrenceID, NodeId: occurrence.node, ParentEntityId: occurrence.parentEntityID, SubscriptionId: occurrence.scope, ParentOccurrenceId: occurrence.parentOccurrenceID, Event: &reactivev1.ProjectionReference_Attached{Attached: target}}
}

func projectionDetach(occurrenceID string, occurrence modelOccurrence) *reactivev1.ProjectionReference {
	return &reactivev1.ProjectionReference{OccurrenceId: occurrenceID, NodeId: occurrence.node, ParentEntityId: occurrence.parentEntityID, SubscriptionId: occurrence.scope, ParentOccurrenceId: occurrence.parentOccurrenceID, Event: &reactivev1.ProjectionReference_Detached{Detached: &reactivev1.ProjectionDetached{}}}
}
