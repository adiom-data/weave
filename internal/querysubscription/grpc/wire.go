package querygrpc

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	querysubscriptionv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

func referenceSourceToProto(from *querysubscription.ReferenceSource) *querysubscriptionv1.ReferenceSource {
	if from == nil {
		return nil
	}
	return &querysubscriptionv1.ReferenceSource{SubscriptionId: from.SubscriptionID, ParentKey: entityKeyToProto(from.ParentKey)}
}

func referenceSourceFromProto(from *querysubscriptionv1.ReferenceSource) (*querysubscription.ReferenceSource, error) {
	if from == nil {
		return nil, nil
	}
	key, err := entityKeyFromProto(from.ParentKey)
	if err != nil {
		return nil, err
	}
	return &querysubscription.ReferenceSource{SubscriptionID: from.SubscriptionId, ParentKey: key}, nil
}

func referenceToProto(reference *storage.ReferenceFetch) (*querysubscriptionv1.ReferenceFetch, error) {
	if reference == nil {
		return nil, nil
	}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	query, err := queryToProto(reference.Query)
	if err != nil {
		return nil, err
	}
	result := &querysubscriptionv1.ReferenceFetch{Query: query}
	for _, binding := range reference.Bindings {
		from, err := fieldToProto(binding.Source)
		if err != nil {
			return nil, err
		}
		to, err := fieldToProto(binding.Target)
		if err != nil {
			return nil, err
		}
		result.Bindings = append(result.Bindings, &querysubscriptionv1.ReferenceBinding{Source: from, Target: to})
	}
	return result, nil
}

func referenceFromProto(reference *querysubscriptionv1.ReferenceFetch) (*storage.ReferenceFetch, error) {
	if reference == nil {
		return nil, nil
	}
	query, err := queryFromProto(reference.Query)
	if err != nil {
		return nil, err
	}
	result := &storage.ReferenceFetch{Query: query}
	for _, binding := range reference.Bindings {
		if binding == nil {
			return nil, errors.New("reference binding is required")
		}
		from, err := fieldFromProto(binding.Source)
		if err != nil {
			return nil, err
		}
		to, err := fieldFromProto(binding.Target)
		if err != nil {
			return nil, err
		}
		result.Bindings = append(result.Bindings, storage.ReferenceBinding{Source: from, Target: to})
	}
	return result, result.Validate()
}

func queryToProto(query storage.QuerySpec) (*querysubscriptionv1.Query, error) {
	result := &querysubscriptionv1.Query{Namespace: query.Relation.Namespace, Relation: query.Relation.Name}
	if result.Relation == "" {
		return nil, errors.New("query relation is required")
	}
	var err error
	result.Filter, err = predicateToProto(query.Filter)
	if err != nil {
		return nil, err
	}
	for _, term := range query.Order {
		field, fieldErr := fieldToProto(term.Field)
		if fieldErr != nil {
			return nil, fieldErr
		}
		result.Order = append(result.Order, &querysubscriptionv1.OrderTerm{Field: field, Descending: term.Descending, NullsFirst: term.NullsFirst})
	}
	return result, nil
}

func queryFromProto(query *querysubscriptionv1.Query) (storage.QuerySpec, error) {
	if query == nil || query.Relation == "" {
		return storage.QuerySpec{}, errors.New("query relation is required")
	}
	result := storage.QuerySpec{Relation: storage.RelationName{Namespace: query.Namespace, Name: query.Relation}}
	var err error
	result.Filter, err = predicateFromProto(query.Filter)
	if err != nil {
		return storage.QuerySpec{}, err
	}
	for _, term := range query.Order {
		if term == nil {
			return storage.QuerySpec{}, errors.New("order term is required")
		}
		field, fieldErr := fieldFromProto(term.Field)
		if fieldErr != nil {
			return storage.QuerySpec{}, fieldErr
		}
		result.Order = append(result.Order, storage.OrderTerm{Field: field, Descending: term.Descending, NullsFirst: term.NullsFirst})
	}
	return result, nil
}

func fieldToProto(field storage.FieldPath) (*querysubscriptionv1.FieldPath, error) {
	if err := field.Validate(); err != nil {
		return nil, err
	}
	return &querysubscriptionv1.FieldPath{Segments: append([]string(nil), field.Segments()...)}, nil
}

func fieldFromProto(field *querysubscriptionv1.FieldPath) (storage.FieldPath, error) {
	if field == nil {
		return "", errors.New("field path is required")
	}
	return storage.NewFieldPath(field.Segments...)
}

func predicateToProto(predicate *storage.Predicate) (*querysubscriptionv1.Predicate, error) {
	if predicate == nil {
		return nil, nil
	}
	set := 0
	if predicate.Comparison != nil {
		set++
	}
	if predicate.Null != nil {
		set++
	}
	if predicate.Junction != nil {
		set++
	}
	if set != 1 {
		return nil, errors.New("predicate requires exactly one expression")
	}
	result := &querysubscriptionv1.Predicate{}
	if comparison := predicate.Comparison; comparison != nil {
		field, err := fieldToProto(comparison.Field)
		if err != nil {
			return nil, err
		}
		value, err := valueToProto(comparison.Value.Value())
		if err != nil {
			return nil, err
		}
		op, err := comparisonToProto(comparison.Operator)
		if err != nil {
			return nil, err
		}
		result.Expression = &querysubscriptionv1.Predicate_Comparison{Comparison: &querysubscriptionv1.Comparison{Field: field, Operator: op, Value: value}}
		return result, nil
	}
	if nullTest := predicate.Null; nullTest != nil {
		field, err := fieldToProto(nullTest.Field)
		if err != nil {
			return nil, err
		}
		result.Expression = &querysubscriptionv1.Predicate_NullTest{NullTest: &querysubscriptionv1.NullTest{Field: field, IsNull: nullTest.IsNull}}
		return result, nil
	}
	junction := predicate.Junction
	op, err := junctionToProto(junction.Operator)
	if err != nil {
		return nil, err
	}
	converted := &querysubscriptionv1.Junction{Operator: op}
	for _, operand := range junction.Operands {
		child, err := predicateToProto(operand)
		if err != nil {
			return nil, err
		}
		if child == nil {
			return nil, errors.New("junction operand is required")
		}
		converted.Operands = append(converted.Operands, child)
	}
	result.Expression = &querysubscriptionv1.Predicate_Junction{Junction: converted}
	return result, nil
}

func predicateFromProto(predicate *querysubscriptionv1.Predicate) (*storage.Predicate, error) {
	if predicate == nil {
		return nil, nil
	}
	switch expression := predicate.Expression.(type) {
	case *querysubscriptionv1.Predicate_Comparison:
		if expression.Comparison == nil {
			return nil, errors.New("comparison is required")
		}
		field, err := fieldFromProto(expression.Comparison.Field)
		if err != nil {
			return nil, err
		}
		value, err := valueFromProto(expression.Comparison.Value)
		if err != nil {
			return nil, err
		}
		literal, err := storage.LiteralFromValue(value)
		if err != nil {
			return nil, err
		}
		op, err := comparisonFromProto(expression.Comparison.Operator)
		if err != nil {
			return nil, err
		}
		return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: op, Value: literal}}, nil
	case *querysubscriptionv1.Predicate_NullTest:
		if expression.NullTest == nil {
			return nil, errors.New("null test is required")
		}
		field, err := fieldFromProto(expression.NullTest.Field)
		if err != nil {
			return nil, err
		}
		return &storage.Predicate{Null: &storage.NullTest{Field: field, IsNull: expression.NullTest.IsNull}}, nil
	case *querysubscriptionv1.Predicate_Junction:
		if expression.Junction == nil {
			return nil, errors.New("junction is required")
		}
		op, err := junctionFromProto(expression.Junction.Operator)
		if err != nil {
			return nil, err
		}
		junction := &storage.Junction{Operator: op}
		for _, operand := range expression.Junction.Operands {
			child, err := predicateFromProto(operand)
			if err != nil {
				return nil, err
			}
			if child == nil {
				return nil, errors.New("junction operand is required")
			}
			junction.Operands = append(junction.Operands, child)
		}
		return &storage.Predicate{Junction: junction}, nil
	default:
		return nil, errors.New("predicate expression is required")
	}
}

func valueToProto(value storage.Value) (*querysubscriptionv1.Value, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	result := &querysubscriptionv1.Value{Type: string(value.Type)}
	switch value.State {
	case storage.ValueNull:
		result.State = querysubscriptionv1.Value_STATE_NULL
	case storage.ValueUnavailable:
		result.State = querysubscriptionv1.Value_STATE_UNAVAILABLE
	case storage.ValuePresent:
		result.State = querysubscriptionv1.Value_STATE_PRESENT
	default:
		return nil, errors.New("missing values must be represented by absent fields")
	}
	if value.State != storage.ValuePresent {
		return result, nil
	}
	switch value.Type {
	case storage.TypeDateTime:
		encoded, err := value.DateTime.MarshalBinary()
		if err != nil {
			return nil, err
		}
		result.Datetime = encoded
	case storage.TypeList:
		for _, child := range value.List {
			converted, err := valueToProto(child)
			if err != nil {
				return nil, err
			}
			result.List = append(result.List, converted)
		}
	case storage.TypeObject:
		result.Object = make(map[string]*querysubscriptionv1.Value, len(value.Object))
		for field, child := range value.Object {
			converted, err := valueToProto(child)
			if err != nil {
				return nil, err
			}
			result.Object[field] = converted
		}
	default:
		result.Data = append([]byte(nil), value.Data...)
	}
	return result, nil
}

func valueFromProto(value *querysubscriptionv1.Value) (storage.Value, error) {
	if value == nil {
		return storage.Value{}, errors.New("value is required")
	}
	result := storage.Value{Type: storage.ValueType(value.Type)}
	switch value.State {
	case querysubscriptionv1.Value_STATE_NULL:
		result.State = storage.ValueNull
	case querysubscriptionv1.Value_STATE_UNAVAILABLE:
		result.State = storage.ValueUnavailable
	case querysubscriptionv1.Value_STATE_PRESENT:
		result.State = storage.ValuePresent
	default:
		return storage.Value{}, errors.New("value state is required")
	}
	if result.State == storage.ValuePresent {
		switch result.Type {
		case storage.TypeDateTime:
			if len(value.Data) != 0 || len(value.List) != 0 || len(value.Object) != 0 {
				return storage.Value{}, errors.New("datetime value carries another payload")
			}
			var instant time.Time
			if err := instant.UnmarshalBinary(value.Datetime); err != nil {
				return storage.Value{}, fmt.Errorf("decode datetime: %w", err)
			}
			result.DateTime = storage.NormalizeDateTime(instant)
		case storage.TypeList:
			if len(value.Data) != 0 || len(value.Datetime) != 0 || len(value.Object) != 0 {
				return storage.Value{}, errors.New("list value carries another payload")
			}
			for _, child := range value.List {
				converted, err := valueFromProto(child)
				if err != nil {
					return storage.Value{}, err
				}
				result.List = append(result.List, converted)
			}
		case storage.TypeObject:
			if len(value.Data) != 0 || len(value.Datetime) != 0 || len(value.List) != 0 {
				return storage.Value{}, errors.New("object value carries another payload")
			}
			result.Object = make(map[string]storage.Value, len(value.Object))
			for field, child := range value.Object {
				converted, err := valueFromProto(child)
				if err != nil {
					return storage.Value{}, err
				}
				result.Object[field] = converted
			}
		default:
			if len(value.Datetime) != 0 || len(value.List) != 0 || len(value.Object) != 0 {
				return storage.Value{}, errors.New("scalar value carries another payload")
			}
			result.Data = append([]byte(nil), value.Data...)
		}
	} else if len(value.Data) != 0 || len(value.Datetime) != 0 || len(value.List) != 0 || len(value.Object) != 0 {
		return storage.Value{}, errors.New("non-present value carries a payload")
	}
	if err := result.Validate(); err != nil {
		return storage.Value{}, err
	}
	return result, nil
}

func recordToProto(record storage.Record) (*querysubscriptionv1.Record, error) {
	if record == nil {
		return nil, nil
	}
	result := &querysubscriptionv1.Record{Fields: make(map[string]*querysubscriptionv1.Value, len(record))}
	for field, value := range record {
		converted, err := valueToProto(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field, err)
		}
		result.Fields[field] = converted
	}
	return result, nil
}

func recordFromProto(record *querysubscriptionv1.Record) (storage.Record, error) {
	if record == nil {
		return nil, nil
	}
	result := make(storage.Record, len(record.Fields))
	for field, value := range record.Fields {
		converted, err := valueFromProto(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field, err)
		}
		result[field] = converted
	}
	return result, nil
}

func positionToProto(position *querysubscription.Position) *querysubscriptionv1.Position {
	if position == nil {
		return nil
	}
	return &querysubscriptionv1.Position{SourceId: string(position.SourceID), Value: position.Value}
}

func positionFromProto(position *querysubscriptionv1.Position) *querysubscription.Position {
	if position == nil {
		return nil
	}
	return &querysubscription.Position{SourceID: storage.SourceID(position.SourceId), Value: position.Value}
}

func entityKeyToProto(key storage.EntityKey) *querysubscriptionv1.EntityKey {
	return &querysubscriptionv1.EntityKey{Relation: &querysubscriptionv1.RelationKey{
		SourceId: string(key.Relation.Source), Namespace: key.Relation.Namespace, Name: key.Relation.Name, OpaqueId: key.Relation.OpaqueID,
	}, Value: key.Value}
}

func entityKeyFromProto(key *querysubscriptionv1.EntityKey) (storage.EntityKey, error) {
	if key == nil || key.Relation == nil {
		return storage.EntityKey{}, errors.New("entity key and relation are required")
	}
	return storage.EntityKey{Relation: storage.RelationKey{
		Source: storage.SourceID(key.Relation.SourceId), Namespace: key.Relation.Namespace, Name: key.Relation.Name, OpaqueID: key.Relation.OpaqueId,
	}, Value: key.Value}, nil
}

func rowToProto(row querysubscription.Row) (*querysubscriptionv1.Row, error) {
	record, err := recordToProto(row.Record)
	if err != nil {
		return nil, err
	}
	return &querysubscriptionv1.Row{Key: entityKeyToProto(row.Key), Record: record, Pending: row.Pending, Cursor: append([]byte(nil), row.Cursor...), ChangeIndex: row.ChangeIndex}, nil
}

func rowFromProto(row *querysubscriptionv1.Row) (querysubscription.Row, error) {
	if row == nil {
		return querysubscription.Row{}, errors.New("row is required")
	}
	key, err := entityKeyFromProto(row.Key)
	if err != nil {
		return querysubscription.Row{}, err
	}
	record, err := recordFromProto(row.Record)
	if err != nil {
		return querysubscription.Row{}, err
	}
	return querysubscription.Row{Key: key, Record: record, Pending: row.Pending, Cursor: append([]byte(nil), row.Cursor...), ChangeIndex: row.ChangeIndex}, nil
}

func boundaryToProto(boundary querysubscription.Boundary) (*querysubscriptionv1.Boundary, error) {
	kind, err := boundaryKindToProto(boundary.Kind)
	if err != nil {
		return nil, err
	}
	if boundary.Kind == querysubscription.BoundaryCursor && len(boundary.Cursor) == 0 {
		return nil, errors.New("cursor boundary requires a cursor")
	}
	return &querysubscriptionv1.Boundary{Kind: kind, Cursor: append([]byte(nil), boundary.Cursor...)}, nil
}

func boundaryFromProto(boundary *querysubscriptionv1.Boundary) (querysubscription.Boundary, error) {
	if boundary == nil {
		return querysubscription.Boundary{}, errors.New("boundary is required")
	}
	kind, err := boundaryKindFromProto(boundary.Kind)
	if err != nil {
		return querysubscription.Boundary{}, err
	}
	if kind == querysubscription.BoundaryCursor && len(boundary.Cursor) == 0 {
		return querysubscription.Boundary{}, errors.New("cursor boundary requires a cursor")
	}
	return querysubscription.Boundary{Kind: kind, Cursor: append([]byte(nil), boundary.Cursor...)}, nil
}

func boundsToProto(bounds querysubscription.Bounds) (*querysubscriptionv1.Bounds, error) {
	left, err := boundaryToProto(bounds.Left)
	if err != nil {
		return nil, err
	}
	right, err := boundaryToProto(bounds.Right)
	if err != nil {
		return nil, err
	}
	return &querysubscriptionv1.Bounds{Left: left, Right: right}, nil
}

func boundsFromProto(bounds *querysubscriptionv1.Bounds) (querysubscription.Bounds, error) {
	if bounds == nil {
		return querysubscription.Bounds{}, errors.New("bounds are required")
	}
	left, err := boundaryFromProto(bounds.Left)
	if err != nil {
		return querysubscription.Bounds{}, err
	}
	right, err := boundaryFromProto(bounds.Right)
	if err != nil {
		return querysubscription.Bounds{}, err
	}
	return querysubscription.Bounds{Left: left, Right: right}, nil
}

func snapshotToProto(snapshot querysubscription.QuerySnapshot) (*querysubscriptionv1.QuerySnapshot, error) {
	bounds, err := boundsToProto(snapshot.Bounds)
	if err != nil {
		return nil, err
	}
	result := &querysubscriptionv1.QuerySnapshot{Bounds: bounds, Checkpoint: positionToProto(snapshot.Checkpoint), Fence: positionToProto(snapshot.Fence), Revision: snapshot.Revision}
	result.OwnedReferences = snapshot.OwnedReferences
	for _, row := range snapshot.Rows {
		converted, rowErr := rowToProto(row.Row)
		if rowErr != nil {
			return nil, rowErr
		}
		value := &querysubscriptionv1.QueryRow{Row: converted}
		if row.Expanded != nil {
			expanded, err := entitySnapshotToProto(*row.Expanded)
			if err != nil {
				return nil, err
			}
			value.Expanded = expanded
		}
		result.Rows = append(result.Rows, value)
	}
	return result, nil
}

func snapshotFromProto(snapshot *querysubscriptionv1.QuerySnapshot) (querysubscription.QuerySnapshot, error) {
	if snapshot == nil {
		return querysubscription.QuerySnapshot{}, errors.New("snapshot is required")
	}
	bounds, err := boundsFromProto(snapshot.Bounds)
	if err != nil {
		return querysubscription.QuerySnapshot{}, err
	}
	result := querysubscription.QuerySnapshot{Bounds: bounds, Checkpoint: positionFromProto(snapshot.Checkpoint), Fence: positionFromProto(snapshot.Fence), Revision: snapshot.Revision}
	result.OwnedReferences = snapshot.OwnedReferences
	for _, row := range snapshot.Rows {
		if row == nil {
			return querysubscription.QuerySnapshot{}, errors.New("query row is required")
		}
		converted, rowErr := rowFromProto(row.Row)
		if rowErr != nil {
			return querysubscription.QuerySnapshot{}, rowErr
		}
		value := querysubscription.QueryRow{Row: converted}
		if row.Expanded != nil {
			expanded, err := entitySnapshotFromProto(row.Expanded)
			if err != nil {
				return querysubscription.QuerySnapshot{}, err
			}
			value.Expanded = &expanded
		}
		result.Rows = append(result.Rows, value)
	}
	return result, nil
}

func entitySnapshotToProto(snapshot querysubscription.EntitySnapshot) (*querysubscriptionv1.EntitySnapshot, error) {
	result := &querysubscriptionv1.EntitySnapshot{Checkpoint: positionToProto(snapshot.Checkpoint), Fence: positionToProto(snapshot.Fence), Revision: snapshot.Revision}
	if snapshot.Row != nil {
		clean := *snapshot.Row
		clean.Cursor = nil
		row, err := rowToProto(clean)
		if err != nil {
			return nil, err
		}
		result.Row = row
	}
	return result, nil
}

func entitySnapshotFromProto(snapshot *querysubscriptionv1.EntitySnapshot) (querysubscription.EntitySnapshot, error) {
	if snapshot == nil {
		return querysubscription.EntitySnapshot{}, errors.New("entity snapshot is required")
	}
	result := querysubscription.EntitySnapshot{Checkpoint: positionFromProto(snapshot.Checkpoint), Fence: positionFromProto(snapshot.Fence), Revision: snapshot.Revision}
	if snapshot.Row != nil {
		row, err := rowFromProto(snapshot.Row)
		if err != nil {
			return querysubscription.EntitySnapshot{}, err
		}
		row.Cursor = nil
		result.Row = &row
	}
	return result, nil
}

func changeToProto(change querysubscription.BoundaryChange) (*querysubscriptionv1.BoundaryChange, error) {
	side, err := boundarySideToProto(change.Side)
	if err != nil {
		return nil, err
	}
	operation, err := boundaryOperationToProto(change.Operation)
	if err != nil {
		return nil, err
	}
	if change.Count < 0 || uint64(change.Count) > math.MaxUint32 {
		return nil, errors.New("boundary count cannot be negative")
	}
	return &querysubscriptionv1.BoundaryChange{Side: side, Operation: operation, Count: uint32(change.Count), Cursor: append([]byte(nil), change.Cursor...)}, nil
}

func changeFromProto(change *querysubscriptionv1.BoundaryChange) (querysubscription.BoundaryChange, error) {
	if change == nil {
		return querysubscription.BoundaryChange{}, errors.New("boundary change is required")
	}
	side, err := boundarySideFromProto(change.Side)
	if err != nil {
		return querysubscription.BoundaryChange{}, err
	}
	operation, err := boundaryOperationFromProto(change.Operation)
	if err != nil {
		return querysubscription.BoundaryChange{}, err
	}
	return querysubscription.BoundaryChange{Side: side, Operation: operation, Count: int(change.Count), Cursor: append([]byte(nil), change.Cursor...)}, nil
}

func batchToProto(batch querysubscription.Batch) (*querysubscriptionv1.Batch, error) {
	result := &querysubscriptionv1.Batch{Clear: batch.Clear, ClearChangeIndex: batch.ClearChangeIndex}
	for _, key := range batch.BindingResets {
		result.BindingResets = append(result.BindingResets, entityKeyToProto(key))
	}
	for _, change := range batch.Changes {
		kind, err := rowChangeKindToProto(change.Kind)
		if err != nil {
			return nil, err
		}
		row, err := rowToProto(change.Row)
		if err != nil {
			return nil, err
		}
		result.Changes = append(result.Changes, &querysubscriptionv1.Change{Kind: kind, Row: row})
	}
	if len(batch.Membership.Removed) > 0 || len(batch.Membership.Insertions) > 0 {
		result.Membership = &querysubscriptionv1.MembershipDelta{}
		for _, key := range batch.Membership.Removed {
			result.Membership.Removed = append(result.Membership.Removed, entityKeyToProto(key))
		}
		for _, insertion := range batch.Membership.Insertions {
			if insertion.FinalIndex < 0 || uint64(insertion.FinalIndex) > math.MaxUint32 {
				return nil, errors.New("membership insertion index cannot be negative")
			}
			result.Membership.Insertions = append(result.Membership.Insertions, &querysubscriptionv1.Insertion{Key: entityKeyToProto(insertion.Key), FinalIndex: uint32(insertion.FinalIndex)})
		}
	}
	if batch.Window != nil {
		bounds, err := boundsToProto(batch.Window.Bounds)
		if err != nil {
			return nil, err
		}
		result.Window = &querysubscriptionv1.WindowTransition{Token: batch.Window.Token, Bounds: bounds, Fence: positionToProto(batch.Window.Fence), Checkpoint: positionToProto(batch.Window.Checkpoint)}
	}
	return result, nil
}

func batchFromProto(batch *querysubscriptionv1.Batch) (querysubscription.Batch, error) {
	if batch == nil {
		return querysubscription.Batch{}, errors.New("batch is required")
	}
	result := querysubscription.Batch{Clear: batch.Clear, ClearChangeIndex: batch.ClearChangeIndex}
	for _, key := range batch.BindingResets {
		value, err := entityKeyFromProto(key)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		result.BindingResets = append(result.BindingResets, value)
	}
	for _, change := range batch.Changes {
		if change == nil {
			return querysubscription.Batch{}, errors.New("change is required")
		}
		kind, err := rowChangeKindFromProto(change.Kind)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		row, err := rowFromProto(change.Row)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		result.Changes = append(result.Changes, querysubscription.Change{Kind: kind, Row: row})
	}
	if batch.Membership != nil {
		for _, key := range batch.Membership.Removed {
			converted, err := entityKeyFromProto(key)
			if err != nil {
				return querysubscription.Batch{}, err
			}
			result.Membership.Removed = append(result.Membership.Removed, converted)
		}
		for _, insertion := range batch.Membership.Insertions {
			if insertion == nil {
				return querysubscription.Batch{}, errors.New("membership insertion is required")
			}
			key, err := entityKeyFromProto(insertion.Key)
			if err != nil {
				return querysubscription.Batch{}, err
			}
			result.Membership.Insertions = append(result.Membership.Insertions, querysubscription.Insertion{Key: key, FinalIndex: int(insertion.FinalIndex)})
		}
	}
	if batch.Window != nil {
		bounds, err := boundsFromProto(batch.Window.Bounds)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		result.Window = &querysubscription.WindowTransition{Token: batch.Window.Token, Bounds: bounds, Fence: positionFromProto(batch.Window.Fence), Checkpoint: positionFromProto(batch.Window.Checkpoint)}
	}
	return result, nil
}

func envelopeToProto(envelope querysubscription.DeliveryEnvelope) (*querysubscriptionv1.DeliveryEnvelope, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	kind, err := deliveryKindToProto(envelope.Delivery.Kind)
	if err != nil {
		return nil, err
	}
	result := &querysubscriptionv1.DeliveryEnvelope{Delivery: &querysubscriptionv1.DeliveryMetadata{
		Kind: kind, Checkpoint: positionToProto(envelope.Delivery.Checkpoint), Fence: positionToProto(envelope.Delivery.Fence), Atomic: envelope.Delivery.Atomic,
		Revision: envelope.Delivery.Revision, PreviousRevision: envelope.Delivery.PreviousRevision,
	}}
	for _, effect := range envelope.Effects {
		if effect.SubscriptionID == "" {
			return nil, errors.New("subscription effect ID is required")
		}
		batch, err := batchToProto(effect.Batch)
		if err != nil {
			return nil, err
		}
		result.Effects = append(result.Effects, &querysubscriptionv1.SubscriptionEffect{SubscriptionId: effect.SubscriptionID, Batch: batch})
	}
	for _, entity := range envelope.Entities {
		converted, err := entityEffectToProto(entity)
		if err != nil {
			return nil, err
		}
		result.Entities = append(result.Entities, converted)
	}
	return result, nil
}

func envelopeFromProto(envelope *querysubscriptionv1.DeliveryEnvelope) (querysubscription.DeliveryEnvelope, error) {
	if envelope == nil || envelope.Delivery == nil {
		return querysubscription.DeliveryEnvelope{}, errors.New("delivery envelope and metadata are required")
	}
	kind, err := deliveryKindFromProto(envelope.Delivery.Kind)
	if err != nil {
		return querysubscription.DeliveryEnvelope{}, err
	}
	result := querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{
		Kind: kind, Checkpoint: positionFromProto(envelope.Delivery.Checkpoint), Fence: positionFromProto(envelope.Delivery.Fence), Atomic: envelope.Delivery.Atomic,
		Revision: envelope.Delivery.Revision, PreviousRevision: envelope.Delivery.PreviousRevision,
	}}
	for _, effect := range envelope.Effects {
		if effect == nil || effect.SubscriptionId == "" {
			return querysubscription.DeliveryEnvelope{}, errors.New("subscription effect and ID are required")
		}
		batch, err := batchFromProto(effect.Batch)
		if err != nil {
			return querysubscription.DeliveryEnvelope{}, err
		}
		result.Effects = append(result.Effects, querysubscription.SubscriptionEffect{SubscriptionID: effect.SubscriptionId, Batch: batch})
	}
	for _, entity := range envelope.Entities {
		converted, err := entityEffectFromProto(entity)
		if err != nil {
			return querysubscription.DeliveryEnvelope{}, err
		}
		result.Entities = append(result.Entities, converted)
	}
	if err := result.Validate(); err != nil {
		return querysubscription.DeliveryEnvelope{}, err
	}
	return result, nil
}

func comparisonToProto(operator storage.ComparisonOperator) (querysubscriptionv1.Comparison_Operator, error) {
	switch operator {
	case storage.ComparisonEqual:
		return querysubscriptionv1.Comparison_OPERATOR_EQUAL, nil
	case storage.ComparisonNotEqual:
		return querysubscriptionv1.Comparison_OPERATOR_NOT_EQUAL, nil
	case storage.ComparisonLess:
		return querysubscriptionv1.Comparison_OPERATOR_LESS_THAN, nil
	case storage.ComparisonLessOrEqual:
		return querysubscriptionv1.Comparison_OPERATOR_LESS_THAN_OR_EQUAL, nil
	case storage.ComparisonGreater:
		return querysubscriptionv1.Comparison_OPERATOR_GREATER_THAN, nil
	case storage.ComparisonGreaterOrEqual:
		return querysubscriptionv1.Comparison_OPERATOR_GREATER_THAN_OR_EQUAL, nil
	default:
		return 0, errors.New("unknown comparison operator")
	}
}

func comparisonFromProto(operator querysubscriptionv1.Comparison_Operator) (storage.ComparisonOperator, error) {
	switch operator {
	case querysubscriptionv1.Comparison_OPERATOR_EQUAL:
		return storage.ComparisonEqual, nil
	case querysubscriptionv1.Comparison_OPERATOR_NOT_EQUAL:
		return storage.ComparisonNotEqual, nil
	case querysubscriptionv1.Comparison_OPERATOR_LESS_THAN:
		return storage.ComparisonLess, nil
	case querysubscriptionv1.Comparison_OPERATOR_LESS_THAN_OR_EQUAL:
		return storage.ComparisonLessOrEqual, nil
	case querysubscriptionv1.Comparison_OPERATOR_GREATER_THAN:
		return storage.ComparisonGreater, nil
	case querysubscriptionv1.Comparison_OPERATOR_GREATER_THAN_OR_EQUAL:
		return storage.ComparisonGreaterOrEqual, nil
	default:
		return 0, errors.New("unknown comparison operator")
	}
}

func junctionToProto(operator storage.JunctionOperator) (querysubscriptionv1.Junction_Operator, error) {
	switch operator {
	case storage.JunctionAnd:
		return querysubscriptionv1.Junction_OPERATOR_AND, nil
	case storage.JunctionOr:
		return querysubscriptionv1.Junction_OPERATOR_OR, nil
	default:
		return 0, errors.New("unknown junction operator")
	}
}

func junctionFromProto(operator querysubscriptionv1.Junction_Operator) (storage.JunctionOperator, error) {
	switch operator {
	case querysubscriptionv1.Junction_OPERATOR_AND:
		return storage.JunctionAnd, nil
	case querysubscriptionv1.Junction_OPERATOR_OR:
		return storage.JunctionOr, nil
	default:
		return 0, errors.New("unknown junction operator")
	}
}

func boundaryKindToProto(kind querysubscription.BoundaryKind) (querysubscriptionv1.Boundary_Kind, error) {
	switch kind {
	case querysubscription.BoundaryStart:
		return querysubscriptionv1.Boundary_KIND_START, nil
	case querysubscription.BoundaryCursor:
		return querysubscriptionv1.Boundary_KIND_CURSOR, nil
	case querysubscription.BoundaryEnd:
		return querysubscriptionv1.Boundary_KIND_END, nil
	default:
		return 0, errors.New("unknown boundary kind")
	}
}

func boundaryKindFromProto(kind querysubscriptionv1.Boundary_Kind) (querysubscription.BoundaryKind, error) {
	switch kind {
	case querysubscriptionv1.Boundary_KIND_START:
		return querysubscription.BoundaryStart, nil
	case querysubscriptionv1.Boundary_KIND_CURSOR:
		return querysubscription.BoundaryCursor, nil
	case querysubscriptionv1.Boundary_KIND_END:
		return querysubscription.BoundaryEnd, nil
	default:
		return 0, errors.New("unknown boundary kind")
	}
}

func boundarySideToProto(side querysubscription.BoundarySide) (querysubscriptionv1.BoundaryChange_Side, error) {
	switch side {
	case querysubscription.BoundaryLeft:
		return querysubscriptionv1.BoundaryChange_SIDE_LEFT, nil
	case querysubscription.BoundaryRight:
		return querysubscriptionv1.BoundaryChange_SIDE_RIGHT, nil
	default:
		return 0, errors.New("unknown boundary side")
	}
}

func boundarySideFromProto(side querysubscriptionv1.BoundaryChange_Side) (querysubscription.BoundarySide, error) {
	switch side {
	case querysubscriptionv1.BoundaryChange_SIDE_LEFT:
		return querysubscription.BoundaryLeft, nil
	case querysubscriptionv1.BoundaryChange_SIDE_RIGHT:
		return querysubscription.BoundaryRight, nil
	default:
		return 0, errors.New("unknown boundary side")
	}
}

func boundaryOperationToProto(operation querysubscription.BoundaryOperation) (querysubscriptionv1.BoundaryChange_Operation, error) {
	switch operation {
	case querysubscription.BoundaryMove:
		return querysubscriptionv1.BoundaryChange_OPERATION_MOVE, nil
	case querysubscription.BoundaryExtend:
		return querysubscriptionv1.BoundaryChange_OPERATION_EXTEND, nil
	case querysubscription.BoundaryContract:
		return querysubscriptionv1.BoundaryChange_OPERATION_CONTRACT, nil
	default:
		return 0, errors.New("unknown boundary operation")
	}
}

func boundaryOperationFromProto(operation querysubscriptionv1.BoundaryChange_Operation) (querysubscription.BoundaryOperation, error) {
	switch operation {
	case querysubscriptionv1.BoundaryChange_OPERATION_MOVE:
		return querysubscription.BoundaryMove, nil
	case querysubscriptionv1.BoundaryChange_OPERATION_EXTEND:
		return querysubscription.BoundaryExtend, nil
	case querysubscriptionv1.BoundaryChange_OPERATION_CONTRACT:
		return querysubscription.BoundaryContract, nil
	default:
		return 0, errors.New("unknown boundary operation")
	}
}

func deliveryKindToProto(kind querysubscription.DeliveryKind) (querysubscriptionv1.DeliveryMetadata_Kind, error) {
	switch kind {
	case querysubscription.DeliveryCommit:
		return querysubscriptionv1.DeliveryMetadata_KIND_COMMIT, nil
	case querysubscription.DeliveryHydration:
		return querysubscriptionv1.DeliveryMetadata_KIND_HYDRATION, nil
	case querysubscription.DeliveryWindow:
		return querysubscriptionv1.DeliveryMetadata_KIND_WINDOW, nil
	default:
		return 0, errors.New("unknown delivery kind")
	}
}

func deliveryKindFromProto(kind querysubscriptionv1.DeliveryMetadata_Kind) (querysubscription.DeliveryKind, error) {
	switch kind {
	case querysubscriptionv1.DeliveryMetadata_KIND_COMMIT:
		return querysubscription.DeliveryCommit, nil
	case querysubscriptionv1.DeliveryMetadata_KIND_HYDRATION:
		return querysubscription.DeliveryHydration, nil
	case querysubscriptionv1.DeliveryMetadata_KIND_WINDOW:
		return querysubscription.DeliveryWindow, nil
	default:
		return 0, errors.New("unknown delivery kind")
	}
}

func rowChangeKindToProto(kind querysubscription.ChangeKind) (querysubscriptionv1.Change_Kind, error) {
	switch kind {
	case querysubscription.Added:
		return querysubscriptionv1.Change_KIND_ADDED, nil
	case querysubscription.Updated:
		return querysubscriptionv1.Change_KIND_UPDATED, nil
	case querysubscription.Removed:
		return querysubscriptionv1.Change_KIND_REMOVED, nil
	default:
		return 0, errors.New("unknown change kind")
	}
}

func rowChangeKindFromProto(kind querysubscriptionv1.Change_Kind) (querysubscription.ChangeKind, error) {
	switch kind {
	case querysubscriptionv1.Change_KIND_ADDED:
		return querysubscription.Added, nil
	case querysubscriptionv1.Change_KIND_UPDATED:
		return querysubscription.Updated, nil
	case querysubscriptionv1.Change_KIND_REMOVED:
		return querysubscription.Removed, nil
	default:
		return 0, errors.New("unknown change kind")
	}
}
