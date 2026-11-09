package projectionserver

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

var (
	minimumProtobufDateTime = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	maximumProtobufDateTime = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
)

func appendModelBatch(target, source *reactivev1.ModelBatch) {
	if target == nil || source == nil {
		return
	}
	target.Entities = append(target.Entities, source.Entities...)
	target.Queries = append(target.Queries, source.Queries...)
	target.Projections = append(target.Projections, source.Projections...)
	target.Computed = append(target.Computed, source.Computed...)
	target.Branches = append(target.Branches, source.Branches...)
}

// Entity state is connection-normalized, so only the final event for an entity
// is observable from an atomic committed batch. Query and projection deltas
// remain independently ordered within their stable subscription identities.
func coalesceEntityEvents(batch *reactivev1.ModelBatch) {
	if batch == nil || len(batch.Entities) < 2 {
		return
	}
	indices := make(map[string]int, len(batch.Entities))
	result := make([]*reactivev1.EntityEvent, 0, len(batch.Entities))
	for _, event := range batch.Entities {
		if index, exists := indices[event.EntityId]; exists {
			result[index] = event
			continue
		}
		indices[event.EntityId] = len(result)
		result = append(result, event)
	}
	batch.Entities = result
}

func projectionNode(root *reactivev1.ProjectionNode, parameters map[string]*reactivev1.Value, maxSelectedFields, maxPredicateDepth, maxPredicateNodes int) (*proj.ProjectionNode, map[string]*fieldSelection, error) {
	selected := make(map[string]*fieldSelection)
	seen := make(map[string]struct{})
	windows := map[string]querysubscription.InitialWindow{}
	var build func(*reactivev1.ProjectionNode) (*plan.Node, error)
	build = func(node *reactivev1.ProjectionNode) (*plan.Node, error) {
		if node == nil || node.NodeId == "" {
			return nil, errors.New("every projection node requires node_id")
		}
		if _, exists := seen[node.NodeId]; exists {
			return nil, fmt.Errorf("projection node_id %q is duplicated", node.NodeId)
		}
		seen[node.NodeId] = struct{}{}

		var query *reactivev1.Query
		var bindings []*reactivev1.ProjectionBinding
		window := querysubscription.InitialWindow{Count: 1}
		switch selection := node.Selection.(type) {
		case *reactivev1.ProjectionNode_Entity:
			if selection.Entity == nil {
				return nil, fmt.Errorf("projection node %q has no entity definition", node.NodeId)
			}
			query, bindings = selection.Entity.Query, selection.Entity.Bindings
			// Entity nodes are represented as entity references publicly even
			// though the engine resolves them through a count-one query.
		case *reactivev1.ProjectionNode_List:
			if selection.List == nil || selection.List.Window == nil {
				return nil, fmt.Errorf("projection list %q requires an initial window", node.NodeId)
			}
			query, bindings = selection.List.Query, selection.List.Bindings
			if selection.List.Window.From != reactivev1.InitialWindow_FROM_START && selection.List.Window.From != reactivev1.InitialWindow_FROM_END {
				return nil, fmt.Errorf("projection list %q must start from START or END", node.NodeId)
			}
			window = querysubscription.InitialWindow{FromEnd: selection.List.Window.From == reactivev1.InitialWindow_FROM_END, Count: int(selection.List.Window.Count)}
		default:
			return nil, fmt.Errorf("projection node %q requires an entity or list selection", node.NodeId)
		}
		base, fields, err := projectionQuery(node, query, maxSelectedFields, maxPredicateDepth, maxPredicateNodes)
		if err != nil {
			return nil, fmt.Errorf("projection node %q: %w", node.NodeId, err)
		}
		selected[node.NodeId] = fields
		_, isEntity := node.Selection.(*reactivev1.ProjectionNode_Entity)
		windows[node.NodeId] = window
		engineNode := &plan.Node{Name: node.NodeId, Entity: isEntity, Query: base}
		if list := node.GetList(); list != nil {
			engineNode.FetchReference = list.FetchReference
		}
		for _, binding := range bindings {
			converted, err := projectionPlanBinding(binding)
			if err != nil {
				return nil, err
			}
			engineNode.Bindings = append(engineNode.Bindings, converted)
		}
		for _, child := range node.Children {
			converted, err := build(child)
			if err != nil {
				return nil, err
			}
			engineNode.Children = append(engineNode.Children, converted)
		}
		if _, err := plan.ReferenceHint(engineNode); err != nil {
			return nil, fmt.Errorf("projection node %q: %w", node.NodeId, err)
		}
		return engineNode, nil
	}
	converted, err := build(root)
	if err != nil {
		return nil, nil, err
	}
	values := storage.Record{}
	for key, value := range parameters {
		converted, err := storageValue(value)
		if err != nil {
			return nil, nil, err
		}
		values[key] = converted
	}
	return proj.FromPlan(converted, values, windows), selected, nil
}

func projectionPlanBinding(binding *reactivev1.ProjectionBinding) (plan.Binding, error) {
	field, err := storageFieldPath(binding.GetColumn())
	if binding == nil || err != nil {
		return plan.Binding{}, errors.New("projection binding requires a column")
	}
	op := map[reactivev1.Comparison_Operator]storage.ComparisonOperator{
		reactivev1.Comparison_OPERATOR_EQUAL:                 storage.ComparisonEqual,
		reactivev1.Comparison_OPERATOR_NOT_EQUAL:             storage.ComparisonNotEqual,
		reactivev1.Comparison_OPERATOR_LESS_THAN:             storage.ComparisonLess,
		reactivev1.Comparison_OPERATOR_LESS_THAN_OR_EQUAL:    storage.ComparisonLessOrEqual,
		reactivev1.Comparison_OPERATOR_GREATER_THAN:          storage.ComparisonGreater,
		reactivev1.Comparison_OPERATOR_GREATER_THAN_OR_EQUAL: storage.ComparisonGreaterOrEqual,
	}[binding.Operator]
	if op == 0 {
		return plan.Binding{}, fmt.Errorf("binding for %s requires a comparison operator", field.Display())
	}
	result := plan.Binding{Field: field, Operator: op}
	switch source := binding.Value.(type) {
	case *reactivev1.ProjectionBinding_Literal:
		value, err := storageValue(source.Literal)
		if err != nil {
			return result, err
		}
		if _, err := storage.LiteralFromValue(value); err != nil {
			return result, err
		}
		result.Literal = &value
	case *reactivev1.ProjectionBinding_Parameter:
		if source.Parameter == "" {
			return result, errors.New("binding parameter is required")
		}
		result.Parameter = storage.MustFieldPath(source.Parameter)
	case *reactivev1.ProjectionBinding_ParentColumn:
		parentField, pathErr := storageFieldPath(source.ParentColumn)
		if pathErr != nil {
			return result, pathErr
		}
		result.Parent = parentField
	default:
		return result, fmt.Errorf("binding for %s requires a value", field.Display())
	}
	return result, nil
}

func storageQuery(query *reactivev1.Query, maxSelectedFields, maxPredicateDepth, maxPredicateNodes int) (storage.QuerySpec, []storage.FieldPath, error) {
	spec, err := storageQuerySpec(query, maxPredicateDepth, maxPredicateNodes)
	if err != nil {
		return storage.QuerySpec{}, nil, err
	}
	if len(query.Columns) < 1 {
		return storage.QuerySpec{}, nil, errors.New("at least one selected column is required")
	}
	if len(query.Columns) > maxSelectedFields {
		return storage.QuerySpec{}, nil, fmt.Errorf("selected field count must not exceed %d", maxSelectedFields)
	}
	seen := make(map[storage.FieldPath]struct{})
	selected := make([]storage.FieldPath, 0, len(query.Columns))
	for _, column := range query.Columns {
		if len(column.GetSegments()) > maxSelectedFields {
			return storage.QuerySpec{}, nil, fmt.Errorf("selected field path depth must not exceed %d", maxSelectedFields)
		}
		path, pathErr := storageFieldPath(column)
		if pathErr != nil {
			return storage.QuerySpec{}, nil, pathErr
		}
		if _, ok := seen[path]; ok {
			return storage.QuerySpec{}, nil, fmt.Errorf("selected field %s is duplicated", path.Display())
		}
		seen[path] = struct{}{}
		selected = append(selected, path)
	}
	sort.Slice(selected, func(i, j int) bool { return string(selected[i]) < string(selected[j]) })
	return spec, selected, nil
}

// The datastore plan has no output selection: every read supplies full records.
// Both explicit-column and all-field publication use exactly this validation.
func storageQuerySpec(query *reactivev1.Query, maxPredicateDepth, maxPredicateNodes int) (storage.QuerySpec, error) {
	if query == nil || query.Schema == "" || query.Table == "" {
		return storage.QuerySpec{}, errors.New("schema and table are required")
	}
	filter, err := storagePredicateWithLimits(query.Filter, maxPredicateDepth, maxPredicateNodes)
	if err != nil {
		return storage.QuerySpec{}, err
	}
	spec := storage.QuerySpec{Relation: storage.RelationName{Namespace: query.Schema, Name: query.Table}, Filter: filter}
	for _, term := range query.Order {
		if term == nil {
			return storage.QuerySpec{}, errors.New("order term is required")
		}
		path, pathErr := storageFieldPath(term.Column)
		if pathErr != nil {
			return storage.QuerySpec{}, pathErr
		}
		spec.Order = append(spec.Order, storage.OrderTerm{Field: path, Descending: term.Descending, NullsFirst: term.NullsFirst})
	}
	return spec, nil
}

func storagePredicateWithLimits(predicate *reactivev1.Predicate, maxDepth, maxNodes int) (*storage.Predicate, error) {
	nodes := 0
	return storagePredicateCanonical(predicate, false, 1, &nodes, maxDepth, maxNodes)
}

// storagePredicateCanonical pushes every NOT to a comparison/null leaf using
// De Morgan's laws. The storage engine and adapter therefore index and
// fingerprint one negation-free predicate representation.
func storagePredicateCanonical(predicate *reactivev1.Predicate, negated bool, depth int, nodes *int, maxDepth, maxNodes int) (*storage.Predicate, error) {
	if predicate == nil {
		if negated {
			return nil, errors.New("NOT operand is required")
		}
		return nil, nil
	}
	(*nodes)++
	if depth > maxDepth || *nodes > maxNodes {
		return nil, errors.New("predicate exceeds nesting or node limit")
	}
	switch expression := predicate.Expression.(type) {
	case *reactivev1.Predicate_Comparison:
		op := map[reactivev1.Comparison_Operator]storage.ComparisonOperator{reactivev1.Comparison_OPERATOR_EQUAL: storage.ComparisonEqual, reactivev1.Comparison_OPERATOR_NOT_EQUAL: storage.ComparisonNotEqual, reactivev1.Comparison_OPERATOR_LESS_THAN: storage.ComparisonLess, reactivev1.Comparison_OPERATOR_LESS_THAN_OR_EQUAL: storage.ComparisonLessOrEqual, reactivev1.Comparison_OPERATOR_GREATER_THAN: storage.ComparisonGreater, reactivev1.Comparison_OPERATOR_GREATER_THAN_OR_EQUAL: storage.ComparisonGreaterOrEqual}[expression.Comparison.Operator]
		if op == 0 {
			return nil, errors.New("comparison operator is required")
		}
		if negated {
			op = map[storage.ComparisonOperator]storage.ComparisonOperator{
				storage.ComparisonEqual:          storage.ComparisonNotEqual,
				storage.ComparisonNotEqual:       storage.ComparisonEqual,
				storage.ComparisonLess:           storage.ComparisonGreaterOrEqual,
				storage.ComparisonLessOrEqual:    storage.ComparisonGreater,
				storage.ComparisonGreater:        storage.ComparisonLessOrEqual,
				storage.ComparisonGreaterOrEqual: storage.ComparisonLess,
			}[op]
		}
		field, err := storageFieldPath(expression.Comparison.Column)
		if err != nil {
			return nil, err
		}
		value, err := storageValue(expression.Comparison.Value)
		if err != nil {
			return nil, err
		}
		if value.State != storage.ValuePresent || value.Type == storage.TypeList || value.Type == storage.TypeObject {
			return nil, errors.New("comparison requires a present scalar value")
		}
		literal, err := storage.LiteralFromValue(value)
		if err != nil {
			return nil, err
		}
		return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: op, Value: literal}}, nil
	case *reactivev1.Predicate_NullTest:
		field, err := storageFieldPath(expression.NullTest.Column)
		if err != nil {
			return nil, err
		}
		return &storage.Predicate{Null: &storage.NullTest{Field: field, IsNull: expression.NullTest.IsNull != negated}}, nil
	case *reactivev1.Predicate_Junction:
		op := storage.JunctionAnd
		if expression.Junction.Operator == reactivev1.Junction_OPERATOR_OR {
			op = storage.JunctionOr
		} else if expression.Junction.Operator != reactivev1.Junction_OPERATOR_AND {
			return nil, errors.New("junction operator is required")
		}
		if negated {
			if op == storage.JunctionAnd {
				op = storage.JunctionOr
			} else {
				op = storage.JunctionAnd
			}
		}
		junction := &storage.Junction{Operator: op}
		for _, operand := range expression.Junction.Operands {
			child, err := storagePredicateCanonical(operand, negated, depth+1, nodes, maxDepth, maxNodes)
			if err != nil {
				return nil, err
			}
			junction.Operands = append(junction.Operands, child)
		}
		return &storage.Predicate{Junction: junction}, nil
	case *reactivev1.Predicate_Negation:
		if expression.Negation == nil || expression.Negation.Operand == nil {
			return nil, errors.New("NOT operand is required")
		}
		return storagePredicateCanonical(expression.Negation.Operand, !negated, depth+1, nodes, maxDepth, maxNodes)
	default:
		return nil, errors.New("predicate expression is required")
	}
}

func queryBoundaryChanges(update *reactivev1.UpdateWindow) ([]querysubscription.BoundaryChange, error) {
	if update == nil || len(update.Updates) == 0 {
		return nil, errors.New("at least one boundary update is required")
	}
	result := make([]querysubscription.BoundaryChange, 0, len(update.Updates))
	seenSides := make(map[reactivev1.BoundaryUpdate_Side]struct{}, 2)
	for _, change := range update.Updates {
		if change == nil {
			return nil, errors.New("boundary update is required")
		}
		converted := querysubscription.BoundaryChange{Count: int(change.Count)}
		switch change.Side {
		case reactivev1.BoundaryUpdate_SIDE_LEFT:
			converted.Side = querysubscription.BoundaryLeft
		case reactivev1.BoundaryUpdate_SIDE_RIGHT:
			converted.Side = querysubscription.BoundaryRight
		default:
			return nil, errors.New("boundary side is required")
		}
		if _, duplicate := seenSides[change.Side]; duplicate {
			return nil, errors.New("a window update may change each boundary at most once")
		}
		seenSides[change.Side] = struct{}{}
		switch change.Operation {
		case reactivev1.BoundaryUpdate_OPERATION_MOVE:
			if len(change.Cursor) == 0 {
				return nil, errors.New("move requires a cursor")
			}
			converted.Operation = querysubscription.BoundaryMove
			converted.Cursor = append([]byte(nil), change.Cursor...)
		case reactivev1.BoundaryUpdate_OPERATION_EXTEND:
			converted.Operation = querysubscription.BoundaryExtend
		case reactivev1.BoundaryUpdate_OPERATION_CONTRACT:
			converted.Operation = querysubscription.BoundaryContract
		default:
			return nil, errors.New("boundary operation is required")
		}
		result = append(result, converted)
	}
	return result, nil
}

func protoValues(record storage.Record, selected []string) (map[string]*reactivev1.Value, error) {
	values := make(map[string]*reactivev1.Value, len(selected))
	for _, name := range selected {
		value, ok := record[name]
		if !ok {
			return nil, fmt.Errorf("record lacks selected field %q", name)
		}
		converted, err := protoValue(value)
		if err != nil {
			return nil, fmt.Errorf("encode field %q: %w", name, err)
		}
		values[name] = converted
	}
	return values, nil
}

func storageFieldPath(path *reactivev1.FieldPath) (storage.FieldPath, error) {
	if path == nil {
		return "", errors.New("field path is required")
	}
	return storage.NewFieldPath(path.Segments...)
}

func storageValue(value *reactivev1.Value) (storage.Value, error) {
	if value == nil || value.Kind == nil {
		return storage.Value{}, errors.New("typed value is required")
	}
	result := storage.Value{State: storage.ValuePresent}
	var raw []byte
	switch kind := value.Kind.(type) {
	case *reactivev1.Value_NullValue:
		if !kind.NullValue {
			return storage.Value{}, errors.New("null_value must be true")
		}
		return storage.Value{State: storage.ValueNull}, nil
	case *reactivev1.Value_BoolValue:
		result.Type, raw = storage.TypeBool, []byte(strconv.FormatBool(kind.BoolValue))
	case *reactivev1.Value_IntegerValue:
		result.Type, raw = storage.TypeInteger, []byte(kind.IntegerValue)
	case *reactivev1.Value_DecimalValue:
		result.Type, raw = storage.TypeDecimal, []byte(kind.DecimalValue)
	case *reactivev1.Value_FloatValue:
		result.Type, raw = storage.TypeFloat, []byte(kind.FloatValue)
	case *reactivev1.Value_TextValue:
		result.Type, raw = storage.TypeText, []byte(kind.TextValue)
	case *reactivev1.Value_BytesValue:
		result.Type, raw = storage.TypeBytes, kind.BytesValue
	case *reactivev1.Value_UuidValue:
		result.Type, raw = storage.TypeUUID, []byte(kind.UuidValue)
	case *reactivev1.Value_DatetimeValue:
		if kind.DatetimeValue == nil {
			return storage.Value{}, errors.New("datetime_value is required")
		}
		if err := kind.DatetimeValue.CheckValid(); err != nil {
			return storage.Value{}, fmt.Errorf("invalid datetime_value: %w", err)
		}
		return storage.NewDateTime(kind.DatetimeValue.AsTime()), nil
	case *reactivev1.Value_ListValue:
		result.Type = storage.TypeList
		for _, child := range kind.ListValue.GetValues() {
			converted, err := storageValue(child)
			if err != nil {
				return storage.Value{}, err
			}
			result.List = append(result.List, converted)
		}
		return result, nil
	case *reactivev1.Value_ObjectValue:
		result.Type = storage.TypeObject
		result.Object = make(map[string]storage.Value, len(kind.ObjectValue.GetFields()))
		for key, child := range kind.ObjectValue.GetFields() {
			converted, err := storageValue(child)
			if err != nil {
				return storage.Value{}, err
			}
			result.Object[key] = converted
		}
		return result, nil
	default:
		return storage.Value{}, errors.New("unknown typed value")
	}
	return storage.ParseScalar(result.Type, raw)
}

func protoValue(value storage.Value) (*reactivev1.Value, error) {
	if value.State == storage.ValueNull {
		return &reactivev1.Value{Kind: &reactivev1.Value_NullValue{NullValue: true}}, nil
	}
	if value.State != storage.ValuePresent {
		return nil, fmt.Errorf("value state %d is not deliverable", value.State)
	}
	result := &reactivev1.Value{}
	switch value.Type {
	case storage.TypeBool:
		parsed, err := strconv.ParseBool(string(value.Data))
		if err != nil {
			return nil, err
		}
		result.Kind = &reactivev1.Value_BoolValue{BoolValue: parsed}
	case storage.TypeInteger:
		result.Kind = &reactivev1.Value_IntegerValue{IntegerValue: string(value.Data)}
	case storage.TypeDecimal:
		result.Kind = &reactivev1.Value_DecimalValue{DecimalValue: string(value.Data)}
	case storage.TypeFloat:
		result.Kind = &reactivev1.Value_FloatValue{FloatValue: string(value.Data)}
	case storage.TypeText:
		result.Kind = &reactivev1.Value_TextValue{TextValue: string(value.Data)}
	case storage.TypeBytes:
		result.Kind = &reactivev1.Value_BytesValue{BytesValue: append([]byte(nil), value.Data...)}
	case storage.TypeUUID:
		result.Kind = &reactivev1.Value_UuidValue{UuidValue: string(value.Data)}
	case storage.TypeDateTime:
		instant := protobufDateTime(value.DateTime)
		result.Kind = &reactivev1.Value_DatetimeValue{DatetimeValue: instant}
	case storage.TypeList:
		list := &reactivev1.ValueList{}
		for _, child := range value.List {
			converted, err := protoValue(child)
			if err != nil {
				return nil, err
			}
			list.Values = append(list.Values, converted)
		}
		result.Kind = &reactivev1.Value_ListValue{ListValue: list}
	case storage.TypeObject:
		object := &reactivev1.ValueObject{Fields: make(map[string]*reactivev1.Value, len(value.Object))}
		for key, child := range value.Object {
			converted, err := protoValue(child)
			if err != nil {
				return nil, err
			}
			object.Fields[key] = converted
		}
		result.Kind = &reactivev1.Value_ObjectValue{ObjectValue: object}
	default:
		return nil, fmt.Errorf("unsupported logical type %q", value.Type)
	}
	return result, nil
}

// protobufDateTime is the transport's single lossy datetime boundary. A
// future wire policy can return an error here instead of saturating without
// changing the storage or datastore adapters.
func protobufDateTime(value time.Time) *timestamppb.Timestamp {
	value = storage.NormalizeDateTime(value)
	if value.Before(minimumProtobufDateTime) {
		value = minimumProtobufDateTime
	} else if value.After(maximumProtobufDateTime) {
		value = maximumProtobufDateTime
	}
	return timestamppb.New(value)
}

func checkpointString(checkpoint storage.Checkpoint) string {
	if checkpoint == nil {
		return ""
	}
	return checkpoint.StableID()
}

type wireBounds struct {
	Left  *reactivev1.Boundary
	Right *reactivev1.Boundary
}

func protoQueryWindow(bounds querysubscription.Bounds) (wireBounds, error) {
	left, err := protoQueryBoundary(bounds.Left)
	if err != nil {
		return wireBounds{}, err
	}
	right, err := protoQueryBoundary(bounds.Right)
	if err != nil {
		return wireBounds{}, err
	}
	return wireBounds{Left: left, Right: right}, nil
}

func protoQueryBoundary(boundary querysubscription.Boundary) (*reactivev1.Boundary, error) {
	switch boundary.Kind {
	case querysubscription.BoundaryStart:
		return &reactivev1.Boundary{Value: &reactivev1.Boundary_Sentinel_{Sentinel: reactivev1.Boundary_SENTINEL_START}}, nil
	case querysubscription.BoundaryCursor:
		if len(boundary.Cursor) == 0 {
			return nil, errors.New("cursor boundary has no cursor")
		}
		return &reactivev1.Boundary{Value: &reactivev1.Boundary_Cursor{Cursor: append([]byte(nil), boundary.Cursor...)}}, nil
	case querysubscription.BoundaryEnd:
		return &reactivev1.Boundary{Value: &reactivev1.Boundary_Sentinel_{Sentinel: reactivev1.Boundary_SENTINEL_END}}, nil
	default:
		return nil, errors.New("unknown query boundary kind")
	}
}

func sendSharedError(stream Stream, subscriptionID, requestID, code, message string) error {
	return stream.Send(&reactivev1.ServerMessage{SubscriptionId: subscriptionID, RequestId: requestID, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: code, Message: message}}})
}
