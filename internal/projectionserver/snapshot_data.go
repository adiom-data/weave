package projectionserver

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// The normalized model remains an internal assembly detail: freshness is shared
// across diamonds, but each occurrence renders only its own selected fields.
func buildSnapshotResponse(request *pb.ProjectionSnapshotRequest, scope string, events []proj.ProjectionEvent, positions map[storage.SourceID]*q.Position, selected map[string]*fieldSelection) (*pb.ProjectionSnapshotResponse, error) {
	m := newConnectionModel()
	if _, err := m.applyProjection(scope, proj.ProjectionBatch{Events: events}, selected); err != nil {
		return nil, status.Errorf(codes.Internal, "build snapshot image: %v", err)
	}
	data, err := snapshotData(m, request.Root)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "materialize snapshot data: %v", err)
	}
	extensions := &pb.ProjectionSnapshotExtensions{Synchronized: request.WaitForSync}
	if request.WaitForSync && len(positions) == 1 {
		for _, position := range positions {
			extensions.Checkpoint = checkpointString(position)
		}
	}
	return &pb.ProjectionSnapshotResponse{Data: data, Extensions: extensions}, nil
}

// Explicit selections define their top-level output names without loading any
// rows. Select-all nodes have no such schema information; their collisions are
// checked while rendering and receive the same InvalidArgument status.
// Called only after projectionNode has validated the tree and field paths.
func validateSnapshotLayout(root *pb.ProjectionNode, selected map[string]*fieldSelection) error {
	fields := selected[root.NodeId].Names()
	for _, child := range root.Children {
		if _, exists := fields[child.NodeId]; exists {
			return projectionOutputCollision(root.NodeId, child.NodeId)
		}
		if err := validateSnapshotLayout(child, selected); err != nil {
			return err
		}
	}
	return nil
}

func snapshotData(m *connectionModel, root *pb.ProjectionNode) (*structpb.Struct, error) {
	type edge struct{ parent, entity, node string }
	index := make(map[edge]string, len(m.occurrences))
	for id, occurrence := range m.occurrences {
		key := edge{occurrence.parentOccurrenceID, occurrence.parentEntityID, occurrence.node}
		if _, exists := index[key]; exists {
			return nil, fmt.Errorf("duplicate occurrence for node %q", occurrence.node)
		}
		index[key] = id
	}
	var visit func(*pb.ProjectionNode, string, string) (*structpb.Value, error)
	visit = func(node *pb.ProjectionNode, parent, entity string) (*structpb.Value, error) {
		id := index[edge{parent, entity, node.NodeId}]
		occurrence := m.occurrences[id]
		row := func(entityID string, selected *fieldSelection) (*structpb.Value, error) {
			image := m.entities[entityID]
			if image == nil || !image.hasImage || image.pending {
				return nil, fmt.Errorf("node %q has an unresolved entity", node.NodeId)
			}
			record := selected.View(image.record)
			for name, value := range m.computed[id][entityID] {
				if _, collision := record[name]; collision {
					return nil, projectionOutputCollision(node.NodeId, name)
				}
				record[name] = value
			}
			values, err := protoAllValues(record)
			if err != nil {
				return nil, err
			}
			fields := make(map[string]*structpb.Value, len(values)+len(node.Children))
			for name, value := range values {
				fields[name], err = snapshotValue(value)
				if err != nil {
					return nil, err
				}
			}
			for _, child := range node.Children {
				if _, exists := fields[child.NodeId]; exists {
					return nil, projectionOutputCollision(node.NodeId, child.NodeId)
				}
				fields[child.NodeId], err = visit(child, id, entityID)
				if err != nil {
					return nil, err
				}
			}
			return structpb.NewStructValue(&structpb.Struct{Fields: fields}), nil
		}
		if node.GetEntity() != nil {
			if id == "" || occurrence.entityID == "" {
				return structpb.NewNullValue(), nil
			}
			return row(occurrence.entityID, occurrence.selected)
		}
		rows := &structpb.ListValue{}
		if id != "" {
			query := m.queries[occurrence.queryID]
			if query == nil {
				return nil, fmt.Errorf("missing query for node %q", node.NodeId)
			}
			for _, key := range query.ordered {
				if _, present := query.members[key]; !present {
					continue
				}
				value, err := row(publicEntityID(key), query.selected)
				if err != nil {
					return nil, err
				}
				rows.Values = append(rows.Values, value)
			}
		}
		return structpb.NewListValue(rows), nil
	}
	value, err := visit(root, "", "")
	if err != nil {
		return nil, err
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{root.NodeId: value}}, nil
}

// Reuse the transport's canonical conversion (including datetime bounds), then
// map its exact scalar representations to ordinary JSON-compatible values.
func snapshotValue(value *pb.Value) (*structpb.Value, error) {
	switch v := value.Kind.(type) {
	case *pb.Value_NullValue:
		return structpb.NewNullValue(), nil
	case *pb.Value_BoolValue:
		return structpb.NewBoolValue(v.BoolValue), nil
	case *pb.Value_IntegerValue:
		return structpb.NewStringValue(v.IntegerValue), nil
	case *pb.Value_DecimalValue:
		return structpb.NewStringValue(v.DecimalValue), nil
	case *pb.Value_FloatValue:
		n, err := strconv.ParseFloat(v.FloatValue, 64)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return structpb.NewStringValue(v.FloatValue), nil
		}
		return structpb.NewNumberValue(n), nil
	case *pb.Value_TextValue:
		return structpb.NewStringValue(v.TextValue), nil
	case *pb.Value_BytesValue:
		return structpb.NewStringValue(base64.StdEncoding.EncodeToString(v.BytesValue)), nil
	case *pb.Value_UuidValue:
		return structpb.NewStringValue(v.UuidValue), nil
	case *pb.Value_DatetimeValue:
		return structpb.NewStringValue(v.DatetimeValue.AsTime().UTC().Format(time.RFC3339Nano)), nil
	case *pb.Value_ObjectValue:
		fields := make(map[string]*structpb.Value, len(v.ObjectValue.Fields))
		for name, child := range v.ObjectValue.Fields {
			converted, err := snapshotValue(child)
			if err != nil {
				return nil, err
			}
			fields[name] = converted
		}
		return structpb.NewStructValue(&structpb.Struct{Fields: fields}), nil
	case *pb.Value_ListValue:
		list := &structpb.ListValue{}
		for _, child := range v.ListValue.Values {
			converted, err := snapshotValue(child)
			if err != nil {
				return nil, err
			}
			list.Values = append(list.Values, converted)
		}
		return structpb.NewListValue(list), nil
	default:
		return nil, fmt.Errorf("unsupported snapshot value %T", value.Kind)
	}
}
