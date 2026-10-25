package projectionserver

import (
	"fmt"

	"github.com/adiom-data/weave/internal/projection/selection"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type fieldSelection = selection.Selection

func selectColumns(paths []storage.FieldPath) *fieldSelection { return selection.Paths(paths) }

func projectionQuery(node *pb.ProjectionNode, query *pb.Query, maxFields, maxPredicateDepth, maxPredicateNodes int) (storage.QuerySpec, *fieldSelection, error) {
	if node.AllFields {
		if node.Fields != nil || len(query.GetColumns()) != 0 {
			return storage.QuerySpec{}, nil, fmt.Errorf("all_fields is mutually exclusive with fields and query columns")
		}
		spec, err := storageQuerySpec(query, maxPredicateDepth, maxPredicateNodes)
		return spec, nil, err // Nil is the internal whole-record output selection.
	}
	spec, columns, err := storageQuery(query, maxFields, maxPredicateDepth, maxPredicateNodes)
	if err != nil {
		return storage.QuerySpec{}, nil, err
	}
	selected, err := projectionFields(node.Fields, columns, maxFields)
	return spec, selected, err
}

// Decode output masks separately from storage queries: the query backend always
// supplies complete records and never sees publication-only field selections.
func projectionFields(fields *pb.ObjectSelection, columns []storage.FieldPath, limit int) (*fieldSelection, error) {
	if fields == nil {
		return selectColumns(columns), nil
	}
	if len(fields.Fields) > limit {
		return nil, fmt.Errorf("output selection exceeds field limit %d", limit)
	}
	remaining := limit
	var object func(*pb.ObjectSelection, int) (*fieldSelection, error)
	var value func(*pb.ValueSelection, int) (*fieldSelection, error)
	consume := func(depth int) error {
		remaining--
		if remaining < 0 || depth > limit {
			return fmt.Errorf("output selection exceeds field/depth limit %d", limit)
		}
		return nil
	}
	object = func(input *pb.ObjectSelection, depth int) (*fieldSelection, error) {
		if input == nil || len(input.Fields) == 0 {
			return nil, fmt.Errorf("object output selection requires fields")
		}
		if len(input.Fields) > remaining {
			return nil, fmt.Errorf("output selection exceeds field limit %d", limit)
		}
		out := make([]selection.Field, 0, len(input.Fields))
		for _, field := range input.Fields {
			if field == nil {
				return nil, fmt.Errorf("output selection requires a field")
			}
			for i := range field.GetPath().GetSegments() {
				if err := consume(depth + i); err != nil {
					return nil, err
				}
			}
			path, err := storageFieldPath(field.Path)
			if err != nil {
				return nil, err
			}
			selected, err := value(field.Selection, depth+len(field.Path.Segments))
			if err != nil {
				return nil, err
			}
			out = append(out, selection.Field{Path: path, Value: selected})
		}
		return selection.Object(out), nil
	}
	value = func(input *pb.ValueSelection, depth int) (*fieldSelection, error) {
		if input == nil {
			return nil, nil
		}
		if err := consume(depth); err != nil {
			return nil, err
		}
		switch kind := input.Kind.(type) {
		case *pb.ValueSelection_Whole:
			if !kind.Whole {
				return nil, fmt.Errorf("whole output selection must be true")
			}
			return nil, nil
		case *pb.ValueSelection_Object:
			return object(kind.Object, depth+1)
		case *pb.ValueSelection_Elements:
			if kind.Elements == nil {
				return nil, fmt.Errorf("element output selection is required")
			}
			element, err := value(kind.Elements, depth+1)
			if err != nil {
				return nil, err
			}
			return selection.Elements(element), nil
		default:
			return nil, fmt.Errorf("output selection kind is required")
		}
	}
	// Masks can narrow selected columns but cannot introduce another column or
	// accidentally omit an existing one. Repeated paths union alias selections.
	expected := make(map[storage.FieldPath]bool, len(columns))
	for _, column := range columns {
		expected[column] = false
	}
	for _, field := range fields.Fields {
		if field == nil {
			return nil, fmt.Errorf("output selection requires a field")
		}
		path, err := storageFieldPath(field.Path)
		if err != nil {
			return nil, err
		}
		if _, ok := expected[path]; !ok {
			return nil, fmt.Errorf("output path %s is not a selected column", path.Display())
		}
		expected[path] = true
	}
	for path, present := range expected {
		if !present {
			return nil, fmt.Errorf("output selection omits selected column %s", path.Display())
		}
	}
	return object(fields, 1)
}
