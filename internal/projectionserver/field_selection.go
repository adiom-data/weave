package projectionserver

import (
	"fmt"
	"strings"

	"github.com/adiom-data/weave/internal/projection/selection"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fieldSelection struct {
	stored   *selection.Selection
	computed []string
}

func (s *fieldSelection) View(record storage.Record) storage.Record {
	return storedSelection(s).View(record)
}
func (s *fieldSelection) Names() map[string]struct{} { return storedSelection(s).Names() }
func storedSelection(s *fieldSelection) *selection.Selection {
	if s == nil {
		return nil
	}
	return s.stored
}
func computedSelection(s *fieldSelection) []string {
	if s == nil {
		return nil
	}
	return s.computed
}

// Explicit output masks are checked during definition validation. Whole-record
// selections discover their stored names at runtime, at the scope's publication
// boundary rather than while mutating the shared connection model.
func (s *fieldSelection) validateComputedOutput(node string, record, computed storage.Record) error {
	if storedSelection(s) == nil {
		for name := range computed {
			if _, collision := record[name]; collision {
				return projectionOutputCollision(node, name)
			}
		}
	}
	return nil
}

func projectionOutputCollision(node, output string) error {
	return status.Errorf(codes.InvalidArgument, "projection node %q: output %q collides with a selected field", node, output)
}

func selectColumns(paths []storage.FieldPath) *fieldSelection {
	return &fieldSelection{stored: selection.Paths(paths)}
}

func projectionQuery(node *pb.ProjectionNode, query *pb.Query, maxFields, maxDepth, maxNodes int) (storage.QuerySpec, *fieldSelection, error) {
	var spec storage.QuerySpec
	var selected *fieldSelection
	var err error
	if node.AllFields {
		if node.Fields != nil || len(query.GetColumns()) != 0 {
			return spec, nil, fmt.Errorf("all_fields is mutually exclusive with fields and query columns")
		}
		spec, err = storageQuerySpec(query, maxDepth, maxNodes)
	} else if len(query.GetColumns()) == 0 && len(node.Computed) > 0 && node.Fields == nil {
		spec, err = storageQuerySpec(query, maxDepth, maxNodes)
		selected = &fieldSelection{stored: selection.Object(nil)}
	} else {
		var columns []storage.FieldPath
		spec, columns, err = storageQuery(query, maxFields, maxDepth, maxNodes)
		if err == nil {
			selected, err = projectionFields(node.Fields, columns, maxFields)
		}
	}
	if err != nil {
		return spec, nil, err
	}
	if len(node.Computed)+len(query.GetColumns()) > maxFields {
		return spec, nil, fmt.Errorf("selected fields exceed limit %d", maxFields)
	}
	if len(node.Computed) > 0 {
		if selected == nil {
			selected = &fieldSelection{}
		}
		seen := map[string]bool{}
		stored := selected.Names()
		for _, name := range node.Computed {
			if name == "" || name == "$visibility" || strings.HasPrefix(name, "_") || seen[name] {
				return spec, nil, fmt.Errorf("invalid or duplicate computed field %q", name)
			}
			if _, collision := stored[name]; collision {
				return spec, nil, fmt.Errorf("computed field %q collides with a stored field", name)
			}
			for _, child := range node.Children {
				if child != nil && child.NodeId == name {
					return spec, nil, fmt.Errorf("computed field %q collides with a child", name)
				}
			}
			seen[name] = true
			selected.computed = append(selected.computed, name)
		}
	}
	return spec, selected, nil
}

// Decode output masks separately from storage queries: the query backend always
// supplies complete records and never sees publication-only field selections.
func projectionFields(fields *pb.ObjectSelection, columns []storage.FieldPath, limit int) (*fieldSelection, error) {
	stored, err := projectionStoredFields(fields, columns, limit)
	if err != nil {
		return nil, err
	}
	return &fieldSelection{stored: stored}, nil
}
func projectionStoredFields(fields *pb.ObjectSelection, columns []storage.FieldPath, limit int) (*selection.Selection, error) {
	if fields == nil {
		return selection.Paths(columns), nil
	}
	if len(fields.Fields) > limit {
		return nil, fmt.Errorf("output selection exceeds field limit %d", limit)
	}
	remaining := limit
	var object func(*pb.ObjectSelection, int) (*selection.Selection, error)
	var value func(*pb.ValueSelection, int) (*selection.Selection, error)
	consume := func(depth int) error {
		remaining--
		if remaining < 0 || depth > limit {
			return fmt.Errorf("output selection exceeds field/depth limit %d", limit)
		}
		return nil
	}
	object = func(input *pb.ObjectSelection, depth int) (*selection.Selection, error) {
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
	value = func(input *pb.ValueSelection, depth int) (*selection.Selection, error) {
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
