package projectioncompiler

import (
	"fmt"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func path(segments []string) *reactivev1.FieldPath { return &reactivev1.FieldPath{Segments: segments} }

func (c *Compiler) node(field *ast.Field, parent *ast.Definition, id string, depth int, count *int) (*reactivev1.ProjectionNode, *Shape, error) {
	*count++
	if depth > 32 || *count > 1024 {
		return nil, nil, fmt.Errorf("projection exceeds 32 expansion levels or 1024 nodes")
	}
	definition := field.Definition
	target := c.schema.Types[definition.Type.Name()]
	if target == nil || target.Kind != ast.Object || target.Directives.ForName("table") == nil {
		return nil, nil, fmt.Errorf("%s: expansion target requires @table", id)
	}
	if definition.Type.Elem != nil && definition.Type.Elem.Elem != nil {
		return nil, nil, fmt.Errorf("%s: relationship cannot be a direct list of lists", id)
	}
	table := target.Directives.ForName("table")
	query := &reactivev1.Query{Schema: text(argument(table, "schema")), Table: text(argument(table, "name"))}
	if deleted := text(argument(table, "softDelete")); deleted != "" {
		stored, err := c.mappedField(target, deleted)
		if err != nil {
			return nil, nil, err
		}
		query.Filter = &reactivev1.Predicate{Expression: &reactivev1.Predicate_NullTest{NullTest: &reactivev1.NullTest{Column: path(column(stored)), IsNull: true}}}
	}
	var bindings []*reactivev1.ProjectionBinding
	if parent != c.schema.Query {
		relation := definition.Directives.ForName("relation")
		if relation == nil {
			return nil, nil, fmt.Errorf("%s: expansion requires @relation", id)
		}
		parents, children := stringsValue(argument(relation, "parent")), stringsValue(argument(relation, "child"))
		for i, child := range children {
			left, _ := c.mappedField(parent, parents[i])
			right, _ := c.mappedField(target, child)
			bindings = append(bindings, &reactivev1.ProjectionBinding{Column: path(column(right)), Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.ProjectionBinding_ParentColumn{ParentColumn: path(column(left))}})
		}
	}
	bound := map[string]bool{}
	for _, d := range definition.Directives {
		if d.Name != "bind" {
			continue
		}
		name, fieldName := text(argument(d, "argument")), text(argument(d, "field"))
		if bound[name] {
			return nil, nil, fmt.Errorf("%s: duplicate @bind for %s", id, name)
		}
		bound[name] = true
		argDef := definition.Arguments.ForName(name)
		stored, err := c.mappedField(target, fieldName)
		if err != nil {
			return nil, nil, err
		}
		if argDef == nil || argDef.Type.Elem != nil || stored.Type.Elem != nil || argDef.Type.Name() != stored.Type.Name() || scalarKinds[stored.Type.Name()] == "" || stored.Type.Name() == "StructuredValue" {
			return nil, nil, fmt.Errorf("%s: argument binding %s has incompatible types", id, name)
		}
		arg := field.Arguments.ForName(name)
		var value *ast.Value
		if arg != nil {
			value = arg.Value
		} else {
			value = argDef.DefaultValue
		}
		if value == nil {
			continue
		}
		binding := &reactivev1.ProjectionBinding{Column: path(column(stored)), Operator: reactivev1.Comparison_OPERATOR_EQUAL}
		if value.Kind == ast.Variable {
			binding.Value = &reactivev1.ProjectionBinding_Parameter{Parameter: value.Raw}
		} else {
			literal, err := literal(stored.Type, value)
			if err != nil {
				return nil, nil, fmt.Errorf("%s: %w", id, err)
			}
			binding.Value = &reactivev1.ProjectionBinding_Literal{Literal: literal}
		}
		bindings = append(bindings, binding)
	}
	for _, argDef := range definition.Arguments {
		if !bound[argDef.Name] {
			return nil, nil, fmt.Errorf("%s: argument %s needs @bind", id, argDef.Name)
		}
	}
	var window *reactivev1.InitialWindow
	var fetchReference string
	allFields := false
	for _, directive := range field.Directives {
		switch directive.Name {
		case "allFields":
			allFields = true
		case "fetchReference":
			child := argument(directive, "child")
			if definition.Type.Elem == nil || child == nil || child.Kind != ast.StringValue || child.Raw == "" {
				return nil, nil, fmt.Errorf("%s: @fetchReference requires a list and a constant child alias", id)
			}
			fetchReference = id + "." + child.Raw
		case "window":
			if definition.Type.Elem == nil {
				return nil, nil, fmt.Errorf("%s: @window requires a list", id)
			}
			v := argument(directive, "initialCount")
			if v.Kind != ast.IntValue {
				return nil, nil, fmt.Errorf("%s: window count must be a constant integer", id)
			}
			count, err := strconv.ParseUint(v.Raw, 10, 32)
			if err != nil || count == 0 {
				return nil, nil, fmt.Errorf("%s: window count must be positive", id)
			}
			from := reactivev1.InitialWindow_FROM_START
			if text(argument(directive, "from")) == "END" {
				from = reactivev1.InitialWindow_FROM_END
			} else if value := argument(directive, "from"); value != nil && value.Kind != ast.EnumValue {
				return nil, nil, fmt.Errorf("%s: window direction must be constant", id)
			}
			window = &reactivev1.InitialWindow{Count: uint32(count), From: from}
		case "orderBy":
			terms := argument(directive, "terms")
			if terms.Kind != ast.ListValue {
				return nil, nil, fmt.Errorf("%s: order terms must be a constant list", id)
			}
			for _, item := range terms.Children {
				if item.Value.Kind != ast.ObjectValue {
					return nil, nil, fmt.Errorf("%s: order terms must be constant objects", id)
				}
				values := item.Value.Children
				fieldName := values.ForName("field")
				if fieldName == nil || fieldName.Kind != ast.StringValue {
					return nil, nil, fmt.Errorf("%s: order field must be a constant name", id)
				}
				stored, err := c.mappedField(target, fieldName.Raw)
				if err != nil {
					return nil, nil, err
				}
				if stored.Type.Elem != nil || scalarKinds[stored.Type.Name()] == "" || stored.Type.Name() == "StructuredValue" {
					return nil, nil, fmt.Errorf("%s: order field must be scalar", id)
				}
				for _, key := range []string{"descending", "nullsFirst"} {
					if v := values.ForName(key); v != nil && v.Kind != ast.BooleanValue {
						return nil, nil, fmt.Errorf("%s: order flags must be constant", id)
					}
				}
				query.Order = append(query.Order, &reactivev1.OrderTerm{Column: path(column(stored)), Descending: text(values.ForName("descending")) == "true", NullsFirst: text(values.ForName("nullsFirst")) == "true"})
			}
		case "where":
			predicate, err := c.predicate(target, argument(directive, "predicate"), 0)
			if err != nil {
				return nil, nil, err
			}
			if query.Filter == nil {
				query.Filter = predicate
			} else {
				query.Filter = &reactivev1.Predicate{Expression: &reactivev1.Predicate_Junction{Junction: &reactivev1.Junction{Operator: reactivev1.Junction_OPERATOR_AND, Operands: []*reactivev1.Predicate{query.Filter, predicate}}}}
			}
		default:
			return nil, nil, fmt.Errorf("%s: unsupported projection directive @%s", id, directive.Name)
		}
	}
	node := &reactivev1.ProjectionNode{NodeId: id, AllFields: allFields}
	shape := &Shape{NodeID: id, Kind: "entity", Fields: []Field{}, Children: map[string]*Shape{}}
	if definition.Type.Elem != nil {
		if window == nil {
			return nil, nil, fmt.Errorf("%s: every list requires @window", id)
		}
		node.Selection = &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{Query: query, Bindings: bindings, Window: window, FetchReference: fetchReference}}
		shape.Kind = "list"
	} else {
		node.Selection = &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{Query: query, Bindings: bindings}}
	}
	fields, err := collectFields(field.SelectionSet, target.Name)
	if err != nil {
		return nil, nil, err
	}
	selectedPaths := make(map[storage.FieldPath]struct{})
	outputFields := &reactivev1.ObjectSelection{}
	hasNestedSelection := false
	for _, selected := range fields {
		stored := selected.Definition
		if stored.Directives.ForName("relation") != nil {
			child, childShape, err := c.node(selected, target, id+"."+selected.Alias, depth+1, count)
			if err != nil {
				return nil, nil, err
			}
			node.Children = append(node.Children, child)
			shape.Children[selected.Alias] = childShape
		} else {
			if len(selected.Arguments) != 0 || len(selected.Directives) != 0 {
				return nil, nil, fmt.Errorf("%s.%s: scalar/embedded fields cannot have arguments or directives", id, selected.Name)
			}
			typ, err := c.valueType(stored.Type, selected.SelectionSet, 0)
			if err != nil {
				return nil, nil, err
			}
			segments := column(stored)
			if computed := stored.Directives.ForName("computed"); computed != nil {
				name := text(argument(computed, "name"))
				if name == "" {
					name = stored.Name
				}
				shape.Fields = append(shape.Fields, Field{Name: selected.Alias, Path: []string{name}, Type: typ, Computed: true})
				found := false
				for _, existing := range node.Computed {
					found = found || existing == name
				}
				if !found {
					node.Computed = append(node.Computed, name)
				}
				continue
			}
			shape.Fields = append(shape.Fields, Field{Name: selected.Alias, Path: segments, Type: typ})
			if allFields {
				continue // No column or nested output mask is needed for ALL.
			}
			key, err := storage.NewFieldPath(segments...)
			if err != nil {
				return nil, nil, fmt.Errorf("%s.%s: %w", id, selected.Name, err)
			}
			// Aliases have independent response shapes, but storage selections
			// are a set of exact paths, in first-selection order.
			if _, exists := selectedPaths[key]; !exists {
				query.Columns = append(query.Columns, path(segments))
				selectedPaths[key] = struct{}{}
			}
			valueSelection := outputValueSelection(typ)
			outputFields.Fields = append(outputFields.Fields, &reactivev1.SelectedField{Path: path(segments), Selection: valueSelection})
			hasNestedSelection = hasNestedSelection || valueSelection != nil
		}
	}
	// Publication is independent from the client shape. Only an explicit
	// directive may request all stored fields; empty columns alone never do.
	if len(query.Columns) == 0 && len(node.Computed) == 0 && !allFields {
		return nil, nil, fmt.Errorf("%s: select at least one stored field (for example id); relationship-only selections are not supported", id)
	}
	if hasNestedSelection {
		node.Fields = outputFields
	}
	if fetchReference != "" {
		var referenced *reactivev1.ProjectionEntity
		for _, child := range node.Children {
			if child.NodeId == fetchReference {
				referenced = child.GetEntity()
			}
		}
		if referenced == nil || len(referenced.Bindings) == 0 {
			return nil, nil, fmt.Errorf("%s: @fetchReference must name a selected immediate entity child with identity bindings", id)
		}
		for _, binding := range referenced.Bindings {
			if binding.GetParentColumn() == nil || binding.Operator != reactivev1.Comparison_OPERATOR_EQUAL {
				return nil, nil, fmt.Errorf("%s: @fetchReference requires only parent-column equality bindings", id)
			}
		}
	}
	return node, shape, nil
}

// These are publication masks in stored names, not output aliases or database
// query paths. Array traversal belongs here rather than the predicate language.
func outputValueSelection(typ Type) *reactivev1.ValueSelection {
	switch typ.Kind {
	case "object":
		object := &reactivev1.ObjectSelection{}
		for _, field := range typ.Fields {
			object.Fields = append(object.Fields, &reactivev1.SelectedField{Path: path(field.Path), Selection: outputValueSelection(field.Type)})
		}
		return &reactivev1.ValueSelection{Kind: &reactivev1.ValueSelection_Object{Object: object}}
	case "list":
		if typ.Element != nil {
			if element := outputValueSelection(*typ.Element); element != nil {
				return &reactivev1.ValueSelection{Kind: &reactivev1.ValueSelection_Elements{Elements: element}}
			}
		}
	}
	// Scalars, including StructuredValue, are explicitly atomic whole values.
	return nil
}
