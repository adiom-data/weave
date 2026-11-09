// Package projectioncompiler compiles a constrained GraphQL read schema and
// query documents into the existing projection protocol and typed TS bindings.
// It is build-time tooling, independent of GraphQL execution and mutations.
package projectioncompiler

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

//go:embed prelude.graphql
var prelude string

type Type struct {
	Kind     string  `json:"kind"`
	Nullable bool    `json:"nullable,omitempty"`
	Element  *Type   `json:"element,omitempty"`
	Fields   []Field `json:"fields,omitempty"`
}
type Field struct {
	Name     string   `json:"name"`
	Path     []string `json:"path"`
	Type     Type     `json:"type"`
	Computed bool     `json:"computed,omitempty"`
}
type Shape struct {
	NodeID   string            `json:"nodeId"`
	Kind     string            `json:"kind"`
	Fields   []Field           `json:"fields"`
	Children map[string]*Shape `json:"children"`
}
type Parameter struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
}
type Operation struct {
	Name       string
	Parameters []Parameter
	Root       *reactivev1.ProjectionNode
	Shape      *Shape
}
type Compiler struct{ schema *ast.Schema }

func New(schemaSource *ast.Source) (*Compiler, error) {
	schema, err := gqlparser.LoadSchema(&ast.Source{Name: "weave.graphql", Input: prelude}, schemaSource)
	if err != nil {
		return nil, err
	}
	if schema.Query == nil {
		return nil, fmt.Errorf("read schema requires a Query type")
	}
	c := &Compiler{schema: schema}
	if err := c.validateMappings(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Compiler) Compile(source *ast.Source) ([]Operation, error) {
	// Parse separately so diagnostics retain the real document's filename.
	document, err := parseDocument(source)
	if err != nil {
		return nil, err
	}
	if errs := validator.Validate(c.schema, document); len(errs) != 0 {
		return nil, errs
	}
	if len(document.Operations) == 0 {
		return nil, fmt.Errorf("document contains no projections")
	}
	var operations []Operation
	for _, operation := range document.Operations {
		if operation.Operation != ast.Query || operation.Name == "" {
			return nil, fmt.Errorf("projections must be named query operations")
		}
		if len(operation.Directives) != 0 {
			return nil, fmt.Errorf("%s: operation directives are not supported", operation.Name)
		}
		var parameters []Parameter
		for _, variable := range operation.VariableDefinitions {
			if !variable.Type.NonNull || variable.Type.Elem != nil || variable.DefaultValue != nil || len(variable.Directives) != 0 {
				return nil, fmt.Errorf("%s: parameter $%s must be a required scalar without a default", operation.Name, variable.Variable)
			}
			typ, err := c.valueType(variable.Type, nil, 0)
			if err != nil {
				return nil, err
			}
			parameters = append(parameters, Parameter{Name: variable.Variable, Type: typ})
		}
		fields, err := collectFields(operation.SelectionSet, c.schema.Query.Name)
		if err != nil {
			return nil, err
		}
		if len(fields) != 1 {
			return nil, fmt.Errorf("%s: exactly one root entity or list is supported", operation.Name)
		}
		count := 0
		root, shape, err := c.node(fields[0], c.schema.Query, fields[0].Alias, 0, &count)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", operation.Name, err)
		}
		operations = append(operations, Operation{Name: operation.Name, Parameters: parameters, Root: root, Shape: shape})
	}
	return operations, nil
}

var scalarKinds = map[string]string{
	"Boolean": "bool", "String": "text", "Int": "int32", "Int64": "integer", "Float": "finiteFloat", "Float64": "float",
	"Decimal": "decimal", "Bytes": "bytes", "UUID": "uuid", "DateTime": "datetime", "StructuredValue": "structured",
}

func argument(d *ast.Directive, name string) *ast.Value {
	if d == nil {
		return nil
	}
	arg := d.Arguments.ForName(name)
	if arg == nil {
		return nil
	}
	return arg.Value
}
func text(v *ast.Value) string {
	if v == nil {
		return ""
	}
	return v.Raw
}
func stringsValue(v *ast.Value) []string {
	if v == nil {
		return nil
	}
	if v.Kind != ast.ListValue {
		return []string{v.Raw}
	}
	var out []string
	for _, child := range v.Children {
		out = append(out, child.Value.Raw)
	}
	return out
}
func column(field *ast.FieldDefinition) []string {
	if d := field.Directives.ForName("column"); d != nil {
		return stringsValue(argument(d, "path"))
	}
	return []string{field.Name}
}

func (c *Compiler) mappedField(def *ast.Definition, name string) (*ast.FieldDefinition, error) {
	field := def.Fields.ForName(name)
	if field == nil || field.Directives.ForName("relation") != nil || field.Directives.ForName("computed") != nil {
		return nil, fmt.Errorf("%s.%s is not a stored field", def.Name, name)
	}
	return field, nil
}

func (c *Compiler) validateMappings() error {
	for _, def := range c.schema.Types {
		if def.Kind != ast.Object || def.Name == c.schema.Query.Name || def.BuiltIn {
			continue
		}
		if table := def.Directives.ForName("table"); table != nil {
			if text(argument(table, "schema")) == "" || text(argument(table, "name")) == "" {
				return fmt.Errorf("%s: table schema/name cannot be empty", def.Name)
			}
			if deleted := text(argument(table, "softDelete")); deleted != "" {
				field, err := c.mappedField(def, deleted)
				if err != nil {
					return err
				}
				if field.Type.NonNull {
					return fmt.Errorf("%s.%s: soft-delete field must be nullable", def.Name, deleted)
				}
			}
		}
		for _, field := range def.Fields {
			if computed := field.Directives.ForName("computed"); computed != nil {
				if def.Directives.ForName("table") == nil || field.Directives.ForName("column") != nil || field.Directives.ForName("relation") != nil || len(field.Arguments) > 0 {
					return fmt.Errorf("%s.%s: computed fields require a table parent and cannot be columns, relations, or parameterized", def.Name, field.Name)
				}
				if name := argument(computed, "name"); name != nil && (name.Kind != ast.StringValue || name.Raw == "") {
					return fmt.Errorf("%s.%s: computed name must be a nonempty string", def.Name, field.Name)
				}
				name := field.Name
				if explicit := argument(computed, "name"); explicit != nil {
					name = explicit.Raw
				}
				if strings.HasPrefix(name, "_") || name == "$visibility" {
					return fmt.Errorf("%s.%s: private computed value %q cannot be exposed", def.Name, field.Name, name)
				}
			}
			for _, segment := range column(field) {
				if segment == "" {
					return fmt.Errorf("%s.%s: empty column path segment", def.Name, field.Name)
				}
			}
			if d := field.Directives.ForName("column"); d != nil && len(column(field)) == 0 {
				return fmt.Errorf("%s.%s: empty column path", def.Name, field.Name)
			}
			if relation := field.Directives.ForName("relation"); relation != nil {
				if field.Directives.ForName("column") != nil || def.Directives.ForName("table") == nil {
					return fmt.Errorf("%s.%s: relation requires a table parent and cannot be a column", def.Name, field.Name)
				}
				target := c.schema.Types[field.Type.Name()]
				if target == nil || target.Directives.ForName("table") == nil {
					return fmt.Errorf("%s.%s: relation target requires @table", def.Name, field.Name)
				}
				parents, children := stringsValue(argument(relation, "parent")), stringsValue(argument(relation, "child"))
				if len(parents) == 0 || len(parents) != len(children) {
					return fmt.Errorf("%s.%s: relation needs matching nonempty parent/child bindings", def.Name, field.Name)
				}
				for index, parent := range parents {
					left, err := c.mappedField(def, parent)
					if err != nil {
						return err
					}
					right, err := c.mappedField(target, children[index])
					if err != nil {
						return err
					}
					if left.Type.Name() != right.Type.Name() || left.Type.Elem != nil || right.Type.Elem != nil || scalarKinds[left.Type.Name()] == "" {
						return fmt.Errorf("%s.%s: relation binding types differ or are not scalar", def.Name, field.Name)
					}
					if !left.Type.NonNull || left.Type.Name() == "StructuredValue" {
						return fmt.Errorf("%s.%s: parent relation bindings must be non-null concrete scalars", def.Name, field.Name)
					}
				}
			}
		}
	}
	return nil
}
