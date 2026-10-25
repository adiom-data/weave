package projectioncompiler

import (
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func parseDocument(source *ast.Source) (*ast.QueryDocument, error) { return parser.ParseQuery(source) }

// Collect aliases/fragments before lowering; repeated selections merge just
// as GraphQL validation permits. Reactive directives must agree on duplicates.
func collectFields(set ast.SelectionSet, typeName string) ([]*ast.Field, error) {
	var result []*ast.Field
	byName := map[string]*ast.Field{}
	// Fragment expansion is a graph walk, not a tree walk. Keep this scoped to
	// one collection so other aliases/occurrences still collect their fields.
	visitedFragments := map[string]struct{}{}
	var visit func(ast.SelectionSet) error
	visit = func(selections ast.SelectionSet) error {
		for _, selection := range selections {
			switch s := selection.(type) {
			case *ast.Field:
				if strings.HasPrefix(s.Alias, "__") {
					return fmt.Errorf("projection response names starting with __ are reserved")
				}
				if strings.HasPrefix(s.Name, "__") {
					return fmt.Errorf("introspection fields are not projection fields")
				}
				if existing := byName[s.Alias]; existing != nil {
					if directiveKey(existing.Directives) != directiveKey(s.Directives) {
						return fmt.Errorf("%s: repeated selections must use identical reactive directives", s.Alias)
					}
					existing.SelectionSet = append(existing.SelectionSet, s.SelectionSet...)
				} else {
					copy := *s
					copy.SelectionSet = append(ast.SelectionSet(nil), s.SelectionSet...)
					byName[s.Alias] = &copy
					result = append(result, &copy)
				}
			case *ast.FragmentSpread:
				if len(s.Directives) != 0 || len(s.Definition.Directives) != 0 {
					return fmt.Errorf("fragment directives are not supported")
				}
				if s.Definition.TypeCondition != typeName {
					return fmt.Errorf("polymorphic fragments are not supported")
				}
				if _, visited := visitedFragments[s.Definition.Name]; visited {
					continue
				}
				visitedFragments[s.Definition.Name] = struct{}{}
				if err := visit(s.Definition.SelectionSet); err != nil {
					return err
				}
			case *ast.InlineFragment:
				if len(s.Directives) != 0 || (s.TypeCondition != "" && s.TypeCondition != typeName) {
					return fmt.Errorf("conditional/polymorphic fragments are not supported")
				}
				if err := visit(s.SelectionSet); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(set); err != nil {
		return nil, err
	}
	return result, nil
}

func directiveKey(directives ast.DirectiveList) string {
	var out strings.Builder
	for _, directive := range directives {
		out.WriteString(directive.Name)
		for _, arg := range directive.Arguments {
			out.WriteString(arg.Name)
			out.WriteString(arg.Value.String())
		}
	}
	return out.String()
}

func (c *Compiler) valueType(typ *ast.Type, selections ast.SelectionSet, depth int) (Type, error) {
	if depth > 32 {
		return Type{}, fmt.Errorf("embedded type nesting exceeds 32")
	}
	result := Type{Nullable: !typ.NonNull}
	if typ.Elem != nil {
		element, err := c.valueType(typ.Elem, selections, depth+1)
		if err != nil {
			return result, err
		}
		result.Kind = "list"
		result.Element = &element
		return result, nil
	}
	if kind := scalarKinds[typ.NamedType]; kind != "" {
		result.Kind = kind
		return result, nil
	}
	def := c.schema.Types[typ.NamedType]
	if def == nil || def.Kind != ast.Object || def.Directives.ForName("table") != nil {
		return result, fmt.Errorf("unsupported value type %s", typ)
	}
	if len(selections) == 0 {
		return result, fmt.Errorf("embedded %s requires selected fields", typ)
	}
	fields, err := collectFields(selections, def.Name)
	if err != nil {
		return result, err
	}
	result.Kind = "object"
	for _, selected := range fields {
		if len(selected.Arguments) != 0 || len(selected.Directives) != 0 {
			return result, fmt.Errorf("embedded fields cannot have arguments or directives")
		}
		field, err := c.mappedField(def, selected.Name)
		if err != nil {
			return result, err
		}
		valueType, err := c.valueType(field.Type, selected.SelectionSet, depth+1)
		if err != nil {
			return result, err
		}
		result.Fields = append(result.Fields, Field{Name: selected.Alias, Path: column(field), Type: valueType})
	}
	return result, nil
}
