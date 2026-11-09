package relationfile

import (
	"errors"
	"fmt"
	"os"

	"github.com/adiom-data/weave/internal/server/config"
	relationdef "github.com/adiom-data/weave/pkg/relation"
	"github.com/adiom-data/weave/pkg/storage"
	"go.yaml.in/yaml/v3"
)

// These are startup-file DTOs, not a second definition/query representation used
// by the engine. Lowering produces the same public definitions as Go callers.
type relationConfig struct {
	Schema string `yaml:"schema"`
	Table  string `yaml:"table"`
}

func (r relationConfig) name() (storage.RelationName, error) {
	if r.Schema == "" || r.Table == "" {
		return storage.RelationName{}, errors.New("relation requires schema and table")
	}
	return storage.RelationName{Namespace: r.Schema, Name: r.Table}, nil
}

type receivingConfig struct {
	From  relationConfig `yaml:"from"`
	Field string         `yaml:"field"`
}
type propagationConfig struct {
	To   relationConfig `yaml:"to"`
	When string         `yaml:"when"`
}
type computedConfig struct {
	Type       storage.ValueType   `yaml:"type"`
	Expression string              `yaml:"expression"`
	Propagate  []propagationConfig `yaml:"propagate"`
}
type relationFile struct {
	Relations []struct {
		relationConfig `yaml:",inline"`
		Visibility     string                    `yaml:"visibility"`
		Receive        []receivingConfig         `yaml:"receive"`
		Computed       map[string]computedConfig `yaml:"computed"`
		Facts          []struct {
			ID         string         `yaml:"id"`
			Projection factProjection `yaml:"projection"`
		} `yaml:"facts"`
	} `yaml:"relations"`
}

type factProjection struct {
	Name     string         `yaml:"name"`
	Kind     string         `yaml:"kind"`
	Relation relationConfig `yaml:"relation"`
	Filter   *filterConfig  `yaml:"filter"`
	Order    []struct {
		Field      []string `yaml:"field"`
		Descending bool     `yaml:"descending"`
		NullsFirst bool     `yaml:"nulls_first"`
	} `yaml:"order"`
	Bindings []struct {
		Field     []string       `yaml:"field"`
		Operator  string         `yaml:"operator"`
		Parameter []string       `yaml:"parameter"`
		Parent    []string       `yaml:"parent"`
		Literal   *literalConfig `yaml:"literal"`
	} `yaml:"bindings"`
	Children []factProjection `yaml:"children"`
}
type filterConfig struct {
	And      []*filterConfig `yaml:"and"`
	Or       []*filterConfig `yaml:"or"`
	Field    []string        `yaml:"field"`
	Operator string          `yaml:"operator"`
	Value    *literalConfig  `yaml:"value"`
}
type literalConfig struct {
	Type  storage.ValueType `yaml:"type"`
	Value yaml.Node         `yaml:"value"`
}

func Load(path string, limits relationdef.Limits) (*relationdef.Store, error) {
	definitions, err := loadDefinitions(path)
	if err != nil {
		return nil, err
	}
	return relationdef.NewStore(definitions, limits)
}

func loadDefinitions(path string) (relationdef.Definitions, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read relation definitions: %w", err)
	}
	var file relationFile
	if err := config.DecodeYAML(data, &file); err != nil {
		return nil, fmt.Errorf("decode relation definitions: %w", err)
	}
	definitions := relationdef.Definitions{}
	for _, r := range file.Relations {
		name, err := r.name()
		if err != nil {
			return nil, err
		}
		if _, ok := definitions[name]; ok {
			return nil, fmt.Errorf("duplicate relation definition %s.%s", name.Namespace, name.Name)
		}
		p := relationdef.Relation{Computed: map[string]relationdef.Computed{}, Visibility: r.Visibility}
		p.Receive, err = receiving(r.Receive)
		if err != nil {
			return nil, err
		}
		for name, c := range r.Computed {
			value := relationdef.Computed{Type: c.Type, Expression: c.Expression}
			for _, edge := range c.Propagate {
				to, err := edge.To.name()
				if err != nil {
					return nil, err
				}
				value.Propagate = append(value.Propagate, relationdef.Propagation{To: to, When: edge.When})
			}
			p.Computed[name] = value
		}
		for _, fact := range r.Facts {
			projection, err := fact.Projection.lower()
			if err != nil {
				return nil, fmt.Errorf("fact %q: %w", fact.ID, err)
			}
			p.Facts = append(p.Facts, relationdef.Fact{ID: fact.ID, Projection: projection})
		}
		definitions[name] = p
	}
	return definitions, nil
}

func receiving(configured []receivingConfig) ([]relationdef.Incoming, error) {
	if configured == nil {
		return nil, nil
	}
	result := make([]relationdef.Incoming, 0, len(configured))
	for _, edge := range configured {
		from, err := edge.From.name()
		if err != nil {
			return nil, err
		}
		result = append(result, relationdef.Incoming{Field: edge.Field, From: from})
	}
	return result, nil
}

func (p factProjection) lower() (*relationdef.Projection, error) {
	relation, err := p.Relation.name()
	if err != nil {
		return nil, err
	}
	if p.Kind != "list" && p.Kind != "entity" {
		return nil, errors.New("fact projection kind must be list or entity")
	}
	filter, err := p.Filter.lower()
	if err != nil {
		return nil, err
	}
	n := &relationdef.Projection{Name: p.Name, Entity: p.Kind == "entity", Query: storage.QuerySpec{Relation: relation, Filter: filter}}
	for _, order := range p.Order {
		field, err := storage.NewFieldPath(order.Field...)
		if err != nil {
			return nil, err
		}
		n.Query.Order = append(n.Query.Order, storage.OrderTerm{Field: field, Descending: order.Descending, NullsFirst: order.NullsFirst})
	}
	for _, source := range p.Bindings {
		field, err := storage.NewFieldPath(source.Field...)
		if err != nil {
			return nil, err
		}
		op, err := comparisonOperator(source.Operator)
		if err != nil {
			return nil, err
		}
		b := relationdef.Binding{Field: field, Operator: op}
		if source.Parameter != nil {
			b.Parameter, err = storage.NewFieldPath(source.Parameter...)
			if err != nil {
				return nil, err
			}
		}
		if source.Parent != nil {
			b.Parent, err = storage.NewFieldPath(source.Parent...)
			if err != nil {
				return nil, err
			}
		}
		if source.Literal != nil {
			v, err := source.Literal.lower()
			if err != nil {
				return nil, err
			}
			b.Literal = &v
		}
		n.Bindings = append(n.Bindings, b)
	}
	for _, child := range p.Children {
		nested, err := child.lower()
		if err != nil {
			return nil, err
		}
		n.Children = append(n.Children, nested)
	}
	return n, nil
}

func (f *filterConfig) lower() (*storage.Predicate, error) {
	if f == nil {
		return nil, nil
	}
	choices := 0
	if f.And != nil {
		choices++
	}
	if f.Or != nil {
		choices++
	}
	if f.Field != nil {
		choices++
	}
	if choices != 1 {
		return nil, errors.New("filter requires exactly one of and, or, field")
	}
	if f.And != nil || f.Or != nil {
		if f.Operator != "" || f.Value != nil {
			return nil, errors.New("junction cannot have operator or value")
		}
		op, children := storage.JunctionAnd, f.And
		if f.Or != nil {
			op, children = storage.JunctionOr, f.Or
		}
		if len(children) == 0 {
			return nil, errors.New("empty filter junction")
		}
		j := &storage.Junction{Operator: op}
		for _, child := range children {
			if child == nil {
				return nil, errors.New("null filter operand")
			}
			p, err := child.lower()
			if err != nil {
				return nil, err
			}
			j.Operands = append(j.Operands, p)
		}
		return &storage.Predicate{Junction: j}, nil
	}
	field, err := storage.NewFieldPath(f.Field...)
	if err != nil {
		return nil, err
	}
	if f.Operator == "is_null" || f.Operator == "is_not_null" {
		if f.Value != nil {
			return nil, errors.New("null test does not take a value")
		}
		return &storage.Predicate{Null: &storage.NullTest{Field: field, IsNull: f.Operator == "is_null"}}, nil
	}
	op, err := comparisonOperator(f.Operator)
	if err != nil {
		return nil, err
	}
	if f.Value == nil {
		return nil, errors.New("comparison requires typed value")
	}
	v, err := f.Value.lower()
	if err != nil {
		return nil, err
	}
	literal, err := storage.LiteralFromValue(v)
	if err != nil {
		return nil, err
	}
	return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: op, Value: literal}}, nil
}

func comparisonOperator(op string) (storage.ComparisonOperator, error) {
	switch op {
	case "eq":
		return storage.ComparisonEqual, nil
	case "ne":
		return storage.ComparisonNotEqual, nil
	case "lt":
		return storage.ComparisonLess, nil
	case "lte":
		return storage.ComparisonLessOrEqual, nil
	case "gt":
		return storage.ComparisonGreater, nil
	case "gte":
		return storage.ComparisonGreaterOrEqual, nil
	default:
		return 0, fmt.Errorf("unknown comparison operator %q", op)
	}
}
func (l literalConfig) lower() (storage.Value, error) {
	if l.Value.Kind != yaml.ScalarNode {
		return storage.Value{}, errors.New("query literal value must be a scalar")
	}
	// Explicit types eliminate ambiguity for UUID/bytes/datetime and ensure the
	// same canonicalization as all other callers, without datastore-specific SQL.
	return storage.ParseScalar(l.Type, []byte(l.Value.Value))
}
