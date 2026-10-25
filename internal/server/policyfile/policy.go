package policyfile

import (
	"errors"
	"fmt"
	"os"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
	"github.com/adiom-data/weave/pkg/storage"
	"go.yaml.in/yaml/v3"
)

// These are startup-file DTOs, not a second policy/query representation used
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

type ruleConfig struct {
	ID   string `yaml:"id"`
	When string `yaml:"when"`
}
type policyFile struct {
	Relations []struct {
		relationConfig `yaml:",inline"`
		Allow          []ruleConfig `yaml:"allow"`
		Deny           []ruleConfig `yaml:"deny"`
		Propagate      []struct {
			ID   string         `yaml:"id"`
			To   relationConfig `yaml:"to"`
			When string         `yaml:"when"`
		} `yaml:"propagate"`
		Receive []struct {
			From relationConfig `yaml:"from"`
			ID   string         `yaml:"id"`
		} `yaml:"receive"`
		Facts []struct {
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

func Load(path string, limits policy.Limits) (policy.Provider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policies: %w", err)
	}
	var file policyFile
	if err := config.DecodeYAML(data, &file); err != nil {
		return nil, fmt.Errorf("decode policies: %w", err)
	}
	definitions := policy.Definition{}
	for _, r := range file.Relations {
		name, err := r.name()
		if err != nil {
			return nil, err
		}
		if _, ok := definitions[name]; ok {
			return nil, fmt.Errorf("duplicate policy relation %s.%s", name.Namespace, name.Name)
		}
		p := policy.Relation{}
		for _, rule := range r.Allow {
			p.Allow = append(p.Allow, policy.Rule{ID: rule.ID, When: rule.When})
		}
		for _, rule := range r.Deny {
			p.Deny = append(p.Deny, policy.Rule{ID: rule.ID, When: rule.When})
		}
		for _, edge := range r.Propagate {
			to, err := edge.To.name()
			if err != nil {
				return nil, err
			}
			p.Propagate = append(p.Propagate, policy.Edge{ID: edge.ID, To: to, When: edge.When})
		}
		if r.Receive != nil {
			p.Receive = make([]policy.EdgeReference, 0, len(r.Receive))
		}
		for _, edge := range r.Receive {
			from, err := edge.From.name()
			if err != nil {
				return nil, err
			}
			p.Receive = append(p.Receive, policy.EdgeReference{ID: edge.ID, From: from})
		}
		for _, fact := range r.Facts {
			projection, err := fact.Projection.lower()
			if err != nil {
				return nil, fmt.Errorf("fact %q: %w", fact.ID, err)
			}
			p.Facts = append(p.Facts, policy.Fact{ID: fact.ID, Projection: projection})
		}
		definitions[name] = p
	}
	return policy.NewStaticProvider(definitions, limits)
}

func (p factProjection) lower() (*policy.Projection, error) {
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
	n := &policy.Projection{Name: p.Name, Entity: p.Kind == "entity", Query: storage.QuerySpec{Relation: relation, Filter: filter}}
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
		b := policy.Binding{Field: field, Operator: op}
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
