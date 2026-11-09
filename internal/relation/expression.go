package relation

import (
	"errors"

	"github.com/adiom-data/weave/internal/storage"
)

// compiledExpression hides execution strategy from relation orchestration. Both
// implementations consume and return canonical values, not CEL values.
// Projection scheduling, fact ownership, and publication never select a backend.
type compiledExpression interface {
	evaluate(*evaluation) (storage.Value, uint64, error)
}

func (s *evaluation) eval(expression compiledExpression) (truth, error) {
	value, err := s.value(expression)
	if err != nil {
		return no, err
	}
	if value.State == storage.ValueUnavailable {
		return unknown, nil
	}
	if value.State != storage.ValuePresent || value.Type != storage.TypeBool {
		return no, errors.New("visibility expression did not return bool")
	}
	if string(value.Data) == "true" {
		return yes, nil
	}
	return no, nil
}

func (s *evaluation) value(expression compiledExpression) (storage.Value, error) {
	if err := s.step(); err != nil {
		return storage.Value{}, err
	}
	value, cost, err := expression.evaluate(s)
	if s.failure != nil {
		return storage.Value{}, s.failure
	}
	if err != nil {
		return storage.Value{}, err
	}
	if cost > uint64(*s.remaining) {
		return storage.Value{}, errors.New("relation evaluation work limit exceeded")
	}
	*s.remaining -= int(cost)
	return value, nil
}
