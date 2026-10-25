package policy

import (
	"errors"
)

// compiledExpression hides execution strategy from policy orchestration. Both
// implementations consume canonical input and return policy truth, not CEL values.
// Projection scheduling, fact ownership, and publication never select a backend.
type compiledExpression interface {
	evaluate(*evaluation) (truth, uint64, error)
}

func (s *evaluation) eval(expression compiledExpression) (truth, error) {
	if err := s.step(); err != nil {
		return no, err
	}
	value, cost, err := expression.evaluate(s)
	if s.failure != nil {
		return no, s.failure
	}
	if err != nil {
		return no, err
	}
	if cost > uint64(*s.remaining) {
		return no, errors.New("authorization evaluation work limit exceeded")
	}
	*s.remaining -= int(cost)
	return value, nil
}
