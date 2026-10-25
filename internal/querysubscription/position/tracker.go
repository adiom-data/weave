// Package position validates source ordering at private feed publication boundaries.
package position

import (
	"errors"
	"fmt"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// Tracker ignores snapshots/local deliveries: only source transactions and
// progress close source history. State belongs to one feed receiver.
type Tracker struct{ last map[storage.SourceID]uint64 }

func (t *Tracker) Observe(p *querysubscription.Position) error { return t.observe(p, false) }

// Requested reports may repeat the latest position but never regress.
func (t *Tracker) ObserveReport(p *querysubscription.Position) error { return t.observe(p, true) }

func (t *Tracker) observe(p *querysubscription.Position, equal bool) error {
	if p == nil || p.SourceID == "" {
		return errors.New("source position is required")
	}
	if t.last == nil {
		t.last = make(map[storage.SourceID]uint64)
	}
	if previous, ok := t.last[p.SourceID]; ok && (p.Value < previous || (!equal && p.Value == previous)) {
		return fmt.Errorf("source position %d does not advance past %d", p.Value, previous)
	}
	t.last[p.SourceID] = p.Value
	return nil
}
