package projection

import (
	"context"
	"errors"
	"testing"
)

func TestRecoveryCapacityIsReleasedOnlyAfterBothInstancesClose(t *testing.T) {
	for _, replacementFirst := range []bool{false, true} {
		name := "predecessor-first"
		if replacementFirst {
			name = "replacement-first"
		}
		t.Run(name, func(t *testing.T) {
			node := recoveryNode("root")
			p := &Projection{ctx: context.Background(), limits: Limits{MaxActiveInstances: 1}, graph: projectionGraph{instances: map[string]*projectionInstance{}}}
			old := &projectionInstance{id: "old", node: node, failure: errors.New("failed")}
			if err := p.installInstanceLocked(old); err != nil {
				t.Fatal(err)
			}
			p.graph.rootID = old.id
			if err := p.installInstanceLocked(&projectionInstance{id: old.id, node: node}); err == nil {
				t.Fatal("duplicate installation reused an existing ID")
			}
			next := &projectionInstance{id: "next", node: node}
			if err := p.installInstanceLocked(next); err != nil {
				t.Fatal("replacement did not reuse failed slot:", err)
			}
			if p.graph.activeSlots != 1 || old.slot != next.slot || old.slot.references != 2 {
				t.Fatal("replacement capacity was not shared")
			}
			// The credit is for one replacement, not unlimited concurrent loads.
			if err := p.installInstanceLocked(&projectionInstance{id: "duplicate", node: node}); err == nil {
				t.Fatal("failed slot admitted a second simultaneous replacement")
			}
			first, last := old, next
			if replacementFirst {
				first, last = next, old
			}
			p.closeInstance(first.id)
			if p.graph.activeSlots != 1 || last.slot.references != 1 {
				t.Fatal("closing one instance released its survivor's capacity")
			}
			if err := p.installInstanceLocked(&projectionInstance{id: "unrelated", node: recoveryNode("other")}); err == nil {
				t.Fatal("unrelated expansion borrowed recovery capacity")
			}
			p.closeInstance(last.id)
			p.closeInstance(first.id) // Idempotent closure must not double release.
			if p.graph.activeSlots != 0 || len(p.graph.instances) != 0 {
				t.Fatal("replacement retirement leaked capacity")
			}
			if err := p.installInstanceLocked(&projectionInstance{id: "fresh", node: recoveryNode("other")}); err != nil {
				t.Fatal("released capacity was not reusable:", err)
			}
			p.graph.rootID = "fresh"
			if err := p.installInstanceLocked(&projectionInstance{id: "healthy-replacement", node: p.graph.instances["fresh"].node}); err == nil {
				t.Fatal("a healthy instance received failed-placeholder capacity credit")
			}
			p.closeInstance("fresh")
		})
	}
}
