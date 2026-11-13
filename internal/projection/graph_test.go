package projection

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

func TestGraphRootReplacementRequiresCurrentInstalledInstances(t *testing.T) {
	graph := newProjectionGraph()
	node := &ProjectionNode{Name: "root"}
	previous := &projectionInstance{id: "previous", node: node, failure: errors.New("failed")}
	if err := graph.installInstanceLocked(previous, 1); err != nil {
		t.Fatal(err)
	}
	graph.rootID = previous.id
	graph.scheduleRecoveryLocked(previous, time.Now(), DefaultLimits().recoveryPolicy())
	next := &projectionInstance{id: "next", node: node}
	if graph.replaceRootLocked(previous, next) {
		t.Fatal("uninstalled replacement became the root")
	}
	if err := graph.installInstanceLocked(next, 1); err != nil {
		t.Fatal(err)
	}
	stale := &projectionInstance{id: previous.id, node: node}
	if graph.replaceRootLocked(stale, next) || !graph.isRootLocked(previous) || graph.failed[previous.id] != previous {
		t.Fatal("stale predecessor changed root ownership or recovery state")
	}
	if !graph.replaceRootLocked(previous, next) || !graph.isRootLocked(next) || next.replaces != previous.id {
		t.Fatal("installed replacement did not take root ownership")
	}
	if graph.failed[previous.id] != nil || graph.instances[previous.id] != previous || graph.activeSlots != 1 {
		t.Fatal("replacement failed to remove retry eligibility or prematurely retired its predecessor")
	}
	if graph.replaceRootLocked(previous, next) {
		t.Fatal("obsolete predecessor replaced the root twice")
	}
}

func TestGraphRetirementTransfersWorkWithoutClosingSubscriptions(t *testing.T) {
	graph := newProjectionGraph()
	handle := &retirementSubscription{}
	child := &ProjectionNode{Name: "child"}
	instance := &projectionInstance{
		id: "parent", node: &ProjectionNode{Name: "parent"},
		subscription: handle, query: handle,
		members: map[storage.EntityKey]*projectionMember{
			{Value: "row"}: {
				children:         []string{"current"},
				replacementRoots: map[*ProjectionNode][]string{child: {"predecessor"}},
			},
		},
	}
	if err := graph.installInstanceLocked(instance, 1); err != nil {
		t.Fatal(err)
	}
	graph.failed[instance.id] = instance
	retired := graph.retireLocked(instance.id)
	if retired.subscription != handle || !slices.Equal(retired.children, []string{"current", "predecessor"}) {
		t.Fatalf("retirement lost owned work: %+v", retired)
	}
	if handle.closes.Load() != 0 {
		t.Fatal("graph performed subscription IO")
	}
	if graph.activeSlots != 0 || len(graph.instances) != 0 || len(graph.failed) != 0 || instance.subscription != nil || instance.query != nil {
		t.Fatal("retired ownership remains accessible")
	}
	if again := graph.retireLocked(instance.id); again.subscription != nil || len(again.children) != 0 {
		t.Fatal("retirement transferred ownership twice")
	}
	// Runtime owns the transferred handle and can close it outside its locks.
	if err := retired.subscription.Close(t.Context()); err != nil || handle.closes.Load() != 1 {
		t.Fatalf("runtime could not close the transferred subscription: %v", err)
	}
}
