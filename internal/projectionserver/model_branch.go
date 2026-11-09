package projectionserver

import (
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/protobuf/proto"
)

type modelBranchKey struct {
	scope, instance string
	key             storage.EntityKey
}

func (m *connectionModel) applyBranch(batch *pb.ModelBatch, scope string, event proj.ProjectionEvent) {
	key := modelBranchKey{scope, event.InstanceID, event.Key}
	old := m.branches[key]
	if event.Failure == nil {
		if old != nil {
			batch.Branches = append(batch.Branches, recoveredBranch(old))
			delete(m.branches, key)
		}
		return
	}
	if old != nil {
		return
	}
	branch := &pb.ProjectionBranch{SubscriptionId: scope, OccurrenceId: event.InstanceID, NodeId: event.Node, ParentOccurrenceId: event.ParentInstanceID}
	if event.ParentEntityKey != (storage.EntityKey{}) {
		branch.ParentEntityId = publicEntityID(event.ParentEntityKey)
	}
	code, message := "QUERY_FAILED", "projection branch could not be loaded"
	if event.Key != (storage.EntityKey{}) {
		branch.EntityId = publicEntityID(event.Key)
		code, message = "EVALUATION_FAILED", "projection branch could not be evaluated"
	}
	// Evaluation failures may mention private facts or row values. Public errors
	// identify the affected branch, never echo arbitrary adapter/CEL error text.
	branch.State = &pb.ProjectionBranch_Error{Error: &pb.ProjectionBranchError{Code: code, Message: message}}
	m.branches[key] = branch
	batch.Branches = append(batch.Branches, branch)
}

func recoveredBranch(old *pb.ProjectionBranch) *pb.ProjectionBranch {
	branch := proto.Clone(old).(*pb.ProjectionBranch)
	branch.State = &pb.ProjectionBranch_Recovered{Recovered: &pb.ProjectionBranchRecovered{}}
	return branch
}

func (m *connectionModel) closeBranches(batch *pb.ModelBatch, scope, instance string) {
	for key, branch := range m.branches {
		if key.scope == scope && (instance == "" || key.instance == instance) {
			batch.Branches = append(batch.Branches, recoveredBranch(branch))
			delete(m.branches, key)
		}
	}
}

// Current-image reconciliation replaces the complete set of branch errors as
// well as live contents. A formerly errored branch can have disappeared entirely.
func (m *connectionModel) replaceBranches(batch *pb.ModelBatch, scope string, events []proj.ProjectionEvent) {
	desired := map[modelBranchKey]bool{}
	for _, event := range events {
		if event.Type != proj.ProjectionBranchChanged || event.Failure == nil {
			continue
		}
		desired[modelBranchKey{scope, event.InstanceID, event.Key}] = true
		m.applyBranch(batch, scope, event)
	}
	for key, old := range m.branches {
		if key.scope == scope && !desired[key] {
			batch.Branches = append(batch.Branches, recoveredBranch(old))
			delete(m.branches, key)
		}
	}
}
