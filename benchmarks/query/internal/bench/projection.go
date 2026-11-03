package bench

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// WeaveProjection keeps the public client path identical for embedded and
// remote query backends. Only the server's composition differs.
type WeaveProjection struct{ Weave }

func (s *WeaveProjection) Run(ctx context.Context, project int64, observer *Observer) error {
	connection, err := grpc.NewClient(s.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(&s.Weave))
	if err != nil {
		return err
	}
	defer connection.Close()
	stream, err := reactivev1.NewReactiveQueriesClient(connection).Open(ctx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	if err := stream.Send(projectionRequest(project, s.Rows)); err != nil {
		return err
	}
	image := projectionImage{rows: make(map[string]Task), count: s.Rows}
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		switch event := message.Event.(type) {
		case *reactivev1.ServerMessage_Error:
			return fmt.Errorf("projection failed: %s: %s", event.Error.Code, event.Error.Message)
		case *reactivev1.ServerMessage_Ready:
			if image.ready || !image.snapshot || len(image.rows) != s.Rows {
				return fmt.Errorf("unexpected projection readiness")
			}
			rows := make([]Task, 0, len(image.rows))
			for _, row := range image.rows {
				rows = append(rows, row)
			}
			if err := observer.Ready(rows); err != nil {
				return err
			}
			image.ready = true
		case *reactivev1.ServerMessage_ModelBatch:
			changes, err := image.apply(event.ModelBatch)
			if err != nil {
				return err
			}
			if image.ready {
				if err := observer.Apply(changes); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected projection message %T", message.Event)
		}
	}
}

func projectionRequest(project int64, rows int) *reactivev1.ClientMessage {
	field := func(name string) *reactivev1.FieldPath { return &reactivev1.FieldPath{Segments: []string{name}} }
	return &reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "project", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{
		WaitForSync: true,
		Root: &reactivev1.ProjectionNode{NodeId: "tasks", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{
			Query:  &reactivev1.Query{Schema: "bench", Table: "tasks", Columns: []*reactivev1.FieldPath{field("id"), field("project_id"), field("revision"), field("payload")}, Filter: &reactivev1.Predicate{Expression: &reactivev1.Predicate_Comparison{Comparison: &reactivev1.Comparison{Column: field("project_id"), Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.Value{Kind: &reactivev1.Value_IntegerValue{IntegerValue: strconv.FormatInt(project, 10)}}}}}},
			Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_FROM_START, Count: uint32(rows + 1)},
		}}},
	}}}
}

type projectionImage struct {
	rows     map[string]Task
	count    int
	snapshot bool
	ready    bool
}

func (m *projectionImage) apply(batch *reactivev1.ModelBatch) ([]Change, error) {
	if batch == nil {
		return nil, fmt.Errorf("missing projection batch")
	}
	if batch.Kind == reactivev1.ModelBatch_KIND_SNAPSHOT {
		if m.snapshot || m.ready {
			return nil, fmt.Errorf("unexpected projection resnapshot")
		}
		if len(batch.Queries) != 1 || batch.Queries[0].GetReset_() == nil {
			return nil, fmt.Errorf("snapshot must introduce one task query")
		}
		reset := batch.Queries[0].GetReset_()
		if reset.Left.GetSentinel() != reactivev1.Boundary_SENTINEL_START || reset.Right.GetSentinel() != reactivev1.Boundary_SENTINEL_END || len(reset.EntityIds) != m.count {
			return nil, fmt.Errorf("projection must cover the entire filtered project")
		}
		m.snapshot = true
	} else if batch.Kind != reactivev1.ModelBatch_KIND_COMMIT || !m.ready {
		return nil, fmt.Errorf("unexpected projection batch kind %v", batch.Kind)
	} else if len(batch.Queries) != 0 {
		return nil, fmt.Errorf("unexpected task membership change")
	}
	var changes []Change
	for _, entity := range batch.Entities {
		if entity.GetUpsert() == nil {
			return nil, fmt.Errorf("unexpected pending/released task")
		}
		row, err := projectionTask(entity.GetUpsert().Values)
		if err != nil {
			return nil, err
		}
		previous, exists := m.rows[entity.EntityId]
		if batch.Kind == reactivev1.ModelBatch_KIND_SNAPSHOT && exists {
			return nil, fmt.Errorf("duplicate snapshot entity")
		}
		if m.ready && (!exists || previous.ID != row.ID || previous.ProjectID != row.ProjectID) {
			return nil, fmt.Errorf("update changed normalized task identity")
		}
		m.rows[entity.EntityId] = row
		changes = append(changes, Change{Task: row})
	}
	if batch.Kind == reactivev1.ModelBatch_KIND_SNAPSHOT {
		if len(m.rows) != m.count {
			return nil, fmt.Errorf("incomplete projection snapshot")
		}
		seen := make(map[string]bool, m.count)
		for _, id := range batch.Queries[0].GetReset_().EntityIds {
			if _, exists := m.rows[id]; !exists || seen[id] {
				return nil, fmt.Errorf("invalid projection membership reference")
			}
			seen[id] = true
		}
	}
	return changes, nil
}

func projectionTask(values map[string]*reactivev1.Value) (Task, error) {
	var task Task
	for name, target := range map[string]*int64{"id": &task.ID, "project_id": &task.ProjectID, "revision": &task.Revision} {
		value, ok := values[name].GetKind().(*reactivev1.Value_IntegerValue)
		if !ok {
			return task, fmt.Errorf("invalid integer %s", name)
		}
		integer, err := strconv.ParseInt(value.IntegerValue, 10, 64)
		if err != nil {
			return task, err
		}
		*target = integer
	}
	payload, ok := values["payload"].GetKind().(*reactivev1.Value_TextValue)
	if !ok {
		return task, fmt.Errorf("invalid payload")
	}
	task.Payload = payload.TextValue
	return task, nil
}
