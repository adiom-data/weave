package projectionserver

import (
	"reflect"

	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func (m *connectionModel) acceptComputed(batch *pb.ModelBatch, occurrence, entity string, values storage.Record) error {
	if len(values) == 0 {
		m.releaseComputed(batch, occurrence, entity)
		return nil
	}
	if reflect.DeepEqual(m.computed[occurrence][entity], values) {
		return nil
	}
	wire, err := protoAllValues(values)
	if err != nil {
		return err
	}
	if m.computed[occurrence] == nil {
		m.computed[occurrence] = map[string]storage.Record{}
	}
	m.computed[occurrence][entity] = storage.CloneRecord(values)
	batch.Computed = append(batch.Computed, &pb.ComputedEvent{OccurrenceId: occurrence, EntityId: entity, Event: &pb.ComputedEvent_Upsert{Upsert: &pb.ValueObject{Fields: wire}}})
	return nil
}
func (m *connectionModel) releaseComputed(batch *pb.ModelBatch, occurrence, entity string) {
	if _, ok := m.computed[occurrence][entity]; !ok {
		return
	}
	delete(m.computed[occurrence], entity)
	if len(m.computed[occurrence]) == 0 {
		delete(m.computed, occurrence)
	}
	batch.Computed = append(batch.Computed, &pb.ComputedEvent{OccurrenceId: occurrence, EntityId: entity, Event: &pb.ComputedEvent_Released{Released: &pb.ComputedReleased{}}})
}
func (m *connectionModel) closeComputed(batch *pb.ModelBatch, occurrence string) {
	for entity := range m.computed[occurrence] {
		m.releaseComputed(batch, occurrence, entity)
	}
}
