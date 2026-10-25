package querygrpc

import (
	"errors"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	pb "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

func entityTargetToProto(target q.EntityTarget) (*pb.EntityTarget, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	result := &pb.EntityTarget{SubscriptionId: target.SubscriptionID}
	if route := target.Reference; route != nil {
		result.Reference = &pb.ReferenceRoute{Selector: route.Selector}
		for _, key := range route.Parents {
			result.Reference.Parents = append(result.Reference.Parents, entityKeyToProto(key))
		}
	}
	return result, nil
}

func entityTargetFromProto(target *pb.EntityTarget) (q.EntityTarget, error) {
	if target == nil {
		return q.EntityTarget{}, errors.New("entity target is required")
	}
	result := q.EntityTarget{SubscriptionID: target.SubscriptionId}
	if route := target.Reference; route != nil {
		result.Reference = &q.ReferenceRoute{Selector: route.Selector}
		for _, key := range route.Parents {
			parent, err := entityKeyFromProto(key)
			if err != nil {
				return q.EntityTarget{}, err
			}
			result.Reference.Parents = append(result.Reference.Parents, parent)
		}
	}
	return result, result.Validate()
}

func entityInstallationToProto(entity q.EntityInstallation) (*pb.EntityInstallation, error) {
	target, err := entityTargetToProto(entity.Target)
	if err != nil {
		return nil, err
	}
	snapshot, err := entitySnapshotToProto(entity.Snapshot)
	if err != nil {
		return nil, err
	}
	return &pb.EntityInstallation{Target: target, Snapshot: snapshot}, nil
}

func entityInstallationFromProto(entity *pb.EntityInstallation) (q.EntityInstallation, error) {
	if entity == nil {
		return q.EntityInstallation{}, errors.New("entity installation is required")
	}
	target, err := entityTargetFromProto(entity.Target)
	if err != nil {
		return q.EntityInstallation{}, err
	}
	snapshot, err := entitySnapshotFromProto(entity.Snapshot)
	return q.EntityInstallation{Target: target, Snapshot: snapshot}, err
}

func entityEffectToProto(entity q.EntityEffect) (*pb.EntityEffect, error) {
	target, err := entityTargetToProto(entity.Target)
	if err != nil {
		return nil, err
	}
	delta, err := batchToProto(q.Batch{Clear: entity.Delta.Clear, ClearChangeIndex: entity.Delta.ClearChangeIndex, Changes: entity.Delta.Changes})
	if err != nil {
		return nil, err
	}
	return &pb.EntityEffect{Target: target, Delta: &pb.EntityDelta{Clear: delta.Clear, ClearChangeIndex: delta.ClearChangeIndex, Changes: delta.Changes}}, nil
}

func entityEffectFromProto(entity *pb.EntityEffect) (q.EntityEffect, error) {
	if entity == nil || entity.Delta == nil {
		return q.EntityEffect{}, errors.New("entity effect and delta are required")
	}
	target, err := entityTargetFromProto(entity.Target)
	if err != nil {
		return q.EntityEffect{}, err
	}
	delta, err := batchFromProto(&pb.Batch{Clear: entity.Delta.Clear, ClearChangeIndex: entity.Delta.ClearChangeIndex, Changes: entity.Delta.Changes})
	return q.EntityEffect{Target: target, Delta: q.EntityDelta{Clear: delta.Clear, ClearChangeIndex: delta.ClearChangeIndex, Changes: delta.Changes}}, err
}
