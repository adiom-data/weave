package projectionserver

import (
	"reflect"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type entityOutput uint8

const (
	entityUnpublished entityOutput = iota
	entityPending
	entityComplete
)

// modelEntity separates accepted source state from its last public image.
// Selection changes only render the accepted state; they never hydrate it.
type modelEntity struct {
	version      querysubscription.RecordVersion
	hasImage     bool
	pending      bool
	record       storage.Record // nil while pending; never a stale hydration fallback
	output       entityOutput
	outputRecord storage.Record
}

func (e *modelEntity) accept(version querysubscription.RecordVersion, record storage.Record, pending bool) {
	if e.hasImage {
		older := version.Revision < e.version.Revision || (version.Revision == e.version.Revision && version.ChangeIndex < e.version.ChangeIndex)
		if older {
			return
		}
		if version == e.version && (!e.pending || pending) {
			return
		}
	}
	e.hasImage = true
	e.version = version
	e.pending = pending
	if pending {
		e.record = nil
	} else {
		e.record = storage.CloneRecord(record)
	}
}

type entityPublication struct {
	event  *reactivev1.EntityEvent
	output entityOutput
	record storage.Record
}

// prepare runs once per dirty entity at the outer publication boundary. It is
// pure: conversion must succeed for the complete batch before output is marked
// published. Equality suppresses output only when completeness is unchanged.
// selectFields returns a private outer map with read-only, borrowed values.
func (e *modelEntity) prepare(id string, selectFields func(storage.Record) storage.Record) (*entityPublication, error) {
	if !e.hasImage {
		return nil, nil
	}
	if e.pending {
		if e.output != entityPending {
			return &entityPublication{event: &reactivev1.EntityEvent{EntityId: id, Event: &reactivev1.EntityEvent_Pending{Pending: &reactivev1.EntityPending{}}}, output: entityPending}, nil
		}
		return nil, nil
	}
	projected := selectFields(e.record)
	if e.output == entityComplete && reflect.DeepEqual(e.outputRecord, projected) {
		return nil, nil
	}
	// Freeze only an image that will actually be published. Unselected changes
	// need no deep copies, while retained public output stays independently owned.
	for name, value := range projected {
		projected[name] = storage.CloneValue(value)
	}
	values, err := protoAllValues(projected)
	if err != nil {
		return nil, err
	}
	return &entityPublication{event: &reactivev1.EntityEvent{EntityId: id, Event: &reactivev1.EntityEvent_Upsert{Upsert: &reactivev1.EntityUpsert{Values: values}}}, output: entityComplete, record: projected}, nil
}
