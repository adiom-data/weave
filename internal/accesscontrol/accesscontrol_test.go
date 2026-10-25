package accesscontrol

import (
	"context"
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestProjectionPathsCaptureImagesAndShareParents(t *testing.T) {
	record := storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("original")}}
	root := NewProjectionPath(nil, ProjectionStep{Node: "project", Record: record, Complete: true})
	left := NewProjectionPath(root, ProjectionStep{Node: "left", Complete: true})
	right := NewProjectionPath(root, ProjectionStep{Node: "right", Complete: true})
	if left.Parent() != root || right.Parent() != root || root.Len() != 1 || left.Len() != 2 {
		t.Fatal("paths did not share their captured parent")
	}
	record["title"].Data[0] = 'X'
	if string(root.Step().Record["title"].Data) != "original" {
		t.Fatal("captured context changed with its input")
	}
	var nodes []string
	for step := range left.Steps() {
		nodes = append(nodes, step.Node)
	}
	if len(nodes) != 2 || nodes[0] != "project" || nodes[1] != "left" {
		t.Fatalf("root-first steps=%v", nodes)
	}
	visits := 0
	for range left.Steps() {
		visits++
		break
	}
	if visits != 1 {
		t.Fatal("iteration ignored early termination")
	}
	var empty *ProjectionPath
	for range empty.Steps() {
		t.Fatal("nil path yielded a step")
	}
}

type testPosition int

func (testPosition) StorageCheckpoint()       {}
func (testPosition) StorageFence()            {}
func (testPosition) Source() storage.SourceID { return "test" }
func (p testPosition) StableID() string       { return fmt.Sprint(int(p)) }
func (p testPosition) Order() uint64          { return uint64(p) }

func TestAllowAllUsesStableViewsAndPreparedLifecycle(t *testing.T) {
	controller := NewAllowAll()
	prepared, err := controller.Prepare(context.Background(), PrepareRequest{ID: "viewer"})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := controller.Synchronize(context.Background(), prepared.ID(), testPosition(7))
	if err != nil {
		t.Fatal(err)
	}
	visible, err := prepared.Evaluate(context.Background(), checkpoint.View, []Candidate{{}, {}})
	if err != nil || len(visible) != 2 || !visible[0] || !visible[1] {
		t.Fatalf("visibility = %v, err=%v", visible, err)
	}
	if err := checkpoint.View.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), PrepareRequest{ID: "viewer"}); err != nil {
		t.Fatalf("released prepared ID was not reusable: %v", err)
	}
}
