package demo_test

import (
	"os/exec"
	"testing"
)

func TestGeneratedProjectionsAreCurrent(t *testing.T) {
	command := exec.CommandContext(t.Context(), "go", "run", "github.com/adiom-data/weave/cmd/projectiongen",
		"-schema", "schema.graphql", "-documents", "projections.graphql",
		"-out", "packages/demo-projections/src/index.ts", "-react-out", "packages/demo-projections/src/react.ts", "-check")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("projection bindings are stale or generation failed: %v\n%s", err, output)
	}
}
