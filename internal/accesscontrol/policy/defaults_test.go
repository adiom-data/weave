package policy

import "testing"

func TestLimitsDefaultIndependently(t *testing.T) {
	got, err := (Limits{}).normalized()
	if err != nil || got != DefaultLimits() {
		t.Fatalf("zero limits: %+v, %v", got, err)
	}
	got, err = (Limits{MaxDepth: 7}).normalized()
	if err != nil || got.MaxDepth != 7 || got.MaxNodes != DefaultLimits().MaxNodes {
		t.Fatalf("partial limits: %+v, %v", got, err)
	}
	if _, err := (Limits{MaxDepth: -1}).normalized(); err == nil {
		t.Fatal("negative limit was accepted")
	}
}
