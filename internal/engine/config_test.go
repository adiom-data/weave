package engine

import (
	"reflect"
	"testing"
)

func TestCoordinatorConfigurationDefaultsAndOverrides(t *testing.T) {
	defaults := DefaultCoordinatorOptions()
	if got := (CoordinatorOptions{}).withDefaults(); got != defaults {
		t.Fatalf("zero configuration = %+v, want %+v", got, defaults)
	}
	// Every current/future numeric tuning field must survive default filling.
	custom := defaults
	fields := reflect.ValueOf(&custom).Elem()
	for i := 0; i < fields.NumField(); i++ {
		fields.Field(i).SetInt(fields.Field(i).Int() + 1)
	}
	if got := custom.withDefaults(); got != custom {
		t.Fatalf("custom configuration changed: got %+v, want %+v", got, custom)
	}
	if defaults.ProgressBarrierDelay != 0 {
		t.Fatal("zero progress delay must retain its immediate-progress semantics")
	}
	if got := (CoordinatorOptions{QueryIdleTTL: -1}).withDefaults(); got.QueryIdleTTL != -1 {
		t.Fatal("invalid explicit value was hidden by defaults")
	}
}
