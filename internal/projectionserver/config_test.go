package projectionserver

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestProjectionConfigurationDefaultsAndOverrides(t *testing.T) {
	defaults := DefaultOptions()
	if got := (Options{}).withDefaults(); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("zero configuration differs from published defaults: %+v", got)
	}
	custom := defaults
	// Walk only tuning fields, leaving the application-owned providers alone.
	var change func(reflect.Value)
	change = func(v reflect.Value) {
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			switch field.Kind() {
			case reflect.Int, reflect.Int64:
				field.SetInt(field.Int() + 1)
			case reflect.Struct:
				change(field)
			}
		}
	}
	change(reflect.ValueOf(&custom).Elem())
	if got := custom.withDefaults(); !reflect.DeepEqual(got, custom) {
		t.Fatal("default filling replaced explicit tuning values")
	}
	if !reflect.DeepEqual(defaults, DefaultOptions()) {
		t.Fatal("customizing options mutated future defaults")
	}
}

func TestProjectionConstructorUsesConfiguredDependencyMaintenance(t *testing.T) {
	o := DefaultOptions()
	o.Dependencies.IdleTTL = time.Millisecond
	o.Dependencies.SweepInterval = 2 * time.Millisecond
	o.Dependencies.EvictionBatchSize = 3
	o.Dependencies.CleanupBatchSize = 5
	s, err := New(context.Background(), constructorQueries{}, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.(*server).options.Dependencies; got != o.Dependencies {
		t.Fatalf("constructor replaced dependency configuration: %+v", got)
	}
}

func TestProjectionConstructorRejectsInvalidRecoverySchedule(t *testing.T) {
	o := DefaultOptions()
	o.ProjectionLimits.RecoveryInitialDelay = time.Minute
	if _, err := New(t.Context(), constructorQueries{}, o); err == nil {
		t.Fatal("accepted initial recovery delay greater than cap")
	}
}
