package server

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// Walk library-owned scalar options, not a hand-picked list of settings. New
// tuning fields must be exposed and forwarded or deliberately excluded here.
func assertTuning(t *testing.T, config, options any, path string, excluded map[string]bool) {
	t.Helper()
	c, o := reflect.ValueOf(config), reflect.ValueOf(options)
	for i := 0; i < o.NumField(); i++ {
		name := o.Type().Field(i).Name
		key := path + "." + name
		if excluded[key] {
			continue
		}
		value := o.Field(i)
		if value.Kind() != reflect.Struct && value.Kind() != reflect.Int && value.Kind() != reflect.Int64 {
			continue // Providers, callbacks, loggers, and identity are not tuning.
		}
		field := c.FieldByName(name)
		if !field.IsValid() {
			t.Errorf("%s: library setting is not exposed", key)
			continue
		}
		if value.Kind() == reflect.Struct {
			assertTuning(t, field.Interface(), value.Interface(), key, excluded)
		} else if field.Int() != value.Int() {
			t.Errorf("%s: config %d != component %d", key, field.Int(), value.Int())
		}
	}
}

func changeTuning(v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Struct:
			changeTuning(f)
		case reflect.Int, reflect.Int64:
			f.SetInt(f.Int() + int64(i+1))
		}
	}
}

func TestAllComponentTuningIsExposedAndForwarded(t *testing.T) {
	for _, changed := range []bool{false, true} {
		c := DefaultConfig()
		if changed {
			changeTuning(reflect.ValueOf(&c.Query).Elem())
			changeTuning(reflect.ValueOf(&c.Projection).Elem())
			changeTuning(reflect.ValueOf(&c.Visibility).Elem())
			changeTuning(reflect.ValueOf(&c.Storage.Postgres).Elem())
		}
		pg, q, p, ac := postgresOptions(c), queryOptions(c), configuredProjectionOptions(c), configuredPolicyOptions(c)
		assertTuning(t, c.Storage.Postgres, pg, "postgres", nil)
		assertTuning(t, c.Query, q, "query", map[string]bool{"query.Transport": true})
		assertTuning(t, c.Projection, p, "projection", nil)
		assertTuning(t, c.Visibility, ac, "visibility", nil)
		if !changed {
			assertTuning(t, pg, storage.DefaultPostgresOptions(), "postgres", nil)
			assertTuning(t, q, queryserver.DefaultOptions(), "query", map[string]bool{"query.Transport": true})
			assertTuning(t, p, projectionserver.DefaultOptions(), "projection", nil)
			assertTuning(t, ac, projectionserver.DefaultPolicyOptions(), "visibility", nil)
		}
	}
}
