package config

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestTuningYAMLRoundTrip(t *testing.T) {
	c := validConfig()
	c.Query.Limits.EntityIdleTTL = 7 * time.Minute
	c.Query.FenceWaitTimeout = 45 * time.Second
	c.Storage.Postgres.Replication.StatusInterval = 3 * time.Second
	c.Projection.Dependencies.CleanupBatchSize = 27
	c.Relations.Limits.MaxNodes = 1234
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var loaded Config
	if err := DecodeYAML(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(loaded)
	if err != nil || !bytes.Equal(raw, encoded) {
		t.Fatal("YAML round trip changed configuration", err)
	}
	if err := loaded.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEveryNumericTuningRejectsInvalidValues(t *testing.T) {
	var walk func([]int, reflect.Type, string)
	walk = func(indices []int, typ reflect.Type, path string) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			index := append(append([]int(nil), indices...), i)
			name := strings.TrimPrefix(path+"."+field.Tag.Get("yaml"), ".")
			if name == "auth" { // Authentication has its own mode-specific tests.
				continue
			}
			if field.Type.Kind() == reflect.Struct {
				walk(index, field.Type, name)
			} else if field.Type.Kind() == reflect.Int || field.Type.Kind() == reflect.Int64 {
				for _, invalid := range []int64{-1, 0} {
					t.Run(name+"/"+time.Duration(invalid).String(), func(t *testing.T) {
						c := validConfig()
						reflect.ValueOf(&c).Elem().FieldByIndex(index).SetInt(invalid)
						err := c.Validate()
						if name == "shutdown_timeout" || name == "read_header_timeout" || name == "idle_timeout" {
							err = c.ValidateListener()
						}
						if name == "query.limits.progress_barrier_delay" && invalid == 0 {
							if err != nil {
								t.Fatal(err)
							}
						} else if err == nil {
							t.Fatal("accepted invalid tuning")
						}
					})
				}
			}
		}
	}
	walk(nil, reflect.TypeOf(Config{}), "")
}

func TestTuningCrossFieldValidation(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"replication retry": func(c *Config) { c.Storage.Postgres.Replication.RetryMinDelay = time.Hour },
		"projection retry":  func(c *Config) { c.Projection.ProjectionLimits.RecoveryInitialDelay = time.Hour },
		"message queue":     func(c *Config) { c.Query.Messages.MaxQueuedBytes = 1 },
		"message framing":   func(c *Config) { c.Query.Messages.MaxBatchBytes = 1 },
		"capture framing":   func(c *Config) { c.Storage.Postgres.MessageLimits.MaxTransactionBytes = 1 },
		"fact window": func(c *Config) {
			c.Visibility.Mode = "definitions"
			c.Relations.File = "relations.yaml"
			c.Relations.MaxFactRows = c.Query.Limits.MaxWindowRows
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := validConfig()
			mutate(&c)
			if c.Validate() == nil {
				t.Fatal("accepted inconsistent tuning")
			}
		})
	}
}

func TestFactCoverageLimitsFollowRelationEvaluation(t *testing.T) {
	for _, test := range []struct {
		name, mode, file     string
		factRows, windowRows int
		invalid              bool
	}{
		{"visibility facts", "definitions", "relations.yaml", 20, 20, true},
		{"computed facts without visibility", "allow_all", "relations.yaml", 20, 20, true},
		{"computed facts with probe room", "allow_all", "relations.yaml", 19, 20, false},
		{"no relation evaluator", "allow_all", "", 20, 20, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := validConfig()
			c.Visibility.Mode, c.Relations.File = test.mode, test.file
			c.Relations.MaxFactRows, c.Query.Limits.MaxWindowRows = test.factRows, test.windowRows
			err := c.Validate()
			if (err != nil) != test.invalid {
				t.Fatalf("validation: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "relations.max_fact_rows") {
				t.Fatalf("wrong config field: %v", err)
			}
		})
	}
}

func TestExampleEnumeratesEveryTuningSetting(t *testing.T) {
	raw, err := os.ReadFile("../../../cmd/server/server.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c := validConfig()
	if err := DecodeYAML(raw, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	var checkDefaults func(reflect.Value, reflect.Value, string)
	checkDefaults = func(actual, expected reflect.Value, path string) {
		for i := 0; i < actual.NumField(); i++ {
			field := actual.Field(i)
			name := path + "." + actual.Type().Field(i).Name
			switch field.Kind() {
			case reflect.Struct:
				checkDefaults(field, expected.Field(i), name)
			case reflect.Int, reflect.Int64:
				if field.Int() != expected.Field(i).Int() {
					t.Errorf("example default %s is %d, library default is %d", name, field.Int(), expected.Field(i).Int())
				}
			}
		}
	}
	checkDefaults(reflect.ValueOf(c), reflect.ValueOf(Default()), "config")
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var check func(reflect.Type, *yaml.Node, string)
	check = func(typ reflect.Type, node *yaml.Node, path string) {
		keys := map[string]*yaml.Node{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			keys[node.Content[i].Value] = node.Content[i+1]
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := field.Tag.Get("yaml")
			if path == "" && name != "query" && name != "projection" && name != "visibility" && name != "relations" && name != "storage" && name != "websocket" {
				continue
			}
			if field.Type.Kind() != reflect.Struct && field.Type.Kind() != reflect.Int && field.Type.Kind() != reflect.Int64 {
				continue
			}
			n := keys[name]
			if n == nil {
				t.Errorf("example omits %s.%s", path, name)
				continue
			}
			if field.Type.Kind() == reflect.Struct {
				check(field.Type, n, path+"."+name)
			}
		}
	}
	check(reflect.TypeOf(Config{}), doc.Content[0], "")
}
