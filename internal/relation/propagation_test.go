package relation

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

type countedExpression struct {
	inner compiledExpression
	calls *int
}

func (e countedExpression) evaluate(s *evaluation) (storage.Value, uint64, error) {
	*e.calls++
	return e.inner.evaluate(s)
}

func TestNamedExportsShareEligibilityAndSupportMultipleIndependentFields(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Computed: map[string]Computed{
			"_role":  {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}},
			"_label": {Type: storage.TypeText, Expression: `"editor"`, Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}},
		}},
		tasks: {Computed: map[string]Computed{
			"present": {Type: storage.TypeBool, Expression: `has(inherited._role)`},
			"canEdit": {Type: storage.TypeBool, Expression: `has(inherited._role) && inherited._role >= 2`},
			"label":   {Type: storage.TypeText, Expression: `has(inherited._label) ? inherited["_label"] : "unknown"`},
		}, Visibility: `has(inherited._role) && inherited._role >= 1`},
	})
	// Instrument the private immutable plan before exposing it to this test's
	// evaluation. The same guard must not execute once per read/consumer field.
	parentDef := e.store.current.Load().relations[projects]
	key := exportTarget{to: tasks, field: "_role"}
	calls := 0
	parentDef.exports[key] = countedExpression{parentDef.exports[key], &calls}
	pass := e.Begin()
	parent := pass.NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")})}, nil, nil, false)
	child := pass.NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{"project_id": text("p1")}), Parent: &ParentState{Candidate: parent.input.Candidate, Visible: true}}, nil, parent, false)
	for _, name := range []string{"present", "canEdit", "label"} {
		value, err := child.Compute(t.Context(), name)
		if err != nil || !value.Complete {
			t.Fatalf("%s: %+v %v", name, value, err)
		}
	}
	if value, err := child.Visibility(t.Context()); err != nil || !decision(value).Visible {
		t.Fatalf("visibility: %+v %v", value, err)
	}
	if calls != 1 || len(parent.values) != 2 || len(child.exports) != 2 {
		t.Fatalf("duplicate export/value work: guard=%d parent=%d exports=%d", calls, len(parent.values), len(child.exports))
	}
	if got := string(child.values["label"].Value.Data); got != "editor" {
		t.Fatal(got)
	}
}

func TestAbsentExportRetainsDefinitionDependencyForLiveReplacement(t *testing.T) {
	defs := Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2"}}},
		tasks:    {Computed: map[string]Computed{"canEdit": {Type: storage.TypeBool, Expression: `has(inherited._role) && inherited._role >= 2`}}},
	}
	store, err := NewStore(defs, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(store)
	watch := e.Watch()
	defer watch.Close()
	input := Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{}), Visible: true}}
	pass := e.Begin()
	value, err := pass.NewContext(input, nil, nil, false).Compute(t.Context(), "canEdit")
	if err != nil || !value.Complete || decision(value).Visible {
		t.Fatalf("absent export: %+v %v", value, err)
	}
	if _, found := value.Definitions[projects]; !found {
		t.Fatal("missing negative parent dependency")
	}
	watch.Replace(nil, pass.Stamps([]Key{projects, tasks}))
	field := defs[projects].Computed["_role"]
	field.Propagate = []Propagation{{To: tasks, When: "true"}}
	defs[projects].Computed["_role"] = field
	if err := store.Replace(defs); err != nil {
		t.Fatal(err)
	}
	if got := watch.Drain(); !reflect.DeepEqual(got, []Key{projects}) {
		t.Fatalf("adding export failed to invalidate: %v", got)
	}
	for _, test := range []struct {
		pass *Evaluation
		want bool
	}{{pass, false}, {e.Begin(), true}} {
		value, err := test.pass.NewContext(input, nil, nil, false).Compute(t.Context(), "canEdit")
		if err != nil || !value.Complete || decision(value).Visible != test.want {
			t.Fatalf("definition snapshot changed: %+v %v", value, err)
		}
	}
}

func TestEmptyIncomingAllowlistRejectsExportsButNotLocalValues(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: tasks, When: "true"}}}}},
		tasks:    {Receive: []Incoming{}, Computed: map[string]Computed{"canEdit": {Type: storage.TypeBool, Expression: `has(inherited._role)`}, "local": {Type: storage.TypeBool, Expression: "true"}}},
	})
	c := e.Begin().NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{}), Visible: true}}, nil, nil, false)
	for name, want := range map[string]bool{"canEdit": false, "local": true} {
		value, err := c.Compute(t.Context(), name)
		if err != nil || !value.Complete || decision(value).Visible != want {
			t.Fatalf("%s: %+v %v", name, value, err)
		}
	}
}

func TestInheritedPresenceDistinguishesNullAndFalseFromAbsent(t *testing.T) {
	for _, expression := range []string{"false", "null"} {
		e := compile(t, Definitions{
			projects: {Computed: map[string]Computed{"_flag": {Type: storage.TypeBool, Expression: `item.flag`, Propagate: []Propagation{{To: tasks, When: "true"}}}}},
			tasks: {Computed: map[string]Computed{
				"present": {Type: storage.TypeBool, Expression: `has(inherited._flag)`},
				"copy":    {Type: storage.TypeBool, Expression: `has(inherited._flag) ? inherited._flag : false`},
			}},
		})
		flag := boolean(false)
		if expression == "null" {
			flag = storage.Value{State: storage.ValueNull}
		}
		c := e.Begin().NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{"flag": flag}), Visible: true}}, nil, nil, false)
		present, err := c.Compute(t.Context(), "present")
		if err != nil || !decision(present).Visible {
			t.Fatalf("eligible %s became absent: %+v %v", expression, present, err)
		}
		value, err := c.Compute(t.Context(), "copy")
		if err != nil || !value.Complete || expression == "null" && value.Value.State != storage.ValueNull || expression == "false" && string(value.Value.Data) != "false" {
			t.Fatalf("export value changed: %+v %v", value, err)
		}
	}
}

func TestInheritedPresencePreservesComprehensionLocalBindings(t *testing.T) {
	e := compile(t, Definitions{projects: {Visibility: `[{"field": true}].exists(inherited, has(inherited.field))`}})
	got, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{})}, nil)
	if err != nil || !got.Visible {
		t.Fatalf("local map was treated as a context export: %+v %v", got, err)
	}
}
