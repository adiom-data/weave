package policy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/storage"
)

var policyBenchmarkResult bool

// Includes provider lookup and compilation-cache admission. The provider is
// in-memory: application policy-store/network latency is deliberately excluded.
func BenchmarkPolicyPreparation(b *testing.B) {
	for _, cached := range []bool{true, false} {
		b.Run(fmt.Sprintf("cached=%t", cached), func(b *testing.B) {
			definition := Relation{Allow: []Rule{{ID: "view", When: `item.organization_id == claims.organization_id`}}}
			var version uint64
			provider := ProviderFunc(func(context.Context, Key) (Snapshot, error) {
				if !cached {
					version++
				}
				return Snapshot{Version: fmt.Sprint(version), Policy: &definition}, nil
			})
			evaluator, err := NewEvaluator(context.Background(), provider, DefaultLimits(), CacheOptions{MaintenancePeriod: time.Hour}, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer evaluator.Close()
			in := Input{Candidate: candidate(projects, "p1", storage.Record{"organization_id": text("org")}), Claims: accesscontrol.Claims{"organization_id": text("org")}}
			if _, err := evaluator.Evaluate(context.Background(), in, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				decision, err := evaluator.Evaluate(context.Background(), in, nil)
				if err != nil || !decision.Visible {
					b.Fatalf("evaluate: %v", err)
				}
			}
		})
	}
}

// Separate the language interpreter from our canonical-value adaptation and
// evaluator bookkeeping. Compilation and fact/database IO are excluded.
func BenchmarkPolicyEvaluation(b *testing.B) {
	ctx := context.Background()
	for _, fields := range []int{4, 100} {
		b.Run(fmt.Sprintf("fields=%d", fields), func(b *testing.B) {
			record := storage.Record{
				"organization_id": text("org"), "assignee_id": text("u1"),
				"team_id": text("team"), "deleted_at": {State: storage.ValueNull},
			}
			for n := 4; n < fields; n++ {
				record[fmt.Sprintf("extra_%d", n)] = text("unused")
			}
			claims := map[string]storage.Value{"organization_id": text("org"), "user_id": text("u1")}
			input := Input{Candidate: candidate(projects, "p1", record), Claims: accesscontrol.Claims(claims)}
			e, err := Compile(Map{projects: {Allow: []Rule{{"view", `item.deleted_at == null && item.organization_id == claims.organization_id && item.assignee_id == claims.user_id`}}}}, DefaultLimits())
			if err != nil {
				b.Fatal(err)
			}
			program := referenceCEL(e.relations[projects].allow[0].program).program
			activation := map[string]any{"item": recordValue(record), "claims": recordValue(storage.Record(claims))}
			b.Run("Go", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					policyBenchmarkResult = record["deleted_at"].State == storage.ValueNull && string(record["organization_id"].Data) == string(claims["organization_id"].Data) && string(record["assignee_id"].Data) == string(claims["user_id"].Data)
				}
			})
			b.Run("CEL/prebound", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					result, _, err := program.ContextEval(ctx, activation)
					if err != nil {
						b.Fatal(err)
					}
					policyBenchmarkResult = result == types.True
				}
			})
			b.Run("Evaluator/fast/no_IO", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					decision, err := e.Evaluate(ctx, input, nil)
					if err != nil {
						b.Fatal(err)
					}
					policyBenchmarkResult = decision.Visible
				}
			})
			baseline := *e
			definition := *e.relations[projects]
			definition.allow = []compiledRule{{id: "view", program: &celExpression{program: program}}}
			baseline.relations = map[storage.RelationName]*compiledRelation{projects: &definition}
			b.Run("Evaluator/CEL/no_IO", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					decision, err := baseline.Evaluate(ctx, input, nil)
					if err != nil {
						b.Fatal(err)
					}
					policyBenchmarkResult = decision.Visible
				}
			})
		})
	}
}

func BenchmarkRecordAccess(b *testing.B) {
	env, err := cel.NewEnv(cel.Variable("item", cel.DynType))
	if err != nil {
		b.Fatal(err)
	}
	ast, issues := env.Compile(`item.a.b.c.d == 1`)
	if issues.Err() != nil {
		b.Fatal(issues.Err())
	}
	value := storage.Record{"d": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}}
	for _, key := range []string{"c", "b", "a"} {
		value = storage.Record{key: object(value)}
	}
	activation := map[string]any{"item": recordValue(value)}
	for _, decorated := range []bool{false, true} {
		b.Run(fmt.Sprintf("decorated=%t", decorated), func(b *testing.B) {
			options := []cel.ProgramOption{cel.CostTracking(nil)}
			if decorated {
				options = append(options, cel.CustomDecoratorV2(recordAccessDecorator()))
			}
			program, err := env.Program(ast, options...)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				result, _, err := program.ContextEval(context.Background(), activation)
				if err != nil {
					b.Fatal(err)
				}
				policyBenchmarkResult = result == types.True
			}
		})
	}
}

func BenchmarkNativePolicyOperations(b *testing.B) {
	ctx := context.Background()
	for _, count := range []int{16, 256} {
		b.Run(fmt.Sprintf("teams=%d", count), func(b *testing.B) {
			teams := make([]storage.Value, count)
			for i := range teams {
				teams[i] = text(fmt.Sprintf("team-%d", i))
			}
			amount, _ := storage.ParseScalar(storage.TypeDecimal, []byte("12.50"))
			minimum, _ := storage.ParseScalar(storage.TypeDecimal, []byte("10"))
			input := Input{Candidate: candidate(projects, "p1", storage.Record{"title": text("task-1"), "team_id": text(fmt.Sprintf("team-%d", count-1)), "amount": amount}), Claims: accesscontrol.Claims{"teams": list(teams...), "minimum": minimum}}
			e, err := Compile(Map{projects: {Allow: []Rule{{"view", `has(item.team_id) && item.team_id in claims.teams && size(claims.teams) > 0 && item.title.startsWith("task-") && item.amount >= claims.minimum`}}}}, DefaultLimits())
			if err != nil {
				b.Fatal(err)
			}
			if _, native := e.relations[projects].allow[0].program.(*fastExpression); !native {
				b.Fatal("benchmark must exercise native execution")
			}
			for _, native := range []bool{true, false} {
				b.Run(fmt.Sprintf("native=%t", native), func(b *testing.B) {
					implementation := *e
					if !native {
						definition := *e.relations[projects]
						definition.allow = []compiledRule{{id: "view", program: referenceCEL(definition.allow[0].program)}}
						implementation.relations = map[storage.RelationName]*compiledRelation{projects: &definition}
					}
					b.ReportAllocs()
					for b.Loop() {
						decision, err := implementation.Evaluate(ctx, input, nil)
						if err != nil {
							b.Fatal(err)
						}
						policyBenchmarkResult = decision.Visible
					}
				})
			}
		})
	}
}
