// Package architecture tests the dependency direction, not application behavior.
package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGenericPackagesNeverImportExamples(t *testing.T) {
	for _, root := range []string{"../../pkg", "../../internal"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imp := range file.Imports {
				value, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					return err
				}
				if strings.HasPrefix(value, "github.com/adiom-data/weave/examples/") {
					t.Errorf("generic package %s imports application %s", path, value)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// These are deliberately supported contracts/configuration, not arbitrary
// internal structs. Adding a forwarded type requires an explicit API decision.
var publicTypes = map[string]string{
	"server":                     "AuthConfig Config Host JSONClaims Options",
	"mutation":                   "Context HeaderNames",
	"projectionserver":           "DependencyOptions MessageOptions Options PolicyOptions ProjectionLimits Server Stream",
	"accesscontrol/policy":       "Binding CacheOptions Definition Edge EdgeReference Fact Key Limits Projection Provider ProviderFunc Relation Rule Snapshot",
	"projectionserver/websocket": "Authentication AuthenticationOptions ServeStream SocketConfig Stream",
	"querysubscription":          "Batch Boundary BoundaryChange BoundaryKind BoundaryOperation BoundarySide Bounds Change ChangeKind Client DeliveryEnvelope DeliveryKind DeliveryMetadata EntitySnapshot EntitySnapshotInstaller EntitySubscribeRequest EntitySubscription Event Feed FeedOptions InitialWindow Insertion MembershipDelta Position Progress QueryRow QuerySnapshot QuerySnapshotInstaller QuerySubscribeRequest QuerySubscription RecordVersion ReferenceSource ReferenceRoute EntityTarget EntityDelta EntityEffect EntityInstallation Row Subscription SubscriptionEffect SubscriptionFailure WindowTransition",
	"querysubscription/grpc":     "Server TransportOptions",
	"storage":                    "AppliedChange AppliedKind AuthoritativeRecord Boundary BoundaryKind Bounds Change ChangeEvent ChangeFeed ChangeKind Checkpoint CommittedChanges Comparison ComparisonOperator DataSource EncodedSortKey EntityKey Fence FieldPath IdentityPreparer IdentityQuery Junction JunctionOperator Literal MessageEvent MessageFeed MessageLimits MessageSource NullTest OrderTerm PostgresOptions PostgresStorage Predicate PreparedQuery PreparedReference PreparedReferenceFetch ProgressRequester QueryFingerprint QueryLimits QuerySpec Record ReferenceBinding ReferenceFetch ReferenceLookup ReferencePreparer ReferenceWindowReader ReferenceWindowRow RelationKey RelationName ReplicationOptions SnapshotSession SortKey SourceID SourcePosition Storage TransactionMessage Value ValueState ValueType WindowRequest",
	"transactionmessage":         "Batch Client Feed MessageLimits Options Service Stream Transaction",
	"transactionmessage/grpc":    "Server",
	"accesscontrol":              "Candidate Checkpoint Controller Invalidation InvalidationTarget PrepareRequest Prepared PreparedID ProjectionPath ProjectionStep Scope SnapshotCheckpoint Claims View",
	"queryserver":                "Backend Limits Options ReferenceOptions",
}

// Public packages contain aliases and one-call forwarding functions only.
// Runtime state and orchestration belong under internal, including transports.
func TestPublicPackagesAreFacades(t *testing.T) {
	err := filepath.WalkDir("../../pkg", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(filepath.ToSlash(path), "/gen/") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		pkg := filepath.ToSlash(filepath.Dir(strings.TrimPrefix(path, "../../pkg/")))
		allowed := map[string]bool{}
		for _, name := range strings.Fields(publicTypes[pkg]) {
			allowed[name] = true
		}
		privateImports := map[string]bool{}
		for _, imp := range file.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(value, "github.com/adiom-data/weave/internal/") {
				name := filepath.Base(value)
				if imp.Name != nil {
					name = imp.Name.Name
				}
				privateImports[name] = true
			}
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv != nil || !ast.IsExported(decl.Name.Name) {
					t.Errorf("%s contains implementation function %s", path, decl.Name.Name)
					continue
				}
				if decl.Body == nil || len(decl.Body.List) != 1 {
					t.Errorf("%s: %s is not a forwarding function", path, decl.Name.Name)
					continue
				}
				var expression ast.Expr
				switch statement := decl.Body.List[0].(type) {
				case *ast.ReturnStmt:
					if len(statement.Results) == 1 {
						expression = statement.Results[0]
					}
				case *ast.ExprStmt:
					expression = statement.X
				}
				call, ok := expression.(*ast.CallExpr)
				if !ok {
					t.Errorf("%s: %s does not forward a call", path, decl.Name.Name)
					continue
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					t.Errorf("%s: %s does not call an internal implementation", path, decl.Name.Name)
					continue
				}
				name, ok := selector.X.(*ast.Ident)
				if !ok || !privateImports[name.Name] {
					t.Errorf("%s: %s does not forward internally", path, decl.Name.Name)
				}
				ast.Inspect(decl.Type, func(node ast.Node) bool {
					if selector, ok := node.(*ast.SelectorExpr); ok {
						if name, ok := selector.X.(*ast.Ident); ok && privateImports[name.Name] {
							t.Errorf("%s: public function %s exposes internal type %s", path, decl.Name.Name, selector.Sel.Name)
						}
					}
					return true
				})
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if !ast.IsExported(spec.Name.Name) || !allowed[spec.Name.Name] || !spec.Assign.IsValid() {
							t.Errorf("%s: %s is not an approved contract alias", path, spec.Name.Name)
						}
						if _, ok := spec.Type.(*ast.SelectorExpr); !ok {
							t.Errorf("%s: %s must forward an existing contract", path, spec.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if !ast.IsExported(name.Name) {
								t.Errorf("%s contains implementation state %s", path, name.Name)
							}
						}
						for _, value := range spec.Values {
							if _, ok := value.(*ast.SelectorExpr); !ok {
								t.Errorf("%s contains state other than forwarded constants/errors", path)
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The example is a real external-style composition, not a privileged importer
// that works only because its module path happens to share our internal prefix.
func TestDemoUsesOnlyPublicCorePackages(t *testing.T) {
	err := filepath.WalkDir("../../examples/demo", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "node_modules" || entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(value, "github.com/adiom-data/weave/internal/") {
				t.Errorf("%s imports private core package %s", path, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestComponentDependencyDirection(t *testing.T) {
	rules := map[string][]string{
		"../queryserver":      {"github.com/adiom-data/weave/internal/postgres", "github.com/adiom-data/weave/internal/projection"},
		"../projectionserver": {"github.com/adiom-data/weave/internal/postgres", "github.com/adiom-data/weave/internal/engine", "github.com/adiom-data/weave/internal/datastore", "github.com/adiom-data/weave/internal/queryserver"},
		"../projection":       {"github.com/adiom-data/weave/internal/postgres", "github.com/adiom-data/weave/internal/engine", "github.com/adiom-data/weave/internal/datastore", "github.com/adiom-data/weave/internal/queryserver"},
		"../postgres":         {"github.com/adiom-data/weave/internal/engine", "github.com/adiom-data/weave/internal/queryserver", "github.com/adiom-data/weave/internal/projection"},
	}
	for root, forbidden := range rules {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imp := range file.Imports {
				value, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					return err
				}
				for _, prefix := range forbidden {
					if value == prefix || strings.HasPrefix(value, prefix+"/") {
						t.Errorf("%s crosses component boundary by importing %s", path, value)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
