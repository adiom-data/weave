// projectiongen compiles the read schema/documents, not mutation endpoints.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/adiom-data/weave/internal/projectioncompiler"
	"github.com/vektah/gqlparser/v2/ast"
)

func main() {
	schemaPath := flag.String("schema", "", "GraphQL read schema")
	documentsPath := flag.String("documents", "", "GraphQL projection documents")
	output := flag.String("out", "", "generated TS core module")
	reactOutput := flag.String("react-out", "", "optional generated React module")
	runtimeImport := flag.String("runtime", "@adiom-data/weave", "runtime module import")
	check := flag.Bool("check", false, "fail if generated files are missing or stale")
	flag.Parse()
	if *schemaPath == "" || *documentsPath == "" || *output == "" {
		fail(fmt.Errorf("-schema, -documents, and -out are required"))
	}
	schema, err := read(*schemaPath)
	if err != nil {
		fail(err)
	}
	documents, err := read(*documentsPath)
	if err != nil {
		fail(err)
	}
	compiler, err := projectioncompiler.New(schema)
	if err != nil {
		fail(err)
	}
	operations, err := compiler.Compile(documents)
	if err != nil {
		fail(err)
	}
	moduleImport := "./" + strings.TrimSuffix(filepath.Base(*output), filepath.Ext(*output)) + ".js"
	if *reactOutput != "" && filepath.Dir(*reactOutput) != filepath.Dir(*output) {
		fail(fmt.Errorf("core and React output must be in the same directory"))
	}
	core, react, err := projectioncompiler.GenerateTypeScript(operations, *runtimeImport, moduleImport)
	if err != nil {
		fail(err)
	}
	if err := write(*output, core, *check); err != nil {
		fail(err)
	}
	if *reactOutput != "" {
		if err := write(*reactOutput, react, *check); err != nil {
			fail(err)
		}
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func read(path string) (*ast.Source, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%s: source exceeds 1 MiB", path)
	}
	return &ast.Source{Name: path, Input: string(data)}, nil
}
func write(path string, data []byte, check bool) error {
	if check {
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(existing) != string(data) {
			return fmt.Errorf("%s is stale; regenerate projection bindings", path)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
