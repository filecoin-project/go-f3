package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// main recursively finds all the fuzz tests within an optionally given directory
// as its first, and prints the results as "<package-name> <fuzz-test-name>".
func main() {
	dir := "."
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if err := filepath.WalkDir(dir, processFile); err != nil {
		fmt.Println("Error walking the path:", err)
		os.Exit(1)
	}
}

func processFile(p string, d os.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if !d.IsDir() && strings.HasSuffix(d.Name(), "_test.go") {
		if funcs, err := getFuzzFunctions(p); err != nil {
			return err
		} else {
			for _, name := range funcs {
				fmt.Printf("%s: %s\n", path.Dir(p), name)
			}
		}
	}
	return nil
}

func getFuzzFunctions(filePath string) ([]string, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var funcs []string
	for _, decl := range node.Decls {
		if fn, isFn := decl.(*ast.FuncDecl); isFn && strings.HasPrefix(fn.Name.Name, "Fuzz") {
			funcs = append(funcs, fn.Name.Name)
		}
	}
	return funcs, nil
}
