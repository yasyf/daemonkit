// Command exportcensus enumerates daemonkit's exported API surface and diffs it
// against a checked-in allowlist, so no export appears or disappears unreviewed.
//
// The census walks every non-internal, non-main package of the module under each
// supported GOOS/GOARCH pair: darwin, where the whole module lives, and linux,
// limited to the portable subset ci/portable.txt declares and
// scripts/portable-gate.sh proves. Output is one sorted line per symbol:
//
//	<package>\t<kind>\t<symbol>\t<platform>
//
// kind is const, var, func, type, method, or field; symbol is Name for a
// package-level identifier and Type.Name for a method or struct field; platform
// is all when a symbol is present under every pair.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

type platform struct {
	goos   string
	goarch string
	// manifest, when set, limits the lane to the module-relative package
	// directories that file lists, so a lane records only the surface a
	// consumer on this GOOS can actually link against. A package whose files
	// merely lack a build constraint still fails to compile off darwin.
	manifest string
}

var platforms = []platform{
	{goos: "darwin", goarch: "arm64"},
	{goos: "linux", goarch: "amd64", manifest: "ci/portable.txt"},
}

type symbol struct {
	pkg  string
	kind string
	name string
}

func (s symbol) less(o symbol) bool {
	if s.pkg != o.pkg {
		return s.pkg < o.pkg
	}
	if s.name != o.name {
		return s.name < o.name
	}
	return s.kind < o.kind
}

func main() {
	root := flag.String("root", "", "module root to census (default: the repository containing this command)")
	out := flag.String("o", "", "write the census to this file instead of stdout")
	check := flag.String("check", "", "diff the census against this allowlist and exit non-zero on any difference")
	flag.Parse()

	dir, err := moduleRoot(*root)
	if err != nil {
		fatal(err)
	}
	lines, err := census(dir)
	if err != nil {
		fatal(err)
	}

	if *check != "" {
		want, err := readLines(*check)
		if err != nil {
			fatal(err)
		}
		if !report(*check, want, lines) {
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "export census: %d symbols, allowlist matches %s\n", len(lines), *check)
		return
	}

	body := strings.Join(lines, "\n") + "\n"
	if *out == "" {
		fmt.Print(body)
	} else if err := os.WriteFile(*out, []byte(body), 0o600); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "export census: %d symbols\n", len(lines))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "exportcensus:", err)
	os.Exit(2)
}

func moduleRoot(override string) (string, error) {
	if override != "" {
		return filepath.Abs(override)
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// census returns the sorted census lines for the module rooted at dir.
func census(dir string) ([]string, error) {
	seen := map[symbol][]string{}
	for _, p := range platforms {
		found, err := collect(dir, p)
		if err != nil {
			return nil, err
		}
		for s := range found {
			seen[s] = append(seen[s], p.goos)
		}
	}

	all := make([]symbol, 0, len(seen))
	for s := range seen {
		all = append(all, s)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].less(all[j]) })

	lines := make([]string, 0, len(all))
	for _, s := range all {
		goos := seen[s]
		slices.Sort(goos)
		tag := strings.Join(goos, ",")
		if len(goos) == len(platforms) {
			tag = "all"
		}
		lines = append(lines, strings.Join([]string{s.pkg, s.kind, s.name, tag}, "\t"))
	}
	return lines, nil
}

// collect parses every censused package under dir with p's build constraints.
func collect(dir string, p platform) (map[symbol]bool, error) {
	ctx := build.Default
	ctx.GOOS = p.goos
	ctx.GOARCH = p.goarch
	ctx.CgoEnabled = false
	ctx.UseAllFiles = false

	only, err := manifested(dir, p.manifest)
	if err != nil {
		return nil, err
	}

	found := map[symbol]bool{}
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if skipDir(rel, d.Name()) {
			return fs.SkipDir
		}
		if only != nil && !only[filepath.ToSlash(rel)] {
			return nil
		}
		pkg, err := ctx.ImportDir(path, 0)
		if err != nil {
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				return nil
			}
			return fmt.Errorf("import %s (%s/%s): %w", rel, p.goos, p.goarch, err)
		}
		if pkg.Name == "main" {
			return nil
		}
		for _, file := range pkg.GoFiles {
			if err := scan(filepath.Join(path, file), filepath.ToSlash(rel), found); err != nil {
				return err
			}
		}
		return nil
	})
	return found, err
}

// manifested reads a lane's package manifest into a lookup set, or returns nil
// for a lane that censuses every package it walks.
func manifested(dir, manifest string) (map[string]bool, error) {
	if manifest == "" {
		return nil, nil
	}
	lines, err := readLines(filepath.Join(dir, manifest))
	if err != nil {
		return nil, err
	}
	only := make(map[string]bool, len(lines))
	for _, line := range lines {
		only[line] = true
	}
	return only, nil
}

func skipDir(rel, name string) bool {
	if rel == "." {
		return false
	}
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return true
	}
	switch name {
	case "internal", "testdata", "vendor", "ci", "scripts", "Sources", "Tests", "docs":
		return true
	}
	return false
}

// scan records every exported declaration in one file.
func scan(path, pkg string, found map[symbol]bool) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	add := func(kind, name string) { found[symbol{pkg: pkg, kind: kind, name: name}] = true }

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			scanFunc(d, add)
		case *ast.GenDecl:
			scanGen(d, add)
		}
	}
	return nil
}

func scanFunc(d *ast.FuncDecl, add func(kind, name string)) {
	if !d.Name.IsExported() {
		return
	}
	if d.Recv == nil {
		add("func", d.Name.Name)
		return
	}
	recv := baseName(d.Recv.List[0].Type)
	if !ast.IsExported(recv) {
		return
	}
	add("method", recv+"."+d.Name.Name)
}

func scanGen(d *ast.GenDecl, add func(kind, name string)) {
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.ValueSpec:
			kind := "var"
			if d.Tok == token.CONST {
				kind = "const"
			}
			for _, name := range s.Names {
				if name.IsExported() {
					add(kind, name.Name)
				}
			}
		case *ast.TypeSpec:
			if !s.Name.IsExported() {
				continue
			}
			add("type", s.Name.Name)
			scanMembers(s.Name.Name, s.Type, add)
		}
	}
}

// scanMembers records the exported struct fields or interface methods a type declares.
func scanMembers(owner string, expr ast.Expr, add func(kind, name string)) {
	switch t := expr.(type) {
	case *ast.StructType:
		for _, field := range t.Fields.List {
			for _, name := range fieldNames(field) {
				add("field", owner+"."+name)
			}
		}
	case *ast.InterfaceType:
		for _, field := range t.Methods.List {
			for _, name := range fieldNames(field) {
				add("method", owner+"."+name)
			}
		}
	}
}

// fieldNames returns the exported names a struct field or interface element
// contributes, resolving an embedded element to the type name it promotes.
func fieldNames(field *ast.Field) []string {
	if len(field.Names) == 0 {
		if name := baseName(field.Type); ast.IsExported(name) {
			return []string{name}
		}
		return nil
	}
	var names []string
	for _, name := range field.Names {
		if name.IsExported() {
			names = append(names, name.Name)
		}
	}
	return names
}

// baseName reduces a receiver or embedded-field type expression to its identifier.
func baseName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return baseName(t.X)
	case *ast.IndexExpr:
		return baseName(t.X)
	case *ast.IndexListExpr:
		return baseName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}

// report prints the added and removed symbols separately, returning true when
// the census and the allowlist agree.
func report(path string, want, got []string) bool {
	have := map[string]bool{}
	for _, line := range want {
		have[line] = true
	}
	current := map[string]bool{}
	for _, line := range got {
		current[line] = true
	}

	var added, removed []string
	for _, line := range got {
		if !have[line] {
			added = append(added, line)
		}
	}
	for _, line := range want {
		if !current[line] {
			removed = append(removed, line)
		}
	}
	if len(added) == 0 && len(removed) == 0 {
		return true
	}

	fmt.Fprintf(os.Stderr, "export census: %s is stale (%d symbols in tree, %d in allowlist)\n",
		path, len(got), len(want))
	section := func(label, sign string, lines []string) {
		if len(lines) == 0 {
			return
		}
		fmt.Fprintf(os.Stderr, "\n%s (%d):\n", label, len(lines))
		for _, line := range lines {
			fmt.Fprintf(os.Stderr, "  %s %s\n", sign, line)
		}
	}
	section("ADDED exports (in the tree, not in the allowlist)", "+", added)
	section("REMOVED exports (in the allowlist, not in the tree)", "-", removed)
	fmt.Fprintf(os.Stderr, "\nEvery difference needs review. Once approved: scripts/export-census.sh --write\n")
	return false
}
