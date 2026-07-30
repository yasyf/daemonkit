package proc

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestSingleIdentityComparisonSite is the gate on the one-comparison-site
// invariant: identity.matches is declared exactly once, in identity.go, no
// other file compares a start stamp or boot session field directly, and no
// other file compares whole identity values with == or !=.
func TestSingleIdentityComparisonSite(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fieldComparison := regexp.MustCompile(`(?i)\.(start|boot)\b\s*[=!]=`)
	matchesDecl := regexp.MustCompile(`\) matches\(`)
	declarations := 0
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		declarations += len(matchesDecl.FindAll(raw, -1))
		if source == "identity.go" {
			if !strings.Contains(string(raw), "func (i identity) matches(o identity) bool { return i == o }") {
				t.Error("identity.go no longer declares the single matches comparison")
			}
			continue
		}
		for i, line := range strings.Split(string(raw), "\n") {
			code, _, _ := strings.Cut(line, "//")
			if fieldComparison.MatchString(code) {
				t.Errorf("%s:%d open-codes an identity comparison outside identity.go: %s", source, i+1, strings.TrimSpace(line))
			}
		}
	}
	if declarations != 1 {
		t.Errorf("found %d matches declarations, want exactly 1 in identity.go", declarations)
	}

	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(pkg.GoFiles))
	for _, source := range pkg.GoFiles {
		parsed, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, parsed)
	}
	// Imports stay unresolved on purpose: local inference still types every
	// identity operand, and the swallowed resolution errors are irrelevant.
	conf := types.Config{Error: func(error) {}}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	_, _ = conf.Check("proc", fset, files, info)
	for _, file := range files {
		if fset.Position(file.Pos()).Filename == "identity.go" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			cmp, ok := n.(*ast.BinaryExpr)
			if !ok || (cmp.Op != token.EQL && cmp.Op != token.NEQ) {
				return true
			}
			if !isIdentityType(info.Types[cmp.X].Type) && !isIdentityType(info.Types[cmp.Y].Type) {
				return true
			}
			pos := fset.Position(cmp.Pos())
			t.Errorf("%s:%d compares whole identity values outside identity.go's matches", pos.Filename, pos.Line)
			return true
		})
	}
}

func isIdentityType(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Name() == "identity" && named.Obj().Pkg() != nil && named.Obj().Pkg().Name() == "proc"
}
