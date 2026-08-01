//go:build mixedera

package mixedera

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
)

const (
	presentObserver = "ObservedPresent"
	absentObserver  = "ObservedAbsent"
	journalReceiver = "evidence"
	journalRecorder = "record"

	// coveragePackage is where the gate's state lives: beside this harness, and
	// out of its reach.
	coveragePackage = "coverage"

	mechanismArgument = 2
	kindArgument      = 3

	// harnessParam is the one parameter a witness may not count as something it
	// was handed: branching on the test handle is branching on the harness, not
	// on what the witness was given to observe.
	harnessParam = "*testing.T"
)

// TestEveryFactReachesTheJournalThroughItsFrozenWitness reads the gate's own
// source, both packages of it, because the binding the journal applies can only
// refuse a call that runs: a fact filed from a foreign site under a branch this
// configuration never takes would sit in the tree unrefused until the branch
// fired. It pins the map both ways — no observer call outside the site
// mechanisms.txt names for what it files, and no site named there that files
// nothing — and holds the coverage package's own source to the one path into the
// journal that the compiler already holds this package to.
func TestEveryFactReachesTheJournalThroughItsFrozenWitness(t *testing.T) {
	sources := packageSources(t)
	literals := stringConstants(sources)
	filed := map[string]bool{}
	for _, file := range sources {
		auditFilings(t, file, literals, filed)
	}
	for _, reserved := range coverage.Reservations() {
		if !filed[filing(reserved)] {
			t.Errorf("%s reserves %q's %s evidence for %s, and %s files none of it: the reservation names a site that does not witness that mechanism",
				coverage.MechanismPath, reserved.Mechanism, reserved.Kind, reserved.Site, reserved.Site)
		}
	}
	coverage.Observe(t)
}

func auditFilings(t *testing.T, file *ast.File, literals map[string]string, filed map[string]bool) {
	t.Helper()
	within := file.Name.Name
	inspect := func(site string, call *ast.CallExpr) {
		if recordsDirectly(call) && site != presentObserver && site != absentObserver {
			t.Errorf("%s reaches %s.%s itself, around the witness binding %s and %s carry",
				site, journalReceiver, journalRecorder, presentObserver, absentObserver)
			return
		}
		switch observer := observerCalled(call); observer {
		case presentObserver, absentObserver:
		default:
			return
		}
		reserved := coverage.Reservation{
			Mechanism: literals[argued(t, site, within, call, mechanismArgument)],
			Kind:      coverage.EvidenceKind(literals[argued(t, site, within, call, kindArgument)]),
			Site:      site,
		}
		filed[filing(reserved)] = true
		if err := coverage.Reserves(reserved.Site, reserved.Mechanism, reserved.Kind); err != nil {
			t.Error(err)
		}
	}
	for _, decl := range file.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			if typed.Body != nil {
				visitCalls(typed.Body, declaredSite(typed), inspect)
			}
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				valued, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, value := range valued.Values {
					visitCalls(value, valued.Names[0].Name, inspect)
				}
			}
		}
	}
}

// TestEveryRegisteredWitnessBranchesOnWhatItWasHanded refuses a witness that
// narrates. Routing a fact to one site says only where it was filed: a function
// that formats whatever three reports it is passed and files unconditionally
// leaves its row PROVEN with every assertion deleted from the case that calls
// it, and no frozen file changes. So each site mechanisms.txt registers is read
// out of the gate's own source and has to branch — an if, a switch, or a
// bounded for — on the receiver, a parameter, or something derived from one.
//
// What this does not refuse is a witness branching on the wrong thing, or one
// whose branch decides nothing: the body is walked flat, with no dominance or
// ordering analysis, so a branch sitting after the filing call, or inside an
// unrelated closure, satisfies it. A branch is a lower bound on observation,
// not a proof of it.
func TestEveryRegisteredWitnessBranchesOnWhatItWasHanded(t *testing.T) {
	registered := map[string]bool{}
	for _, reserved := range coverage.Reservations() {
		registered[reserved.Site] = true
	}
	for _, file := range packageSources(t) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !registered[declaredSite(fn)] {
				continue
			}
			if !branchesOnWhatItWasHanded(fn) {
				t.Errorf("%s files evidence %s registers it to witness, and nothing in its body branches on the receiver or on any argument it was handed: it formats what it is passed and files it. Delete every assertion from the case that calls it and the row still reads PROVEN. Refuse the fact in the witness, on the artifact, before filing it",
					declaredSite(fn), coverage.MechanismPath)
			}
		}
	}
	coverage.Observe(t)
}

// branchesOnWhatItWasHanded reports whether some branch in fn tests a value that
// reached it from outside: the receiver, a parameter other than the test handle,
// or anything assigned or ranged from one of those.
func branchesOnWhatItWasHanded(fn *ast.FuncDecl) bool {
	handed := handedNames(fn)
	branched := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.IfStmt:
			branched = branched || mentions(typed.Cond, handed)
		case *ast.ForStmt:
			branched = branched || (typed.Cond != nil && mentions(typed.Cond, handed))
		case *ast.SwitchStmt:
			branched = branched || switchReads(typed, handed)
		}
		return true
	})
	return branched
}

// switchReads reads both switch shapes: a tagged switch branches on its tag, and
// a tagless one on each case's own expression.
func switchReads(switched *ast.SwitchStmt, handed map[string]bool) bool {
	if switched.Tag != nil {
		return mentions(switched.Tag, handed)
	}
	for _, stmt := range switched.Body.List {
		clause, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}
		for _, expr := range clause.List {
			if mentions(expr, handed) {
				return true
			}
		}
	}
	return false
}

// handedNames names everything in fn that came from outside it, following one
// step at a time through assignments and range clauses: a witness that unpacks
// its argument before testing it has still branched on that argument.
func handedNames(fn *ast.FuncDecl) map[string]bool {
	handed := map[string]bool{}
	fields := fn.Type.Params.List
	if fn.Recv != nil {
		fields = append(slices.Clone(fn.Recv.List), fields...)
	}
	for _, field := range fields {
		if types.ExprString(field.Type) == harnessParam {
			continue
		}
		for _, name := range field.Names {
			handed[name.Name] = true
		}
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.AssignStmt:
			if mentionsAny(typed.Rhs, handed) {
				markNames(typed.Lhs, handed)
			}
		case *ast.RangeStmt:
			if mentions(typed.X, handed) {
				markNames([]ast.Expr{typed.Key, typed.Value}, handed)
			}
		}
		return true
	})
	return handed
}

func markNames(exprs []ast.Expr, handed map[string]bool) {
	for _, expr := range exprs {
		if named, ok := expr.(*ast.Ident); ok && named.Name != "_" {
			handed[named.Name] = true
		}
	}
}

func mentions(expr ast.Expr, handed map[string]bool) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if named, ok := node.(*ast.Ident); ok && handed[named.Name] {
			found = true
		}
		return true
	})
	return found
}

func mentionsAny(exprs []ast.Expr, handed map[string]bool) bool {
	for _, expr := range exprs {
		if mentions(expr, handed) {
			return true
		}
	}
	return false
}

// visitCalls hands every call under one declaration to seen, naming the
// function it sits in. A call inside a func literal is named for the literal
// rather than for what encloses it: the matrix's own cases are literals in the
// gateCases table, and the runtime never sees such a call as any witness.
func visitCalls(node ast.Node, site string, seen func(string, *ast.CallExpr)) {
	ast.Inspect(node, func(inner ast.Node) bool {
		switch typed := inner.(type) {
		case *ast.FuncLit:
			if inner != node {
				visitCalls(typed.Body, "a func literal in "+site, seen)
				return false
			}
		case *ast.CallExpr:
			seen(site, typed)
		}
		return true
	})
}

func filing(reserved coverage.Reservation) string {
	return strings.Join([]string{reserved.Mechanism, string(reserved.Kind), reserved.Site}, "\x00")
}

// observerCalled names the observer a call reaches for, whether the harness
// names it through the coverage package or that package's own source names it
// directly.
func observerCalled(call *ast.CallExpr) string {
	switch called := call.Fun.(type) {
	case *ast.Ident:
		return called.Name
	case *ast.SelectorExpr:
		if pkg, ok := called.X.(*ast.Ident); ok && pkg.Name == coveragePackage {
			return called.Sel.Name
		}
	}
	return ""
}

// argued reads the constant an observer was handed, qualified by the package
// that declares it, so a call naming its mechanism or its evidence class with
// anything this audit cannot follow is reported rather than skipped.
func argued(t *testing.T, site, within string, call *ast.CallExpr, at int) string {
	t.Helper()
	if at >= len(call.Args) {
		t.Fatalf("%s files a fact with %d arguments", site, len(call.Args))
	}
	switch named := call.Args[at].(type) {
	case *ast.Ident:
		return within + "." + named.Name
	case *ast.SelectorExpr:
		if pkg, ok := named.X.(*ast.Ident); ok {
			return pkg.Name + "." + named.Sel.Name
		}
	}
	t.Fatalf("%s files a fact naming argument %d with %s rather than one of this gate's constants, so this audit cannot read what it files",
		site, at, types.ExprString(call.Args[at]))
	return ""
}

func recordsDirectly(call *ast.CallExpr) bool {
	selected, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selected.Sel.Name != journalRecorder {
		return false
	}
	receiver, ok := selected.X.(*ast.Ident)
	return ok && receiver.Name == journalReceiver
}

func declaredSite(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return fn.Name.Name
	}
	return "(" + types.ExprString(fn.Recv.List[0].Type) + ")." + fn.Name.Name
}

func stringConstants(sources []*ast.File) map[string]string {
	held := map[string]string{}
	for _, file := range sources {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				valued := spec.(*ast.ValueSpec)
				for i, name := range valued.Names {
					if i >= len(valued.Values) {
						continue
					}
					literal, ok := valued.Values[i].(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						continue
					}
					text, err := strconv.Unquote(literal.Value)
					if err != nil {
						continue
					}
					held[file.Name.Name+"."+name.Name] = text
				}
			}
		}
	}
	return held
}

func packageSources(t *testing.T) []*ast.File {
	t.Helper()
	fset := token.NewFileSet()
	var parsed []*ast.File
	for _, dir := range []string{".", coveragePackage} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			parsed = append(parsed, file)
		}
	}
	if len(parsed) == 0 {
		t.Fatal("this audit found none of the gate's own source beside it")
	}
	return parsed
}
