package daemonkit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// systemBinDirs are the directories a test names when it wants a real
// executable off the machine rather than one it built itself. Homebrew's two
// prefixes are among them: an out-of-tree tool literal lives there, and
// artifact resolves exactly such a tool through WithUV.
var systemBinDirs = []string{
	"/bin/",
	"/sbin/",
	"/usr/bin/",
	"/usr/sbin/",
	"/usr/libexec/",
	"/usr/local/bin/",
	"/opt/homebrew/bin/",
}

// absentBinaryPrefix is the module's reserved way to name a system path that
// must NOT exist. daemonkit installs nothing into systemBinDirs, so a
// deliberately absent executable is spelled /usr/bin/daemonkit-<what>.
const absentBinaryPrefix = "daemonkit-"

// TestEverySystemBinaryNamedByATestExists makes the vacuous guard
// unconstructible. A test that names /bin/true on a machine that has only
// /usr/bin/true still refuses, still errors, and still passes — while proving
// nothing but ENOENT — so the assertion it claims to make cannot fail. This
// walks the module's own test sources instead of trusting each author to
// check, and reports the file:line of any system executable that is not
// there.
//
// Three blind spots, stated so the guard is not read as coverage it lacks. It
// sees one string literal at a time, so a path built by concatenation escapes
// it — a const does not, since the literal it binds is still a literal in this
// file set. It walks only *_test.go, so a literal in ordinary source compiled
// into a test binary escapes it too — today that is adopt.go's /bin/sh and
// launchd/apply.go's /bin/launchctl, both present. And a literal that carries
// an argument is not a path, so the executable inside a command line goes
// unchecked; name it as its own literal to get it covered.
func TestEverySystemBinaryNamedByATestExists(t *testing.T) {
	fset := token.NewFileSet()
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "testdata" || name == ".build" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !namesASystemBinary(value) {
				return true
			}
			if info, err := os.Stat(value); err != nil || info.Mode()&0o111 == 0 {
				t.Errorf("%s: %q is not an executable on this machine; a guard proven against it proves ENOENT instead", fset.Position(literal.Pos()), value)
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk the module's test sources: %v", walkErr)
	}
}

func namesASystemBinary(value string) bool {
	if !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return false
	}
	if strings.ContainsAny(value, " \t\n") {
		return false
	}
	if strings.HasPrefix(filepath.Base(value), absentBinaryPrefix) {
		return false
	}
	for _, dir := range systemBinDirs {
		if strings.HasPrefix(value, dir) {
			return true
		}
	}
	return false
}
