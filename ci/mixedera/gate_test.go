//go:build mixedera

package mixedera

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
)

const (
	gateScript       = "../../scripts/mixed-era.sh"
	coverageGate     = "unproven"
	stubGate         = "stub"
	redemptionSource = coveragePackage + "/manifest.go"
	redemptionType   = "Redemption"
)

// TestTheGateRefusesExactlyTheUnredeemedStatuses drives the shell's own
// coverage line against every status status can report, because the shell
// re-derives the verdict from a status column it does not own: a status the
// manifest reports and the gate does not classify would otherwise decide a
// release by accident. A row is legitimate exactly when something redeemed it,
// so ABSENT and ENTAILED clear the gate and only an unredeemed row refuses it.
func TestTheGateRefusesExactlyTheUnredeemedStatuses(t *testing.T) {
	line := gateLine(t, coverageGate)
	tests := []struct {
		name     string
		absence  string
		redeemed coverage.Redemption
	}{
		{"claimed, evidenced", "", coverage.ByEvidence},
		{"claimed, entailed", "", coverage.ByEntailment},
		{"claimed, unredeemed", "", coverage.Unredeemed},
		{"declared absent, evidenced", "predates the cut", coverage.ByEvidence},
		{"declared absent, entailed", "predates the cut", coverage.ByEntailment},
		{"declared absent, unredeemed", "predates the cut", coverage.Unredeemed},
	}
	exercised := map[coverage.Redemption]bool{}
	for _, tt := range tests {
		exercised[tt.redeemed] = true
	}
	if declared := redemptionMembers(t); len(exercised) != declared {
		t.Fatalf("%s declares %d %s constants and this table exercises %d, so a status it produces reaches no case here",
			redemptionSource, declared, redemptionType, len(exercised))
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := coverage.StatusOf(tt.absence, tt.redeemed)
			want := tt.redeemed == coverage.Unredeemed
			if got := gateRefuses(t, line, coverageRow(cutEra, mechanismTrustGate, status)); got != want {
				t.Errorf("%s reports %s and %s refuses it = %v, want %v", tt.name, status, gateScript, got, want)
			}
		})
	}
	coverage.Observe(t)
}

// TestTheGateReadsOnlyTheSubjectEra pins the coverage line's era filter. The
// record carries every era the run touched, and only the subject's rows are
// this release's to redeem: a gate reading all of them would refuse a release
// over an era that predates the boundary and can no longer be proven.
func TestTheGateReadsOnlyTheSubjectEra(t *testing.T) {
	line := gateLine(t, coverageGate)
	if gateRefuses(
		t, line,
		coverageRow(cutEra, mechanismTrustGate, coverage.StatusProven),
		coverageRow(precutEra, mechanismTrustGate, coverage.StatusOpen),
	) {
		t.Errorf("a %s row refuses a %s subject whose own rows are all %s, so the coverage line no longer filters on the era",
			precutEra, cutEra, coverage.StatusProven)
	}
	coverage.Observe(t)
}

// TestTheGateRefusesAStubPeerOnlyForTheSubjectEra drives the coverage line's
// sibling. Both re-derive a tag-mode refusal from the same record and both
// filter on the subject era, so a change that reaches one reaches the other:
// this is the half that refuses a release whose subject peer is a stub, and
// release.yml's TODO names it as the reason a tag cannot be gated yet.
func TestTheGateRefusesAStubPeerOnlyForTheSubjectEra(t *testing.T) {
	line := gateLine(t, stubGate)
	if !gateRefuses(t, line, peerRow(cutEra, "stub")) {
		t.Errorf("the %s peer reports a stub era and %s clears it, so a release the cut era's real transport never saw goes out unrefused",
			cutEra, gateScript)
	}
	if gateRefuses(
		t, line,
		peerRow(cutEra, cutEra),
		peerRow(precutEra, "stub"),
		coverageRow(cutEra, mechanismTrustGate, coverage.StatusProven),
	) {
		t.Errorf("a %s peer row or a %s coverage row refuses a %s subject whose own peer reports %s, so the peer line no longer reads only the subject era's peer rows",
			precutEra, cutEra, cutEra, cutEra)
	}
	coverage.Observe(t)
}

// redemptionMembers reads the enum out of the manifest's source because an int
// enum carries no compile-time exhaustiveness: a member added there produces a
// status the hand-written table above reaches no case for, silently.
func redemptionMembers(t *testing.T) int {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), redemptionSource, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", redemptionSource, err)
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		named, ok := gen.Specs[0].(*ast.ValueSpec).Type.(*ast.Ident)
		if !ok || named.Name != redemptionType {
			continue
		}
		members := 0
		for _, spec := range gen.Specs {
			members += len(spec.(*ast.ValueSpec).Names)
		}
		return members
	}
	t.Fatalf("%s declares no %s enum, so this test pins a type that no longer exists", redemptionSource, redemptionType)
	return 0
}

func gateLine(t *testing.T, variable string) string {
	t.Helper()
	raw, err := os.ReadFile(gateScript)
	if err != nil {
		t.Fatalf("read %s: %v", gateScript, err)
	}
	opening := variable + "=$(awk"
	for line := range strings.Lines(string(raw)) {
		if strings.HasPrefix(strings.TrimSpace(line), opening) {
			return line
		}
	}
	t.Fatalf("%s carries no line opening %q, so this test pins a gate that no longer exists", gateScript, opening)
	return ""
}

func coverageRow(era, mechanism, status string) string {
	return fmt.Sprintf("coverage\t%s\t%s\t%s\n", era, mechanism, status)
}

func peerRow(era, declared string) string {
	return fmt.Sprintf("peer\t%s\t%s\n", era, declared)
}

func gateRefuses(t *testing.T, line string, rows ...string) bool {
	t.Helper()
	variable, _, _ := strings.Cut(strings.TrimSpace(line), "=")
	record := filepath.Join(t.TempDir(), "coverage")
	body := fmt.Sprintf("subject\t%s\n%s", cutEra, strings.Join(rows, ""))
	if err := os.WriteFile(record, []byte(body), 0o644); err != nil {
		t.Fatalf("write the coverage record the gate reads: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), "sh", "-c", line+"\nprintf '%s' \"$"+variable+"\"")
	cmd.Env = append(os.Environ(), "subject="+cutEra, "record="+record)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run %s's coverage line: %v", gateScript, err)
	}
	return len(out) > 0
}
