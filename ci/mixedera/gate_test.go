//go:build mixedera

package mixedera

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	gateScript   = "../../scripts/mixed-era.sh"
	coverageGate = "unproven=$(awk"
)

// TestTheGateRefusesExactlyTheUnredeemedStatuses drives the shell's own
// coverage line against every status status can report, because the shell
// re-derives the verdict from a status column it does not own: a status the
// manifest reports and the gate does not classify would otherwise decide a
// release by accident. A row is legitimate exactly when something redeemed it,
// so ABSENT and ENTAILED clear the gate and only an unredeemed row refuses it.
func TestTheGateRefusesExactlyTheUnredeemedStatuses(t *testing.T) {
	line := coverageLine(t)
	tests := []struct {
		name     string
		absence  string
		redeemed redemption
	}{
		{"claimed, evidenced", "", byEvidence},
		{"claimed, entailed", "", byEntailment},
		{"claimed, unredeemed", "", unredeemed},
		{"declared absent, evidenced", "predates the cut", byEvidence},
		{"declared absent, entailed", "predates the cut", byEntailment},
		{"declared absent, unredeemed", "predates the cut", unredeemed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &manifest{covered: map[string]map[string]*coverage{
				cutEra: {mechanismTrustGate: {absence: tt.absence, redeemed: tt.redeemed}},
			}}
			status := m.status(cutEra, mechanismTrustGate)
			want := tt.redeemed == unredeemed
			if got := gateRefuses(t, line, status); got != want {
				t.Errorf("%s reports %s and %s refuses it = %v, want %v", tt.name, status, gateScript, got, want)
			}
		})
	}
}

func coverageLine(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(gateScript)
	if err != nil {
		t.Fatalf("read %s: %v", gateScript, err)
	}
	for line := range strings.Lines(string(raw)) {
		if strings.HasPrefix(strings.TrimSpace(line), coverageGate) {
			return line
		}
	}
	t.Fatalf("%s carries no line opening %q, so this test pins a gate that no longer exists", gateScript, coverageGate)
	return ""
}

func gateRefuses(t *testing.T, line, status string) bool {
	t.Helper()
	record := filepath.Join(t.TempDir(), "coverage")
	rows := fmt.Sprintf("subject\t%s\ncoverage\t%s\t%s\t%s\n", cutEra, cutEra, mechanismTrustGate, status)
	if err := os.WriteFile(record, []byte(rows), 0o644); err != nil {
		t.Fatalf("write the coverage record the gate reads: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), "sh", "-c", line+"\nprintf '%s' \"$unproven\"")
	cmd.Env = append(os.Environ(), "subject="+cutEra, "record="+record)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run %s's coverage line: %v", gateScript, err)
	}
	return len(out) > 0
}
