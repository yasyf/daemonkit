//go:build mixedera

package mixedera

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	precutEra   = "precut"
	cutEra      = "cut"
	summaryEnv  = "GITHUB_STEP_SUMMARY"
	coverageEnv = "MIXED_ERA_COVERAGE"
	conformWait = 30 * time.Second

	statusProven     = "PROVEN"
	statusAbsent     = "ABSENT"
	statusEntailed   = "ENTAILED"
	statusOpen       = "UNPROVEN"
	statusUnasserted = "UNASSERTED"
)

type conformance struct {
	Era        string            `json:"era"`
	Protocol   uint16            `json:"protocol"`
	Mechanisms map[string]string `json:"mechanisms"`
}

type redemption int

const (
	unredeemed redemption = iota
	byEvidence
	byEntailment
)

type coverage struct {
	absence    string
	redeemed   redemption
	antecedent claim
}

type manifest struct {
	boundary   string
	mechanisms []string
	declared   map[string]conformance
	covered    map[string]map[string]*coverage
}

func newManifest(t *testing.T, boundary string, built ...peer) *manifest {
	t.Helper()
	m := &manifest{
		boundary:   boundary,
		mechanisms: frozenLines(t, mechanismFixture),
		declared:   map[string]conformance{},
		covered:    map[string]map[string]*coverage{},
	}
	for _, p := range built {
		out := output(t, p, conformWait, "conformance")
		var declared conformance
		if err := json.Unmarshal([]byte(out), &declared); err != nil {
			t.Fatalf("decode %s era conformance %q: %v", p.era, out, err)
		}
		if declared.Era == "" {
			t.Fatalf("%s era peer names no era", p.era)
		}
		m.declared[p.era] = declared
		m.covered[p.era] = map[string]*coverage{}
		for _, mechanism := range m.mechanisms {
			absence, accounted := declared.Mechanisms[mechanism]
			if !accounted {
				t.Fatalf("%s era peer accounts for no %q: every frozen mechanism carries a verdict",
					p.era, mechanism)
			}
			m.covered[p.era][mechanism] = &coverage{absence: absence}
		}
		for mechanism := range declared.Mechanisms {
			if _, frozen := m.covered[p.era][mechanism]; !frozen {
				t.Fatalf("%s era peer reports %q, which this gate does not assert", p.era, mechanism)
			}
		}
	}
	return m
}

// redeem trades a case's claim for evidence the harness observed while the case
// ran. The claim is never itself proof: it only names a mechanism, and the
// frozen expectation decides what redeems that name.
func (m *manifest) redeem(t *testing.T, era string, mechanisms ...string) {
	t.Helper()
	for _, mechanism := range mechanisms {
		m.redeemOne(t, era, mechanism)
	}
}

func (m *manifest) redeemOne(t *testing.T, era, mechanism string) {
	t.Helper()
	asked := claim{era: era, mechanism: mechanism}
	entry, frozen := m.covered[era][mechanism]
	if !frozen {
		t.Fatalf("no frozen mechanism %q in the %s era", mechanism, era)
	}
	owed := claimEntry(t.Name(), asked)
	if !expected.lists(owed) {
		t.Fatalf("%s redeems the %s era's %q, which %s does not demand. Add this line to that file, in sorted order, in this same commit:\n  %s evidenced",
			t.Name(), era, mechanism, expectationPath, owed)
	}
	if antecedent, entailed := expected.entailment(owed); entailed {
		m.entail(t, owed, entry, asked, antecedent)
		return
	}
	held, found := evidence.holds(t.Name(), era, mechanism, entry.absence == "")
	if !found {
		t.Errorf("%s claims the %s era's %q and nothing outside the case's own bookkeeping observed it. %s\nThis case produced:\n%s",
			t.Name(), era, mechanism, m.wanted(era, mechanism), listed(m.produced(t, era, mechanism)))
		return
	}
	t.Logf("redeemed the %s era's %q against %s", era, mechanism, held)
	entry.redeemed = byEvidence
	observations.mark(owed)
}

// entail carries a claim on another claim's artifact, for the one shape with no
// artifact of its own: a mechanism whose absence follows from the absence of the
// thing it would gate. The antecedent must be redeemed by evidence this same
// case produced, so entailment cannot reach across the matrix to borrow a proof.
func (m *manifest) entail(t *testing.T, owed string, entry *coverage, asked, antecedent claim) {
	t.Helper()
	backing, frozen := m.covered[antecedent.era][antecedent.mechanism]
	if !frozen {
		t.Fatalf("%s entails the %s era's %q by the %s era's %q, which is no frozen mechanism",
			expectationPath, asked.era, asked.mechanism, antecedent.era, antecedent.mechanism)
	}
	_, held := evidence.holds(t.Name(), antecedent.era, antecedent.mechanism, backing.absence == "")
	if backing.redeemed != byEvidence || !held {
		t.Errorf("%s claims the %s era's %q is entailed by the %s era's %q, and this case redeemed no evidence of that antecedent. This case produced:\n%s",
			t.Name(), asked.era, asked.mechanism, antecedent.era, antecedent.mechanism,
			listed(m.produced(t, antecedent.era, antecedent.mechanism)))
		return
	}
	entry.redeemed, entry.antecedent = byEntailment, antecedent
	observations.mark(owed)
}

func (m *manifest) wanted(era, mechanism string) string {
	if m.covered[era][mechanism].absence != "" {
		return fmt.Sprintf(
			"The %s peer declares %q absent, so this claim needs an observation that it did NOT happen — a daemon the OS never reaped, or a connection the daemon closed unanswered.",
			era, mechanism,
		)
	}
	return "A claim is redeemable only against an artifact this case cannot fabricate: a process exit the OS reaped, bytes the relay copied, or the peer process's own verdict."
}

func (m *manifest) produced(t *testing.T, era, mechanism string) []string {
	t.Helper()
	about := evidence.about(t.Name(), era, mechanism)
	if len(about) == 0 {
		return []string{fmt.Sprintf("nothing at all about the %s era's %q", era, mechanism)}
	}
	return about
}

func (m *manifest) status(era, mechanism string) string {
	entry := m.covered[era][mechanism]
	switch {
	case entry.redeemed == byEntailment:
		return statusEntailed
	case entry.absence == "" && entry.redeemed == byEvidence:
		return statusProven
	case entry.absence == "":
		return statusUnasserted
	case entry.redeemed == byEvidence:
		return statusAbsent
	default:
		return statusOpen
	}
}

func (m *manifest) finish(t *testing.T) {
	t.Helper()
	for _, era := range m.eras() {
		for _, mechanism := range m.mechanisms {
			if m.status(era, mechanism) == statusUnasserted {
				t.Errorf("%s era claims %q and no case in this gate redeems it", era, mechanism)
			}
		}
	}
	report := m.render()
	t.Log("\n" + report)
	if summary := os.Getenv(summaryEnv); summary != "" {
		if err := appendSummary(summary, report); err != nil {
			t.Errorf("append the coverage manifest to the job summary: %v", err)
		}
	}
	if record := os.Getenv(coverageEnv); record != "" {
		if err := os.WriteFile(record, []byte(m.record()), 0o644); err != nil {
			t.Errorf("write the coverage record scripts/mixed-era.sh gates on: %v", err)
		}
	}
}

func (m *manifest) record() string {
	var out strings.Builder
	fmt.Fprintf(&out, "subject\t%s\n", cutEra)
	for _, era := range m.eras() {
		fmt.Fprintf(&out, "peer\t%s\t%s\n", era, m.declared[era].Era)
	}
	for _, era := range m.eras() {
		for _, mechanism := range m.mechanisms {
			fmt.Fprintf(&out, "coverage\t%s\t%s\t%s\n", era, mechanism, m.status(era, mechanism))
		}
	}
	return out.String()
}

func (m *manifest) eras() []string {
	names := make([]string, 0, len(m.declared))
	for era := range m.declared {
		names = append(names, era)
	}
	sort.Strings(names)
	return names
}

func appendSummary(path, report string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(report + "\n"); err != nil {
		return errors.Join(err, file.Close())
	}
	return file.Close()
}

func (m *manifest) render() string {
	eras := m.eras()
	var out strings.Builder
	out.WriteString("### Mixed-era coverage\n\n")
	fmt.Fprintf(&out, "Boundary release `%s`. ", m.boundary)
	for _, era := range eras {
		declared := m.declared[era]
		fmt.Fprintf(&out, "The %s peer reports era `%s` at protocol %d. ", era, declared.Era, declared.Protocol)
	}
	out.WriteString("\n\n| mechanism |")
	for _, era := range eras {
		fmt.Fprintf(&out, " %s |", era)
	}
	out.WriteString("\n|---|")
	for range eras {
		out.WriteString("---|")
	}
	out.WriteString("\n")
	for _, mechanism := range m.mechanisms {
		fmt.Fprintf(&out, "| `%s` |", mechanism)
		for _, era := range eras {
			fmt.Fprintf(&out, " %s |", m.status(era, mechanism))
		}
		out.WriteString("\n")
	}
	if entailed := m.entailed(); len(entailed) > 0 {
		out.WriteString("\n**Entailed, not observed.** Nothing outside these claims' antecedents witnessed them.\n\n")
		for _, line := range entailed {
			fmt.Fprintf(&out, "- %s\n", line)
		}
	}
	if open := m.unproven(); len(open) > 0 {
		out.WriteString("\n**This run did not check everything it names.**\n\n")
		for _, line := range open {
			fmt.Fprintf(&out, "- %s\n", line)
		}
	}
	return out.String()
}

func (m *manifest) unproven() []string {
	var open []string
	for _, era := range m.eras() {
		for _, mechanism := range m.mechanisms {
			if m.status(era, mechanism) != statusOpen {
				continue
			}
			open = append(open, fmt.Sprintf("`%s` · `%s` — %s",
				era, mechanism, m.covered[era][mechanism].absence))
		}
	}
	return open
}

func (m *manifest) entailed() []string {
	var carried []string
	for _, era := range m.eras() {
		for _, mechanism := range m.mechanisms {
			if m.status(era, mechanism) != statusEntailed {
				continue
			}
			entry := m.covered[era][mechanism]
			carried = append(carried, fmt.Sprintf("`%s` · `%s` — entailed by `%s` · `%s`: %s",
				era, mechanism, entry.antecedent.era, entry.antecedent.mechanism, entry.absence))
		}
	}
	return carried
}
