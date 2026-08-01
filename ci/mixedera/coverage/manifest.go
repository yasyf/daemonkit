//go:build mixedera

package coverage

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
)

// StatusProven and StatusOpen are the ends of the record's vocabulary: a row
// something redeemed against an artifact, and a row this release still owes.
const (
	StatusProven = "PROVEN"
	StatusOpen   = "UNPROVEN"
)

const (
	statusAbsent     = "ABSENT"
	statusEntailed   = "ENTAILED"
	statusUnasserted = "UNASSERTED"
)

// Redemption is why a coverage row counts as answered.
type Redemption int

// The ways a row is redeemed. Nothing outside this package sets one on a row:
// Redeem decides between them against the journal.
const (
	Unredeemed Redemption = iota
	ByEvidence
	ByEntailment
)

// StatusOf is the status one coverage row reports: what its era's peer declared
// about the mechanism, and what redeemed it. It is the whole of the classifier
// scripts/mixed-era.sh re-derives a release verdict from.
func StatusOf(absence string, redeemed Redemption) string {
	switch {
	case redeemed == ByEntailment:
		return statusEntailed
	case absence == "" && redeemed == ByEvidence:
		return StatusProven
	case absence == "":
		return statusUnasserted
	case redeemed == ByEvidence:
		return statusAbsent
	default:
		return StatusOpen
	}
}

// verdict is one era peer's account of one mechanism: the frozen proposition it
// quotes back, and why that proposition does not hold for it, empty when it
// does.
type verdict struct {
	Proposition string `json:"proposition"`
	Absence     string `json:"absence,omitempty"`
}

type conformance struct {
	Era        string             `json:"era"`
	Protocol   uint16             `json:"protocol"`
	Mechanisms map[string]verdict `json:"mechanisms"`
}

type row struct {
	absence    string
	redeemed   Redemption
	antecedent claim
}

// Declaration is one era peer's conformance as that peer's own process printed
// it: the era the harness built it for, and the JSON it answered with.
type Declaration struct {
	Era  string
	JSON string
}

// Manifest is this run's coverage matrix: which mechanisms each era claims,
// what has redeemed each row so far, and the record the release gate reads.
type Manifest struct {
	boundary string
	declared map[string]conformance
	covered  map[string]map[string]*row
}

// NewManifest freezes the matrix this run answers: every era peer is held to the
// proposition and the disposition mechanisms.txt declares for each frozen
// mechanism, and every row starts unredeemed.
//
// Which eras a manifest covers is not the caller's to choose. Every era that
// file disposes of has to be declared here, exactly once, because a manifest
// missing an era renders a record with no row of that era to refuse — the empty
// record being the shape of a release gate that passes by having nothing to say.
func NewManifest(t *testing.T, boundary string, declared ...Declaration) *Manifest {
	t.Helper()
	m := &Manifest{
		boundary: boundary,
		declared: map[string]conformance{},
		covered:  map[string]map[string]*row{},
	}
	for _, given := range declared {
		if _, twice := m.declared[given.Era]; twice {
			t.Fatalf("the %s era is declared twice: a manifest covers one peer per era", given.Era)
		}
		var reported conformance
		if err := json.Unmarshal([]byte(given.JSON), &reported); err != nil {
			t.Fatalf("decode %s era conformance %q: %v", given.Era, given.JSON, err)
		}
		if reported.Era == "" {
			t.Fatalf("%s era peer names no era", given.Era)
		}
		m.declared[given.Era] = reported
		m.covered[given.Era] = map[string]*row{}
		for _, frozen := range denoted().ordered {
			answered, accounted := reported.Mechanisms[frozen.name]
			if !accounted {
				t.Fatalf("%s era peer accounts for no %q: every frozen mechanism carries a verdict",
					given.Era, frozen.name)
			}
			if answered.Proposition != frozen.proposition {
				t.Fatalf("the %s era peer answers a different question about %q than %s freezes, so its column is not about that mechanism at all:\n  frozen:   %s\n  declared: %s",
					given.Era, frozen.name, MechanismPath, frozen.proposition, answered.Proposition)
			}
			assertDisposition(t, given.Era, frozen, answered)
			m.covered[given.Era][frozen.name] = &row{absence: answered.Absence}
		}
		for name := range reported.Mechanisms {
			if _, frozen := m.covered[given.Era][name]; !frozen {
				t.Fatalf("%s era peer reports %q, which this gate does not assert", given.Era, name)
			}
		}
	}
	for _, era := range disposedEras() {
		if _, covered := m.covered[era]; !covered {
			t.Fatalf("%s disposes of a %s era and this manifest declares no peer for it, so the record it renders carries no %s row for the release gate to refuse",
				MechanismPath, era, era)
		}
	}
	return m
}

// disposedEras is every era mechanisms.txt answers for, read back out of that
// file rather than named here, so the set of columns a record owes is the frozen
// text's to say.
func disposedEras() []string {
	held := map[string]bool{}
	for _, frozen := range denoted().ordered {
		for era := range frozen.dispositions {
			held[era] = true
		}
	}
	return slices.Sorted(maps.Keys(held))
}

// assertDisposition holds an era peer to the disposition mechanisms.txt freezes
// for it. The proposition binds only what a name means; without the disposition
// beside it a peer drops an Absence field on its own — the whole of the edit
// that turns an UNPROVEN row PROVEN — and the frozen file stays byte-identical
// while the row it exists to keep red goes green.
func assertDisposition(t *testing.T, era string, frozen mechanism, answered verdict) {
	t.Helper()
	switch {
	case answered.Absence == "" && frozen.absent(era):
		t.Fatalf("the %s era peer claims %q, and %s disposes of that era as %s. A peer does not promote its own row: flip the disposition there, in this same commit, or restore the absence the peer dropped",
			era, frozen.name, MechanismPath, dispositionAbsent)
	case answered.Absence != "" && !frozen.absent(era):
		t.Fatalf("the %s era peer declares %q absent and %s disposes of that era as %s:\n  %s",
			era, frozen.name, MechanismPath, dispositionClaimed, answered.Absence)
	}
}

// Redeem trades a case's claim for evidence the harness observed while the case
// ran. The claim is never itself proof: it only names a mechanism, and the
// frozen expectation decides what redeems that name.
func (m *Manifest) Redeem(t *testing.T, era string, mechanisms ...string) {
	t.Helper()
	for _, mechanism := range mechanisms {
		m.redeemOne(t, era, mechanism)
	}
}

func (m *Manifest) redeemOne(t *testing.T, era, mechanism string) {
	t.Helper()
	asked := claim{era: era, mechanism: mechanism}
	entry, frozen := m.covered[era][mechanism]
	if !frozen {
		t.Fatalf("no frozen mechanism %q in the %s era", mechanism, era)
	}
	owed := claimEntry(t.Name(), asked)
	demanded := expected()
	if !demanded.lists(owed) {
		t.Fatalf("%s redeems the %s era's %q, which %s does not demand. Add this line to that file, in sorted order, in this same commit:\n  %s evidenced",
			t.Name(), era, mechanism, expectationPath, owed)
	}
	if carried, entailed := demanded.entailment(owed); entailed {
		m.entail(t, owed, entry, asked, carried.antecedent)
		return
	}
	held, found := evidence.holds(t.Name(), era, mechanism, entry.absence == "")
	if !found {
		t.Errorf("%s claims the %s era's %q and nothing outside the case's own bookkeeping observed it. %s\nThis case produced:\n%s",
			t.Name(), era, mechanism, m.wanted(era, mechanism), listed(m.produced(t, era, mechanism)))
		return
	}
	t.Logf("redeemed the %s era's %q against %s", era, mechanism, held)
	entry.redeemed = ByEvidence
	observations.mark(owed)
}

// entail carries a claim on another claim's artifact, for the one shape with no
// artifact of its own: a mechanism whose absence follows from the absence of the
// thing it would gate. Which row may carry which, and at what polarity, is
// declared in mechanisms.txt and settled before the matrix runs; what is left
// here is that the antecedent was redeemed by evidence this same case produced,
// so entailment cannot reach across the matrix to borrow a proof.
func (m *Manifest) entail(t *testing.T, owed string, entry *row, asked, antecedent claim) {
	t.Helper()
	backing := m.covered[antecedent.era][antecedent.mechanism]
	_, held := evidence.holds(t.Name(), antecedent.era, antecedent.mechanism, backing.absence == "")
	if backing.redeemed != ByEvidence || !held {
		t.Errorf("%s claims the %s era's %q is entailed by the %s era's %q, and this case redeemed no evidence of that antecedent. This case produced:\n%s",
			t.Name(), asked.era, asked.mechanism, antecedent.era, antecedent.mechanism,
			listed(m.produced(t, antecedent.era, antecedent.mechanism)))
		return
	}
	entry.redeemed, entry.antecedent = ByEntailment, antecedent
	observations.mark(owed)
}

func (m *Manifest) wanted(era, mechanism string) string {
	if m.covered[era][mechanism].absence != "" {
		return fmt.Sprintf(
			"The %s peer declares %q absent, so this claim needs an observation that it did NOT happen — a daemon the OS never reaped, or a connection the daemon closed unanswered.",
			era, mechanism,
		)
	}
	return "A claim is redeemable only against an artifact this case cannot fabricate: a process exit the OS reaped, bytes the relay copied, or the peer process's own verdict."
}

func (m *Manifest) produced(t *testing.T, era, mechanism string) []string {
	t.Helper()
	about := evidence.about(t.Name(), era, mechanism)
	if len(about) == 0 {
		return []string{fmt.Sprintf("nothing at all about the %s era's %q", era, mechanism)}
	}
	return about
}

func (m *Manifest) status(era, mechanism string) string {
	entry := m.covered[era][mechanism]
	return StatusOf(entry.absence, entry.redeemed)
}

// Finish reports the matrix: a claimed row no case redeemed takes the run red,
// and the record the release gate reads is written from the rows as they stand,
// to the destinations Bind was given.
func (m *Manifest) Finish(t *testing.T) {
	t.Helper()
	for _, era := range m.eras() {
		for _, frozen := range denoted().ordered {
			if m.status(era, frozen.name) == statusUnasserted {
				t.Errorf("%s era claims %q and no case in this gate redeems it", era, frozen.name)
			}
		}
	}
	report := m.render()
	t.Log("\n" + report)
	if bound.Summary != "" {
		if err := appendSummary(bound.Summary, report); err != nil {
			t.Errorf("append the coverage manifest to the job summary: %v", err)
		}
	}
	if bound.Record != "" {
		if err := os.WriteFile(bound.Record, []byte(m.record()), 0o600); err != nil {
			t.Errorf("write the coverage record scripts/mixed-era.sh gates on: %v", err)
		}
	}
}

func (m *Manifest) record() string {
	var out strings.Builder
	fmt.Fprintf(&out, "subject\t%s\n", CutEra)
	for _, era := range m.eras() {
		fmt.Fprintf(&out, "peer\t%s\t%s\n", era, m.declared[era].Era)
	}
	for _, era := range m.eras() {
		for _, frozen := range denoted().ordered {
			fmt.Fprintf(&out, "coverage\t%s\t%s\t%s\n", era, frozen.name, m.status(era, frozen.name))
		}
	}
	return out.String()
}

func (m *Manifest) eras() []string {
	return slices.Sorted(maps.Keys(m.declared))
}

func appendSummary(path, report string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(report + "\n"); err != nil {
		return errors.Join(err, file.Close())
	}
	return file.Close()
}

func (m *Manifest) render() string {
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
	for _, frozen := range denoted().ordered {
		fmt.Fprintf(&out, "| `%s` |", frozen.name)
		for _, era := range eras {
			fmt.Fprintf(&out, " %s |", m.status(era, frozen.name))
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

func (m *Manifest) unproven() []string {
	var open []string
	for _, era := range m.eras() {
		for _, frozen := range denoted().ordered {
			if m.status(era, frozen.name) != StatusOpen {
				continue
			}
			open = append(open, fmt.Sprintf("`%s` · `%s` — %s",
				era, frozen.name, m.covered[era][frozen.name].absence))
		}
	}
	return open
}

func (m *Manifest) entailed() []string {
	var carried []string
	for _, era := range m.eras() {
		for _, frozen := range denoted().ordered {
			if m.status(era, frozen.name) != statusEntailed {
				continue
			}
			entry := m.covered[era][frozen.name]
			carried = append(carried, fmt.Sprintf("`%s` · `%s` — entailed by `%s` · `%s`: %s",
				era, frozen.name, entry.antecedent.era, entry.antecedent.mechanism, entry.absence))
		}
	}
	return carried
}
