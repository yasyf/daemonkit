//go:build mixedera

package mixedera

// This gate catches accidental narrowing: a run filtered down by -run, a case
// deleted or built out, a requirement orphaned by a rename, a case body emptied
// while its claims stay behind, a peer built against the wrong era. It does not
// catch a case that deliberately feeds the observers fabricated inputs —
// observedPresent and observedAbsent are package-level and every case body can
// call them directly. TODO(phase 2): move the verdict out of the run, into a
// separate binary that reads artifacts the run cannot forge.

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
)

const (
	expectationFixture = "observations.txt"
	expectationPath    = "ci/mixedera/testdata/frozen/" + expectationFixture

	// assertsMarker opens the clause a case carries when it redeems no
	// mechanism, so a case that owes nothing says here what it asserts instead.
	assertsMarker = "asserts"
)

type claim struct {
	era       string
	mechanism string
}

type ledger struct {
	mu   sync.Mutex
	seen map[string]bool
}

var observations = ledger{seen: map[string]bool{}}

func (l *ledger) mark(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen[entry] = true
}

func (l *ledger) missing(required []string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var absent []string
	for _, entry := range required {
		if !l.seen[entry] {
			absent = append(absent, entry)
		}
	}
	return absent
}

func (l *ledger) beyond(required []string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var extra []string
	for entry := range l.seen {
		if !slices.Contains(required, entry) {
			extra = append(extra, entry)
		}
	}
	slices.Sort(extra)
	return extra
}

func observe(t *testing.T) {
	t.Helper()
	observations.mark(caseEntry(t.Name()))
}

func caseEntry(name string) string { return "case " + name }

func claimEntry(name string, asserted claim) string {
	return fmt.Sprintf("claim %s %s %s", name, asserted.era, asserted.mechanism)
}

// expectation is the set of observations the matrix owes, read from a
// git-tracked file so that no edit inside the run can shrink it.
type expectation struct {
	owed        []string
	owes        map[string]bool
	entailments map[string]claim
}

var expected *expectation

func (e *expectation) lists(entry string) bool { return e.owes[entry] }

func (e *expectation) entailment(entry string) (claim, bool) {
	antecedent, entailed := e.entailments[entry]
	return antecedent, entailed
}

func readExpectation() (*expectation, error) {
	lines, err := readFrozen(expectationFixture)
	if err != nil {
		return nil, err
	}
	owed := &expectation{owes: map[string]bool{}, entailments: map[string]claim{}}
	var cases []string
	claiming, asserting := map[string]bool{}, map[string]bool{}
	for _, line := range lines {
		fields := strings.Fields(line)
		var entry string
		switch {
		case len(fields) == 2 && fields[0] == "case":
			entry = line
			cases = append(cases, fields[1])
		case len(fields) > 3 && fields[0] == "case" && fields[2] == assertsMarker:
			entry = strings.Join(fields[:2], " ")
			cases = append(cases, fields[1])
			asserting[fields[1]] = true
		case len(fields) == 5 && fields[0] == "claim" && fields[4] == "evidenced":
			entry = strings.Join(fields[:4], " ")
			claiming[fields[1]] = true
		case len(fields) == 7 && fields[0] == "claim" && fields[4] == "entailed-by":
			entry = strings.Join(fields[:4], " ")
			claiming[fields[1]] = true
			owed.entailments[entry] = claim{era: fields[5], mechanism: fields[6]}
		default:
			return nil, fmt.Errorf(
				"%s carries %q, which is none of \"case <test>\", \"case <test> %s <what it asserts>\", \"claim <test> <era> <mechanism> evidenced\", or \"claim <test> <era> <mechanism> entailed-by <era> <mechanism>\"",
				expectationPath, line, assertsMarker,
			)
		}
		if owed.owes[entry] {
			return nil, fmt.Errorf("%s demands %q twice", expectationPath, entry)
		}
		owed.owes[entry] = true
		owed.owed = append(owed.owed, entry)
	}
	if !slices.IsSorted(owed.owed) {
		return nil, fmt.Errorf("%s is out of order; sort it so a new observation lands beside its neighbours instead of at the end", expectationPath)
	}
	if err := everyCaseRedeems(cases, claiming, asserting); err != nil {
		return nil, err
	}
	return owed, nil
}

// everyCaseRedeems holds each listed case to a claim or to a written reason it
// has none, so deleting a case's claims — the edit that empties a case body
// while the coverage table stays whole, because other cases reprove the same
// mechanisms — is a visible change to the expectation rather than an absence.
func everyCaseRedeems(cases []string, claiming, asserting map[string]bool) error {
	for _, name := range cases {
		switch {
		case claiming[name] && asserting[name]:
			return fmt.Errorf("%s says %s redeems nothing and then demands a claim from it; drop the %q clause or drop the claim",
				expectationPath, name, assertsMarker)
		case !claiming[name] && !asserting[name]:
			return fmt.Errorf("%s demands %s and no claim from it, so emptying that case's body would go unreported. Either restore the claims it redeems or write down what it asserts instead:\n  case %s %s <what it asserts>",
				expectationPath, name, name, assertsMarker)
		}
	}
	return nil
}

func listed(entries []string) string { return "  " + strings.Join(entries, "\n  ") }

func TestMain(m *testing.M) {
	owed, err := readExpectation()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mixed-era: read the observations this gate requires: %v\n", err)
		os.Exit(1)
	}
	expected = owed

	code := m.Run()
	if absent := observations.missing(expected.owed); code == 0 && len(absent) > 0 {
		fmt.Fprintf(os.Stderr,
			"mixed-era: %d of %d observations %s requires never happened, so this run is not the matrix and carries no verdict:\n%s\n",
			len(absent), len(expected.owed), expectationPath, listed(absent))
		code = 1
	}
	if extra := observations.beyond(expected.owed); len(extra) > 0 {
		fmt.Fprintf(os.Stderr,
			"mixed-era: this run observed what %s does not demand, so the frozen expectation no longer describes the matrix. Add each line below to that file, in sorted order, in the commit that adds the case producing it:\n%s\n",
			expectationPath, listed(extra))
		code = 1
	}
	os.Exit(code)
}
