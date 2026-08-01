//go:build mixedera

package coverage

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

// assertsMarker opens the clause a case carries when it redeems no mechanism,
// so a case that owes nothing says here what it asserts instead.
const assertsMarker = "asserts"

type claim struct {
	era       string
	mechanism string
}

// entailment is one claim observations.txt carries on another's artifact. Which
// pairs are allowed is not read from there: mechanisms.txt declares the relation
// and Bind refuses a claim that reaches outside it.
type entailment struct {
	consequent claim
	antecedent claim
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

// Observe records that this case ran. It marks the case's own line and nothing
// else: a claim's line is marked by the redemption that earned it, which is why
// a case emptied to bookkeeping cannot report its rows redeemed.
func Observe(t *testing.T) {
	t.Helper()
	observations.mark(caseEntry(t.Name()))
}

// Settle reports where this run and the frozen expectation disagree: the
// observations observations.txt demands that never happened — only when the run
// otherwise passed, since a failing run already says why — and the ones this run
// made that the file does not demand.
func Settle(passed bool) error {
	required := expected().owed
	var refusals []error
	if absent := observations.missing(required); passed && len(absent) > 0 {
		refusals = append(refusals, fmt.Errorf(
			"%d of %d observations %s requires never happened, so this run is not the matrix and carries no verdict:\n%s",
			len(absent), len(required), expectationPath, listed(absent),
		))
	}
	if extra := observations.beyond(required); len(extra) > 0 {
		refusals = append(refusals, fmt.Errorf(
			"this run observed what %s does not demand, so the frozen expectation no longer describes the matrix. Add each line below to that file, in sorted order, in the commit that adds the case producing it:\n%s",
			expectationPath, listed(extra),
		))
	}
	return errors.Join(refusals...)
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
	entailments map[string]entailment
}

func expected() expectation {
	owed, err := readExpectation()
	if err != nil {
		panic("mixed-era: " + err.Error())
	}
	return owed
}

func (e expectation) lists(entry string) bool { return e.owes[entry] }

func (e expectation) entailment(entry string) (entailment, bool) {
	carried, entailed := e.entailments[entry]
	return carried, entailed
}

func readExpectation() (expectation, error) {
	owed := expectation{owes: map[string]bool{}, entailments: map[string]entailment{}}
	var cases []string
	claiming, asserting := map[string]bool{}, map[string]bool{}
	for _, line := range FrozenLines(expectationFixture) {
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
			owed.entailments[entry] = entailment{
				consequent: claim{era: fields[2], mechanism: fields[3]},
				antecedent: claim{era: fields[5], mechanism: fields[6]},
			}
		default:
			return expectation{}, fmt.Errorf(
				"%s carries %q, which is none of \"case <test>\", \"case <test> %s <what it asserts>\", \"claim <test> <era> <mechanism> evidenced\", or \"claim <test> <era> <mechanism> entailed-by <era> <mechanism>\"",
				expectationPath, line, assertsMarker,
			)
		}
		if owed.owes[entry] {
			return expectation{}, fmt.Errorf("%s demands %q twice", expectationPath, entry)
		}
		owed.owes[entry] = true
		owed.owed = append(owed.owed, entry)
	}
	if len(owed.owed) == 0 {
		return expectation{}, fmt.Errorf("%s demands no observation at all", expectationPath)
	}
	if !slices.IsSorted(owed.owed) {
		return expectation{}, fmt.Errorf("%s is out of order; sort it so a new observation lands beside its neighbours instead of at the end", expectationPath)
	}
	if err := everyCaseRedeems(cases, claiming, asserting); err != nil {
		return expectation{}, err
	}
	return owed, nil
}

// entailedAsDeclared holds every entailment observations.txt demands to a
// relation mechanisms.txt declares. Entailment is the one redemption with no
// artifact under it, so what may carry what is not the claiming file's to say:
// the pair and the era it holds for are read from the frozen mechanism, and an
// entailment reaching outside that declaration is refused before the matrix
// runs. The polarities are already matched there, so a present fact cannot be
// named as an absence's antecedent.
func (e expectation) entailedAsDeclared(l lexicon) error {
	for _, entry := range e.owed {
		carried, entailed := e.entailments[entry]
		if !entailed {
			continue
		}
		held, frozen := l.named(carried.consequent.mechanism)
		if !frozen {
			return fmt.Errorf("%s carries %q, and %s names no mechanism %q",
				expectationPath, entry, MechanismPath, carried.consequent.mechanism)
		}
		declared, permitted := held.entailments[carried.consequent.era]
		switch {
		case !permitted:
			return fmt.Errorf("%s entails the %s era's %q by the %s era's %q, and %s declares that mechanism's %s era entailed by nothing. An entailment is a relation between two rows, and this gate reads that relation from the frozen file rather than from the claim: declare it there, in this same commit, or redeem the row against an artifact of its own:\n  entailed-by %s <mechanism>",
				expectationPath, carried.consequent.era, carried.consequent.mechanism,
				carried.antecedent.era, carried.antecedent.mechanism,
				MechanismPath, carried.consequent.era, carried.consequent.era)
		case carried.antecedent.era != carried.consequent.era:
			return fmt.Errorf("%s entails the %s era's %q by the %s era's %q, and %s declares that entailment within the %s era: one era's row does not follow from the other's",
				expectationPath, carried.consequent.era, carried.consequent.mechanism,
				carried.antecedent.era, carried.antecedent.mechanism,
				MechanismPath, carried.consequent.era)
		case declared != carried.antecedent.mechanism:
			return fmt.Errorf("%s entails the %s era's %q by %q, and %s declares it entailed by %q and nothing else",
				expectationPath, carried.consequent.era, carried.consequent.mechanism,
				carried.antecedent.mechanism, MechanismPath, declared)
		}
	}
	return nil
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
