//go:build mixedera

// Package coverage holds the mixed-era release gate's verdict: what each
// mechanism name denotes, which observations the matrix owes, the journal every
// claim is redeemed against, and the coverage record scripts/mixed-era.sh reads
// a release out of.
//
// It is a package rather than more files beside the harness because the object
// a release is gated on used to be state the harness could assign. Nothing that
// decides a verdict is exported: not a coverage row, not the ledger of
// observations, not the evidence journal, not the map of eras the record is
// rendered from. So the moves that used to clear this gate from inside a case
// body — writing a row redeemed, marking a claim observed, deleting the subject
// era, filing a fact past the witness binding, rewriting the seal — are compile
// errors rather than audits.
//
// What is exported is the redemption verbs, which decide for themselves against
// this journal; the manifest the shell gate and the job summary read; and the
// readers for the frozen files, sealed here so the text a run reads is the text
// it started on.
//
// TODO(phase 2): the record is still written by the run that produces it, and a
// process that can write files can write that one. A verdict no participant can
// forge needs a separate binary that reads the artifacts — process exits,
// captured bytes, peer output — and forms the verdict outside the run.
package coverage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PrecutEra and CutEra name the two release lines this matrix drives at each
// other.
const (
	PrecutEra = "precut"
	CutEra    = "cut"
)

const (
	// MechanismPath is where the frozen mechanism file sits in the tree, so a
	// refusal names the file its reader has to edit.
	MechanismPath = "ci/mixedera/" + frozenDir + "/" + mechanismFixture

	expectationPath = "ci/mixedera/" + frozenDir + "/" + expectationFixture

	frozenDir          = "testdata/frozen"
	mechanismFixture   = "mechanisms.txt"
	expectationFixture = "observations.txt"
)

// sealed is every frozen file as Bind read it. Nothing here holds a parsed
// lexicon or expectation between reads: each read goes back to the file and is
// refused unless the bytes are still the ones sealed, and the seal is reachable
// from nowhere else.
var sealed map[string]string

// Destinations is where a finished run's account of itself goes: the record
// scripts/mixed-era.sh re-derives a release verdict from, and the job summary a
// reader gets. Either empty is that destination skipped, which is both of them
// off CI.
type Destinations struct {
	Record  string
	Summary string
}

var bound Destinations

// Bind seals the frozen files this gate reads, takes the destinations a finished
// manifest is written to, and settles everything those files declare before the
// matrix runs: what each mechanism name denotes, who may witness it, how each era
// disposes of it, what may carry what by entailment, and every observation the
// matrix owes. A run binds once — a second seal is how a run would come to read
// text it did not start on.
func Bind(into Destinations) error {
	if sealed != nil {
		return errors.New("this run already sealed its frozen files; sealing again is how the text a run reads comes to differ from the text it started on")
	}
	held, err := sealFrozen()
	if err != nil {
		return fmt.Errorf("seal the frozen files this gate reads: %w", err)
	}
	sealed, bound = held, into
	claims, err := readMechanisms()
	if err != nil {
		return fmt.Errorf("read what this gate's mechanism names denote: %w", err)
	}
	owed, err := readExpectation()
	if err != nil {
		return fmt.Errorf("read the observations this gate requires: %w", err)
	}
	return owed.entailedAsDeclared(claims)
}

// FrozenLines is one frozen fixture's content lines, blank lines and comments
// dropped, refused unless the file on disk is still the one Bind sealed.
func FrozenLines(name string) []string {
	var lines []string
	for line := range strings.Lines(frozenText(name)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func sealFrozen() (map[string]string, error) {
	entries, err := os.ReadDir(frozenDir)
	if err != nil {
		return nil, err
	}
	held := map[string]string{}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(frozenDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		held[entry.Name()] = string(raw)
	}
	if len(held) == 0 {
		return nil, fmt.Errorf("%s carries no frozen file at all", frozenDir)
	}
	return held, nil
}

func frozenText(name string) string {
	raw, err := os.ReadFile(filepath.Join(frozenDir, name))
	if err != nil {
		panic(fmt.Sprintf("mixed-era: read the frozen %s: %v", name, err))
	}
	held, known := sealed[name]
	switch {
	case !known:
		panic(fmt.Sprintf("mixed-era: %s/%s is not one of the frozen files this run sealed", frozenDir, name))
	case string(raw) != held:
		panic(fmt.Sprintf("mixed-era: %s/%s is no longer the file this run sealed, so what this gate reads is not what the tree carries", frozenDir, name))
	}
	return held
}

func listed(entries []string) string { return "  " + strings.Join(entries, "\n  ") }
