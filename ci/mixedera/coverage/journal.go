//go:build mixedera

package coverage

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// journalling is this file's own path from a witness into the journal, skipped
// so a fact is attributed to the function that produced it.
var journalling = map[string]bool{
	"witnessed":         true,
	"(*journal).record": true,
	"ObservedPresent":   true,
	"ObservedAbsent":    true,
}

type fact struct {
	present bool
	kind    EvidenceKind
	detail  string
}

func (f fact) String() string {
	happened := "it happened"
	if !f.present {
		happened = "it did not happen"
	}
	return fmt.Sprintf("%s — %s: %s", happened, artifacts[f.kind], f.detail)
}

type journal struct {
	mu   sync.Mutex
	held map[string][]fact
}

var evidence = journal{held: map[string][]fact{}}

// ObservedPresent files a fact the harness observed happening. It reaches the
// journal only from the site mechanisms.txt reserves for that mechanism's
// evidence of that class, read off the call stack rather than taken from the
// caller's word.
func ObservedPresent(t *testing.T, era, mechanism string, kind EvidenceKind, detail string) {
	t.Helper()
	evidence.record(t, era, mechanism, fact{present: true, kind: kind, detail: detail})
}

// ObservedAbsent files a fact the harness observed not happening, under the same
// witness binding ObservedPresent files under: an absence costs what a presence
// costs.
func ObservedAbsent(t *testing.T, era, mechanism string, kind EvidenceKind, detail string) {
	t.Helper()
	evidence.record(t, era, mechanism, fact{present: false, kind: kind, detail: detail})
}

func factKey(name, era, mechanism string) string {
	return strings.Join([]string{name, era, mechanism}, "\x00")
}

func (j *journal) record(t *testing.T, era, mechanism string, seen fact) {
	t.Helper()
	witnessed(t, mechanism, seen.kind)
	j.mu.Lock()
	defer j.mu.Unlock()
	key := factKey(t.Name(), era, mechanism)
	j.held[key] = append(j.held[key], seen)
	t.Logf("evidence: %s era %s — %s", era, mechanism, seen)
}

func witnessed(t *testing.T, mechanism string, kind EvidenceKind) {
	t.Helper()
	if err := Reserves(filedBy(), mechanism, kind); err != nil {
		t.Fatal(err)
	}
}

func filedBy() string {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if site := siteName(frame.Function); !journalling[site] {
			return site
		}
		if !more {
			return "an unattributable site"
		}
	}
}

// siteName strips a runtime frame's package qualifier, leaving the function as
// the harness's own source spells it: witnessVerdict, or
// (*daemonProc).witnessPreamble for a method.
func siteName(qualified string) string {
	if slash := strings.LastIndex(qualified, "/"); slash >= 0 {
		qualified = qualified[slash+1:]
	}
	if dot := strings.Index(qualified, "."); dot >= 0 {
		return qualified[dot+1:]
	}
	return qualified
}

func (j *journal) holds(name, era, mechanism string, present bool) (fact, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, held := range j.held[factKey(name, era, mechanism)] {
		if held.present == present {
			return held, true
		}
	}
	return fact{}, false
}

func (j *journal) about(name, era, mechanism string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	about := j.held[factKey(name, era, mechanism)]
	lines := make([]string, 0, len(about))
	for _, held := range about {
		lines = append(lines, held.String())
	}
	return lines
}
