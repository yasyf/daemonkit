//go:build mixedera

package mixedera

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type evidenceKind string

const (
	fromProcessTable evidenceKind = "a process exit the OS reaped"
	fromWire         evidenceKind = "bytes the relay copied"
	fromPeerVerdict  evidenceKind = "the peer process's own verdict"
)

type fact struct {
	present bool
	kind    evidenceKind
	detail  string
}

func (f fact) String() string {
	happened := "it happened"
	if !f.present {
		happened = "it did not happen"
	}
	return fmt.Sprintf("%s — %s: %s", happened, f.kind, f.detail)
}

type journal struct {
	mu   sync.Mutex
	held map[string][]fact
}

var evidence = journal{held: map[string][]fact{}}

func factKey(name, era, mechanism string) string {
	return strings.Join([]string{name, era, mechanism}, "\x00")
}

func (j *journal) record(t *testing.T, era, mechanism string, seen fact) {
	t.Helper()
	j.mu.Lock()
	defer j.mu.Unlock()
	key := factKey(t.Name(), era, mechanism)
	j.held[key] = append(j.held[key], seen)
	t.Logf("evidence: %s era %s — %s", era, mechanism, seen)
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

func observedPresent(t *testing.T, era, mechanism string, kind evidenceKind, detail string) {
	t.Helper()
	evidence.record(t, era, mechanism, fact{present: true, kind: kind, detail: detail})
}

func observedAbsent(t *testing.T, era, mechanism string, kind evidenceKind, detail string) {
	t.Helper()
	evidence.record(t, era, mechanism, fact{present: false, kind: kind, detail: detail})
}
