package converge

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

func servingNone(context.Context) (wire.HealthReport, proc.Identity, error) {
	return wire.HealthReport{}, proc.Identity{}, nil
}

func recordsNone(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }

func TestObserveKeepsTheAttachRefusalVerbatim(t *testing.T) {
	refusal := errors.New("nothing is listening")
	world, err := Observe(t.Context(), Sources{
		Serving: func(context.Context) (wire.HealthReport, proc.Identity, error) {
			return wire.HealthReport{}, proc.Identity{}, refusal
		},
		Recorded: recordsNone,
	})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !errors.Is(world.Attach, refusal) {
		t.Fatalf("Attach = %v, want %v", world.Attach, refusal)
	}
	if world.Serving() {
		t.Fatal("Serving() = true for a refused attach")
	}
	if !reflect.DeepEqual(world.Health, wire.HealthReport{}) {
		t.Fatalf("Health = %+v, want zero", world.Health)
	}
}

func TestObserveReportsTheServedHealthAndOwnerRecord(t *testing.T) {
	report := wire.HealthReport{Phase: wire.PhaseReady, Protocol: wire.ProtocolVersion, Generation: 7, PID: 42, Build: "b"}
	pinned := proc.Identity{PID: 42, Start: 3, Boot: 9}
	owner := proc.Owner{PID: 42, Start: 3, Boot: 9, Generation: 7, Build: "b"}
	world, err := Observe(t.Context(), Sources{
		Serving: func(context.Context) (wire.HealthReport, proc.Identity, error) {
			return report, pinned, nil
		},
		Recorded:   func(string) (proc.Owner, bool, error) { return owner, true, nil },
		RecordPath: "/records",
	})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !world.Serving() {
		t.Fatalf("Serving() = false, attach = %v", world.Attach)
	}
	if !reflect.DeepEqual(world.Health, report) {
		t.Fatalf("Health = %+v, want %+v", world.Health, report)
	}
	if !world.Recorded || world.Owner != owner {
		t.Fatalf("Owner = %+v recorded = %v, want %+v true", world.Owner, world.Recorded, owner)
	}
	if world.Pinned != pinned {
		t.Fatalf("Pinned = %+v, want the identity the attach pinned %+v", world.Pinned, pinned)
	}
}

// TestObservedPrefersTheAttachPin pins which of an observation's two identities
// a later proof correlates against. The peer the attach pinned answered on the
// socket; the owner record beside it is writable, and on the pass that genuinely observed a live peer it may
// name nobody at all — an upgrade unlinks the daemon's bytes and its record with
// them — so an observation that handed the record down had nothing to hand.
func TestObservedPrefersTheAttachPin(t *testing.T) {
	pinned := proc.Identity{PID: 42, Start: 3, Boot: 9}
	owner := proc.Owner{PID: 77, Start: 5, Boot: 9, Generation: 7, Build: "b"}
	tests := []struct {
		name  string
		world World
		want  proc.Identity
	}{
		{
			name:  "the peer this attach pinned",
			world: World{Pinned: pinned, Owner: owner, Recorded: true},
			want:  pinned,
		},
		{
			name:  "the recorded incumbent when nothing served",
			world: World{Attach: errors.New("nothing is listening"), Owner: owner, Recorded: true},
			want:  owner.Identity(),
		},
		{
			name:  "the zero identity when neither named one",
			world: World{Attach: errors.New("nothing is listening")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.world.Observed(); got != tt.want {
				t.Fatalf("Observed() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestObservePassesTheRecordPathToTheReader(t *testing.T) {
	var seen string
	if _, err := Observe(t.Context(), Sources{
		Serving: servingNone,
		Recorded: func(path string) (proc.Owner, bool, error) {
			seen = path
			return proc.Owner{}, false, nil
		},
		RecordPath: "/state/daemon.records",
	}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if seen != "/state/daemon.records" {
		t.Fatalf("record path = %q, want %q", seen, "/state/daemon.records")
	}
}

func TestObserveFailsWhenTheRecordCannotBeRead(t *testing.T) {
	unreadable := errors.New("record file is corrupt")
	_, err := Observe(t.Context(), Sources{
		Serving:  servingNone,
		Recorded: func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, unreadable },
	})
	if !errors.Is(err, unreadable) {
		t.Fatalf("Observe() error = %v, want %v", err, unreadable)
	}
}

func TestObserveRequiresEveryObserver(t *testing.T) {
	tests := []struct {
		name    string
		sources Sources
	}{
		{"no serving observer", Sources{Recorded: recordsNone}},
		{"no record observer", Sources{Serving: servingNone}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Observe(t.Context(), tt.sources); err == nil {
				t.Fatal("Observe() error = nil, want a refusal")
			}
		})
	}
}
