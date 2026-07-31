package deploy

import (
	"debug/macho"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/yasyf/daemonkit/internal/proc"
)

// TestInventoryReadsTheRealProcessTable runs against the kernel, not a fake:
// the gate's whole value is that it observes what no record file can forge,
// so a test that mocked the table would assert nothing.
func TestInventoryReadsTheRealProcessTable(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	found, err := Inventory(resolved)
	if err != nil {
		t.Fatalf("Inventory(%q): %v", resolved, err)
	}
	if !slices.ContainsFunc(found.Live, func(p LiveProcess) bool { return p.PID == os.Getpid() }) {
		t.Fatalf("Inventory(%q) = %+v, want this test process", resolved, found.Live)
	}
	for _, process := range found.Live {
		if process.Executable != resolved {
			t.Fatalf("Inventory reported %q under %q", process.Executable, resolved)
		}
		if process.Start == 0 || process.Boot == 0 {
			t.Fatalf("Inventory reported %+v, want a pinned survivor", process)
		}
	}
	if !slices.IsSortedFunc(found.Live, func(a, b LiveProcess) int { return a.PID - b.PID }) {
		t.Fatal("Inventory returned unsorted survivors")
	}
}

// TestInventoryOverNothingIsEmpty is the gate's other failure mode, the one a
// process nothing can name used to cause: a machine carries long-lived husks
// under deleted binaries — a homebrew service, a helper whose bundle was
// upgraded out from under it — and attributing them to every query answers
// "live" for a path nothing runs from, forever. They come back apart from the
// answer, pinned and unattributed.
func TestInventoryOverNothingIsEmpty(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
	}{
		{"no paths", nil},
		{"a path nothing runs from", []string{filepath.Join(t.TempDir(), "absent")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, err := Inventory(tt.paths...)
			if err != nil {
				t.Fatalf("Inventory: %v", err)
			}
			if len(found.Live) != 0 {
				t.Fatalf("Inventory = %+v, want none", found.Live)
			}
			for _, husk := range found.Unnameable {
				if husk.Executable != "" {
					t.Fatalf("unnameable survivor = %+v, want no executable named", husk)
				}
				if husk.PID <= 0 || husk.Start == 0 || husk.Boot == 0 {
					t.Fatalf("unnameable survivor = %+v, want the whole pin", husk)
				}
			}
			if !slices.IsSortedFunc(found.Unnameable, func(a, b LiveProcess) int { return a.PID - b.PID }) {
				t.Fatal("Inventory returned unsorted unnameable survivors")
			}
		})
	}
}

// TestInventoryReportsOneHuskPerProcess pins the dedup every multi-path query
// needs: one process table is scanned per path, so a husk would otherwise be
// reported once for each path asked about.
func TestInventoryReportsOneHuskPerProcess(t *testing.T) {
	dir := t.TempDir()
	found, err := Inventory(filepath.Join(dir, "absent-one"), filepath.Join(dir, "absent-two"))
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	seen := make(map[int]int, len(found.Unnameable))
	for _, husk := range found.Unnameable {
		seen[husk.PID]++
		if seen[husk.PID] > 1 {
			t.Fatalf("Inventory reported pid %d %d times", husk.PID, seen[husk.PID])
		}
	}
}

func TestExecutablesCoversEveryAgentAndHostBinary(t *testing.T) {
	f := newFixture(t)
	host := filepath.Join(f.root, "hookd")
	f.deploy.config.Executables = []string{host, f.agent.Program}
	got, err := f.deploy.executables()
	if err != nil {
		t.Fatalf("executables: %v", err)
	}
	want := []string{f.agent.Program, host}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("executables() = %q, want %q", got, want)
	}
}

// TestExecutablesCoversTheBundlesOwnHelpers pins the half no declaration
// reaches: an in-bundle helper that is neither an agent nor a declared host
// binary would otherwise have its bundle deleted out from under it. The
// fixture writes the bundle's own executable as plain bytes, so the Mach-O
// helper is the only tree entry that joins the agent's Program — a shell
// script and a non-executable Mach-O are both nothing the kernel can report.
func TestExecutablesCoversTheBundlesOwnHelpers(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	helper := filepath.Join(f.app, "Contents", "Library", "LoginItems", "helper")
	writeMachO(t, helper, 0o755)
	writeMachO(t, filepath.Join(f.app, "Contents", "Resources", "unreadable-magic"), 0o644)
	if err := os.WriteFile(filepath.Join(f.app, "Contents", "Resources", "script"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := f.deploy.executables()
	if err != nil {
		t.Fatalf("executables: %v", err)
	}
	want := []string{f.agent.Program, helper}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("executables() = %q, want %q", got, want)
	}
}

// TestExecutablesCoversTheBundleMovedAside is the supersede blind spot's
// regression. Supersede renames the incumbent aside to the prior slot before
// it renames the candidate in, so the generation a process is still running
// sits at neither the canonical path nor anywhere the agents declare — and it
// is exactly that generation's bytes the next step destroys.
func TestExecutablesCoversTheBundleMovedAside(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	writeMachO(t, filepath.Join(f.app, "Contents", "Library", "LoginItems", "helper"), 0o755)
	if err := os.MkdirAll(filepath.Dir(f.deploy.layout.prior), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.app, f.deploy.layout.prior); err != nil {
		t.Fatal(err)
	}
	writeMachO(t, filepath.Join(f.deploy.layout.candidate, "Contents", "MacOS", "example"), 0o755)
	got, err := f.deploy.executables()
	if err != nil {
		t.Fatalf("executables: %v", err)
	}
	want := []string{
		f.agent.Program,
		filepath.Join(f.deploy.layout.prior, "Contents", "Library", "LoginItems", "helper"),
		filepath.Join(f.deploy.layout.candidate, "Contents", "MacOS", "example"),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("executables() = %q, want %q", got, want)
	}
}

// TestExecutablesCoversTheBundleMovedIntoTheRemovalSlot is the same blind spot
// on the uninstall arm. Uninstall renames the whole installed generation into
// the removal slot and destroys it there, and Reset destroys whatever an
// earlier pass left in that slot, so a process still running those bytes has to
// be visible to the gate that stands immediately before the removal.
func TestExecutablesCoversTheBundleMovedIntoTheRemovalSlot(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	writeMachO(t, filepath.Join(f.app, "Contents", "Library", "LoginItems", "helper"), 0o755)
	if err := os.MkdirAll(filepath.Dir(f.deploy.layout.removed), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.app, f.deploy.layout.removed); err != nil {
		t.Fatal(err)
	}
	got, err := f.deploy.executables()
	if err != nil {
		t.Fatalf("executables: %v", err)
	}
	want := []string{
		f.agent.Program,
		filepath.Join(f.deploy.layout.removed, "Contents", "Library", "LoginItems", "helper"),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("executables() = %q, want %q", got, want)
	}
}

func TestRequireEmptyNamesTheSurvivors(t *testing.T) {
	f := newFixture(t)
	f.live = []LiveProcess{{PID: 321, Start: 77, Boot: 9, Executable: f.agent.Program}}
	err := f.deploy.requireEmpty()
	if !errors.Is(err, ErrLive) {
		t.Fatalf("requireEmpty err = %v, want ErrLive", err)
	}
	if !isSentinel(err, errors.New("321")) || !isSentinel(err, errors.New(f.agent.Program)) {
		t.Fatalf("requireEmpty err = %v, want the surviving pid and executable named", err)
	}
	f.live = nil
	if err := f.deploy.requireEmpty(); err != nil {
		t.Fatalf("requireEmpty = %v, want nil", err)
	}
}

// TestRequireEmptyHoldsOnlyItsOwnUnnameableHusk is the gate's precision in both
// directions. Its own husk — the daemon whose bytes an upgrade unlinked — is in
// the owner record it wrote before binding, so the gate still refuses. The
// stranger's husk is in no record of this deployment, and a gate that counted
// it would never pass again on a machine that carries one.
func TestRequireEmptyHoldsOnlyItsOwnUnnameableHusk(t *testing.T) {
	tests := []struct {
		name    string
		record  bool
		husk    func(proc.Identity) LiveProcess
		wantErr error
	}{
		{
			name:    "the husk this deployment's daemon recorded",
			record:  true,
			husk:    func(id proc.Identity) LiveProcess { return LiveProcess{PID: id.PID, Start: id.Start, Boot: id.Boot} },
			wantErr: ErrLive,
		},
		{
			name:   "a husk this deployment never recorded",
			record: true,
			husk: func(id proc.Identity) LiveProcess {
				return LiveProcess{PID: id.PID + 1, Start: id.Start, Boot: id.Boot}
			},
		},
		{
			name:   "the recorded pid at an instance the record does not name",
			record: true,
			husk: func(id proc.Identity) LiveProcess {
				return LiveProcess{PID: id.PID, Start: id.Start + 1, Boot: id.Boot}
			},
		},
		{
			name:   "the recorded pin from another boot session",
			record: true,
			husk: func(id proc.Identity) LiveProcess {
				return LiveProcess{PID: id.PID, Start: id.Start, Boot: id.Boot + 1}
			},
		},
		{
			name: "a husk beside a daemon that recorded nothing",
			husk: func(proc.Identity) LiveProcess { return LiveProcess{PID: 4242, Start: 77, Boot: 9} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			var recorded proc.Identity
			if tt.record {
				recorded = f.recordOwner(t)
			}
			f.unnameable = []LiveProcess{tt.husk(recorded)}
			err := f.deploy.requireEmpty()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("requireEmpty = %v, want a clear gate", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("requireEmpty err = %v, want %v", err, tt.wantErr)
			}
			if !isSentinel(err, errors.New("unnameable")) || !isSentinel(err, errors.New(strconv.Itoa(recorded.PID))) {
				t.Fatalf("requireEmpty err = %v, want the surviving pin named", err)
			}
		})
	}
}

func writeMachO(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header, macho.Magic64)
	if err := os.WriteFile(path, header, mode); err != nil {
		t.Fatal(err)
	}
}
