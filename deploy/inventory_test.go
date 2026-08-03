package deploy

import (
	"debug/macho"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
		filepath.Join(f.deploy.layout.prior, "Contents", "MacOS", "example"),
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
		filepath.Join(f.deploy.layout.removed, "Contents", "MacOS", "example"),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("executables() = %q, want %q", got, want)
	}
}

// TestExecutablesCoversALeakedStagingTree is the last of the places a whole
// generation sits at. stage names its copy through MkdirTemp and publishes it
// only by rename, so a crash in between strands a full bundle copy under the
// metadata directory — bytes a process can be running and Reset destroys.
func TestExecutablesCoversALeakedStagingTree(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	leaked, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix)
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(leaked, "Contents", "MacOS", "example")
	writeMachO(t, helper, 0o755)
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

// TestBundleExecutablesClassifiesWhatASlotHolds pins the gate against the shape
// that wedged it: a generation slot that exists and is not a directory. Failing
// the whole inventory for it left every gated verb refusing — Reset included,
// which is the documented way out of a state no other verb accepts — so a plain
// file planted at a slot bricked the deployment for good. Such a slot carries no
// bundle, but it can still be an executable itself, and the next destroying step
// destroys it, so it answers for exactly its own bytes.
func TestBundleExecutablesClassifiesWhatASlotHolds(t *testing.T) {
	root := t.TempDir()
	slot := func(name string) string { return filepath.Join(root, name+".app") }
	bundled := slot("bundle")
	writeMachO(t, filepath.Join(bundled, "Contents", "MacOS", "example"), 0o755)
	plain := slot("plain")
	writeMachO(t, plain, 0o755)
	unreadable := slot("unreadable")
	writeMachO(t, unreadable, 0o644)
	script := slot("script")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	linked := slot("linked")
	if err := os.Symlink(bundled, linked); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		slot string
		want []string
	}{
		{"a slot nothing occupies", slot("absent"), nil},
		{"a bundle", bundled, []string{filepath.Join(bundled, "Contents", "MacOS", "example")}},
		{"a plain Mach-O executable", plain, []string{plain}},
		{"a Mach-O carrying no execute bit", unreadable, nil},
		{"an executable that is not Mach-O", script, nil},
		{"a symlink to a bundle", linked, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bundleExecutables(tt.slot)
			if err != nil {
				t.Fatalf("bundleExecutables(%q): %v", tt.slot, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("bundleExecutables(%q) = %q, want %q", tt.slot, got, tt.want)
			}
		})
	}
}

// TestGenerationSlotsNamesEveryBundleTheLayoutDerives guards the enumeration
// itself. Every layout path naming a bundle is a place a whole generation sits,
// so a slot added without reaching generationSlots is one the gate never scans
// and a destructive step still destroys — the hole the enumeration exists to
// close, reopened in silence.
func TestGenerationSlotsNamesEveryBundleTheLayoutDerives(t *testing.T) {
	f := newFixture(t)
	slots, err := f.deploy.generationSlots()
	if err != nil {
		t.Fatalf("generationSlots: %v", err)
	}
	derived := reflect.ValueOf(f.deploy.layout)
	for i := range derived.NumField() {
		path := derived.Field(i).String()
		if !strings.HasSuffix(path, stageSuffix) || slices.Contains(slots, path) {
			continue
		}
		t.Errorf("layout.%s = %q names a bundle generationSlots() does not: %q",
			derived.Type().Field(i).Name, path, slots)
	}
}

func TestRequireEmptyNamesTheSurvivors(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	child := f.live(f.agent.Program)
	err := f.deploy.requireEmpty()
	if !errors.Is(err, ErrLive) {
		t.Fatalf("requireEmpty err = %v, want ErrLive", err)
	}
	pid := child.Process.Pid
	if !isSentinel(err, errors.New(strconv.Itoa(pid))) || !isSentinel(err, errors.New(f.agent.Program)) {
		t.Fatalf("requireEmpty err = %v, want pid %d and %q named", err, pid, f.agent.Program)
	}
	f.settle(child, f.agent.Program)
	if err := f.deploy.requireEmpty(); err != nil {
		t.Fatalf("requireEmpty = %v, want nil", err)
	}
}

// TestRequireEmptyHoldsOnlyItsOwnUnnameableHusk is the gate's precision in both
// directions, against husks the kernel really cannot name — a child of this
// binary whose executable was unlinked out from under it. Its own husk is in
// the owner record it wrote before binding, so the gate still refuses. The
// stranger's husk is in no record of this deployment, and a gate that counted
// it would never pass again on a machine that carries one.
func TestRequireEmptyHoldsOnlyItsOwnUnnameableHusk(t *testing.T) {
	t.Run("the husk this deployment's daemon recorded", func(t *testing.T) {
		f := newFixture(t)
		pid := f.husk(true)
		err := f.deploy.requireEmpty()
		if !errors.Is(err, ErrLive) {
			t.Fatalf("requireEmpty err = %v, want ErrLive", err)
		}
		if !isSentinel(err, errors.New("unnameable")) || !isSentinel(err, errors.New(strconv.Itoa(pid))) {
			t.Fatalf("requireEmpty err = %v, want the surviving pin named", err)
		}
	})
	t.Run("a husk this deployment never recorded", func(t *testing.T) {
		f := newFixture(t)
		recorded := f.recordOwner(t)
		pid := f.husk(false)
		if pid == recorded.PID {
			t.Fatalf("the husk took the recorded pid %d", pid)
		}
		if err := f.deploy.requireEmpty(); err != nil {
			t.Fatalf("requireEmpty = %v, want a clear gate", err)
		}
	})
	t.Run("a husk beside a daemon that recorded nothing", func(t *testing.T) {
		f := newFixture(t)
		f.husk(false)
		if err := f.deploy.requireEmpty(); err != nil {
			t.Fatalf("requireEmpty = %v, want a clear gate", err)
		}
	})
}

// TestAttributedComparesTheWholePin is the correlation the husk gate rests on,
// held to inputs no live process can be made to take: the kernel hands a pid to
// a stranger the moment its owner leaves, and a boot counter only moves across
// a reboot. Both are reachable as values and neither is reachable as a process,
// so this is where they are covered.
func TestAttributedComparesTheWholePin(t *testing.T) {
	recorded := proc.Identity{PID: 4242, Start: 77, Boot: 9}
	tests := []struct {
		name string
		husk LiveProcess
		want bool
	}{
		{"the recorded pin", LiveProcess{PID: 4242, Start: 77, Boot: 9}, true},
		{"another pid at the recorded instance", LiveProcess{PID: 4243, Start: 77, Boot: 9}, false},
		{"the recorded pid at an instance the record does not name", LiveProcess{PID: 4242, Start: 78, Boot: 9}, false},
		{"the recorded pin from another boot session", LiveProcess{PID: 4242, Start: 77, Boot: 10}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Survivors{Unnameable: []LiveProcess{tt.husk}}.attributed(recorded)
			if (len(got) == 1) != tt.want {
				t.Fatalf("attributed(%+v) = %+v, want attributed=%v", tt.husk, got, tt.want)
			}
		})
	}
	if got := (Survivors{Unnameable: []LiveProcess{{PID: 4242, Start: 77, Boot: 9}}}).attributed(); len(got) != 0 {
		t.Fatalf("attributed() over an empty record = %+v, want none", got)
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
