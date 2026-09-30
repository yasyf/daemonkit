package proc

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// copyOf stages a private copy of a system binary, so a test can unlink or
// replace the file a running process was executed from.
func copyOf(t *testing.T, source string) string {
	t.Helper()
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(dir, "program")
	if err := os.WriteFile(program, body, 0o700); err != nil {
		t.Fatal(err)
	}
	return program
}

// TestExecutablePathSurvivesAnInPlaceUpgrade is the inventory's fail-closed
// contract on linux. An upgrade renames new bytes over a running daemon's
// program, which unlinks the inode it runs and has the kernel report the path
// as deleted; the daemon must still be named by the path it was executed from,
// or the inventory over that path reads clear with the daemon still running.
func TestExecutablePathSurvivesAnInPlaceUpgrade(t *testing.T) {
	program := copyOf(t, "/bin/sleep")
	pid := suspendedChild(t, program)
	if got, err := ExecutablePath(pid); err != nil || got != program {
		t.Fatalf("ExecutablePath() = %q, %v, want %q", got, err, program)
	}
	replacement := program + ".next"
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, program); err != nil {
		t.Fatal(err)
	}
	if got, err := ExecutablePath(pid); err != nil || got != program {
		t.Fatalf("ExecutablePath() after the program was replaced = %q, %v, want %q", got, err, program)
	}
	report, err := ExecutableIdentities(program)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", program, err)
	}
	if !slices.ContainsFunc(report.Matched, func(id Identity) bool { return id.PID == pid }) {
		t.Fatalf("Matched = %+v, want the process still running the replaced program, pid %d", report.Matched, pid)
	}
}

// TestExecutablePathSurvivesAnUpgradeThatKeepsAnotherLink replaces a program
// whose inode a second name still holds. The kernel marks the path deleted for
// the replaced directory entry, not for the inode's link count, so a link left
// elsewhere must not make the marker read as part of the name.
func TestExecutablePathSurvivesAnUpgradeThatKeepsAnotherLink(t *testing.T) {
	program := copyOf(t, "/bin/sleep")
	if err := os.Link(program, program+".kept"); err != nil {
		t.Fatal(err)
	}
	pid := suspendedChild(t, program)
	replacement := program + ".next"
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, program); err != nil {
		t.Fatal(err)
	}
	if got, err := ExecutablePath(pid); err != nil || got != program {
		t.Fatalf("ExecutablePath() after the program was replaced = %q, %v, want %q", got, err, program)
	}
}

// TestExecutablePathKeepsANameThatEndsInTheMarker runs a program whose own
// name ends in the kernel's deleted marker.
func TestExecutablePathKeepsANameThatEndsInTheMarker(t *testing.T) {
	staged := copyOf(t, "/bin/sleep")
	program := staged + deletedSuffix
	if err := os.Rename(staged, program); err != nil {
		t.Fatal(err)
	}
	pid := suspendedChild(t, program)
	if got, err := ExecutablePath(pid); err != nil || got != program {
		t.Fatalf("ExecutablePath() = %q, %v, want the literal name %q", got, err, program)
	}
}

// TestExecutablePathReportsAnUnlinkedProgramAsUnnameable is the other half: a
// process whose program is gone for good runs from a path that names nothing,
// so it is reported beside its pin rather than matched or dropped.
func TestExecutablePathReportsAnUnlinkedProgramAsUnnameable(t *testing.T) {
	program := copyOf(t, "/bin/sleep")
	pid := suspendedChild(t, program)
	if err := os.Remove(program); err != nil {
		t.Fatal(err)
	}
	if got, err := ExecutablePath(pid); !errors.Is(err, errUnresolvedExecutable) {
		t.Fatalf("ExecutablePath() of an unlinked program = %q, %v, want errUnresolvedExecutable", got, err)
	}
	report, err := ExecutableIdentities(program)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", program, err)
	}
	names := func(id Identity) bool { return id.PID == pid }
	if slices.ContainsFunc(report.Matched, names) {
		t.Fatalf("Matched = %+v, want pid %d unattributed", report.Matched, pid)
	}
	if !slices.ContainsFunc(report.Unnameable, names) {
		t.Fatalf("Unnameable = %+v, want pid %d reported rather than dropped", report.Unnameable, pid)
	}
}

// TestProcessIDsHoldsTheSameEUIDFloor pins the trust floor where the scan's
// population is decided, against the owner each process's own status reports.
func TestProcessIDsHoldsTheSameEUIDFloor(t *testing.T) {
	pids, err := processIDs()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(pids, os.Getpid()) {
		t.Fatalf("processIDs() returned %d pids, want this process among them", len(pids))
	}
	for _, pid := range pids {
		uid, err := effectiveUID(pid)
		if errors.Is(err, errNoProc) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if uid != os.Geteuid() {
			t.Fatalf("processIDs() returned pid %d owned by uid %d, want the same-euid floor", pid, uid)
		}
	}
	foreign := foreignPID(t)
	if slices.Contains(pids, foreign) {
		t.Fatalf("processIDs() returned pid %d, another user's process", foreign)
	}
}

// TestExecutablePathRefusesAnotherUsersProcess is the floor at the read
// itself: another user's process is neither an error that aborts a scan nor a
// survivor counted against the gate.
func TestExecutablePathRefusesAnotherUsersProcess(t *testing.T) {
	pid := foreignPID(t)
	if got, err := ExecutablePath(pid); !errors.Is(err, errForeignProcess) {
		t.Fatalf("ExecutablePath(%d) = %q, %v, want errForeignProcess", pid, got, err)
	}
	read, err := readExecutable(pid)
	if err != nil || read.state != execSkipped {
		t.Fatalf("readExecutable(%d) = %+v, %v, want it skipped", pid, read, err)
	}
}

// foreignPID names a live user process owned by another uid. The floor is
// unprovable without one, so a host where this uid owns every process fails
// here rather than passing on nothing.
func foreignPID(t *testing.T) int {
	t.Helper()
	table, err := tablePIDs()
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range table {
		if pid <= 1 {
			continue
		}
		stat, err := readStat(pid)
		if err != nil || stat.zombie() || stat.flags&taskKernelThread != 0 {
			continue
		}
		uid, err := effectiveUID(pid)
		if err != nil || uid == os.Geteuid() {
			continue
		}
		return pid
	}
	t.Fatal("no process owned by another user; the euid floor is unprovable without one (run the suite as an unprivileged user beside at least one process it does not own)")
	return 0
}
