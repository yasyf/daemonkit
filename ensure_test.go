package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

func TestEnsureRequiresDeadline(t *testing.T) {
	client := openClient(t, Daemon{Label: "com.example.ensure"})
	if _, err := client.Ensure(context.Background()); err == nil {
		t.Fatal("Ensure() without a deadline succeeded")
	}
}

// TestEnsureNamesAnUnsetProgram refuses a Daemon no constructor built a Program
// for where the cause is still nameable. Past this point the ladder holds an
// empty path and an empty build, and reports a missing file or an unpinned
// incumbent instead of the field that was never set.
func TestEnsureNamesAnUnsetProgram(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := openClient(t, Daemon{Label: "com.example.ensure"}).Ensure(ctx)
	if err == nil || !strings.Contains(err.Error(), "Daemon.Program is unset") {
		t.Fatalf("Ensure() error = %v, want the unset Program named", err)
	}
}

func TestWaitReadyRequiresDeadline(t *testing.T) {
	client := openClient(t, Daemon{Label: "com.example.ensure"})
	if _, err := client.WaitReady(context.Background()); err == nil {
		t.Fatal("WaitReady() without a deadline succeeded")
	}
	control := &Control{}
	if _, err := control.WaitReady(context.Background()); err == nil {
		t.Fatal("Control.WaitReady() without a deadline succeeded")
	}
}

// TestProgramBuildIsTheContentDigestServePublishes pins the launcher's half of
// every upgrade comparison to the daemon's. Ensure wants the build the Program
// carries and the daemon publishes buildDigest of its own executable, so the
// two are the same function of the same bytes or no decision between them can
// ever come out equal.
func TestProgramBuildIsTheContentDigestServePublishes(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())
	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	carried, err := program.build()
	if err != nil {
		t.Fatalf("build() error = %v", err)
	}
	published, err := buildDigest()
	if err != nil {
		t.Fatalf("buildDigest() error = %v", err)
	}
	if carried != published {
		t.Fatalf("Program.build = %q, the daemon publishes %q", carried, published)
	}
}

// TestLabelIsRefusedUnlessLaunchdWouldAcceptIt pins the one rule to launchd's
// own. The Label names a LaunchAgent before it names a directory, a lock, a
// socket, or a binary copy, so the strictest reading of it is the only reading:
// a leading dot, a trailing dot, an embedded "..", and anything outside
// launchd's alphabet are refused here, and the rule this package runs is
// launchd's own rather than one beside it that could disagree.
func TestLabelIsRefusedUnlessLaunchdWouldAcceptIt(t *testing.T) {
	refused := []Label{
		"", ".", "..", ".hidden", "trailing.", "com.example..daemon",
		"../daemon", "bin/daemon", "/daemon", "daemon/", "com example", "com.example.daemon\n",
	}
	for _, label := range refused {
		t.Run(string(label), func(t *testing.T) {
			el, err := label.element()
			if err == nil {
				t.Fatalf("element() = %q, want %q refused", el.label, label)
			}
			if err := launchd.ValidateLabel(string(label)); err == nil {
				t.Fatalf("launchd.ValidateLabel(%q) accepted what daemonkit refused: the two rules disagree", label)
			}
		})
	}
	for _, label := range []Label{"com.example.daemon", "dkt", "with-dash", "a.b-c.9"} {
		t.Run(string(label), func(t *testing.T) {
			el, err := label.element()
			if err != nil {
				t.Fatalf("element() error = %v, want %q accepted", err, label)
			}
			if el.label != string(label) {
				t.Fatalf("element = %q, want %q verbatim", el.label, label)
			}
		})
	}
}

// TestNoPathIsJoinedFromALabelLaunchdWouldRefuse is the class the per-policy
// check is one instance of, driven as the cross product it is: every exported
// verb that takes a Daemon, against every shape of Label launchd refuses — a
// leading dot, a trailing dot, an embedded "..", a traversal out of the state
// root, a second path element, a byte outside launchd's alphabet, and no label
// at all. The whole tree the home sits two directories inside is compared byte
// for byte after each verb, so a state directory, a lock, a socket, a record
// file, a binary copy, or a plist created anywhere under it — or above it, where
// a traversal lands — fails the door that created it.
//
// Every client verb is reached through Open, which runs the rule, so Open is
// the door the verbs stand behind. RecordPath is the one derivation that
// states the layout without running the rule, so it is driven here for what it
// must not do: it names an escaped path and reads nothing at it.
func TestNoPathIsJoinedFromALabelLaunchdWouldRefuse(t *testing.T) {
	root := escapeRoot(t)
	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	settled := treeOf(t, root)

	for _, bad := range []Label{
		"", ".", "..", ".hidden", "trailing.", "com.example..daemon", "../../evil",
		"../daemon", "bin/daemon", "/daemon", "daemon/", "com example", "com.example.daemon\n",
	} {
		t.Run(fmt.Sprintf("%q", string(bad)), func(t *testing.T) {
			daemon := Daemon{Label: bad, Program: program, Trust: Trust{Serving: ServingSameUser()}}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()

			doors := []struct {
				name    string
				refuses bool
				run     func() error
			}{
				{"Open", true, func() error { _, err := Open(daemon); return err }},
				{"Serve", true, func() error {
					_, err := Serve(ctx, daemon, func(Ctx) (Product, error) { return nil, nil })
					return err
				}},
				{"ValidateForServe", true, daemon.ValidateForServe},
				{"ValidateForClient", true, daemon.ValidateForClient},
				{"Daemon.agent", true, func() error { _, err := daemon.agent(); return err }},
				{"RecordPath", false, func() error {
					record := daemon.RecordPath()
					if _, err := os.Stat(record); !errors.Is(err, fs.ErrNotExist) {
						return fmt.Errorf("stat %q = %v", record, err)
					}
					return nil
				}},
			}
			for _, door := range doors {
				t.Run(door.name, func(t *testing.T) {
					err := door.run()
					switch {
					case door.refuses && (err == nil || !strings.Contains(err.Error(), "is not canonical")):
						t.Errorf("%s() error = %v, want the label refused by launchd's own rule", door.name, err)
					case !door.refuses && err != nil:
						t.Errorf("%s() error = %v", door.name, err)
					}
					if got := treeOf(t, root); !reflect.DeepEqual(got, settled) {
						t.Errorf("%s() changed %q: %v", door.name, root, treeDelta(settled, got))
						settled = got
					}
				})
			}
		})
	}
}

// escapeRoot stands the passwd home up two directories inside a root the test
// owns, so a Label that traverses out of the state root lands somewhere the
// walk still sees rather than in a parent nobody can assert on.
func escapeRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-%d-", os.Getpid()))
	if err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	home := filepath.Join(root, "s", "h")
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	t.Setenv(realhome.EnvOverride, home)
	return root
}

func treeOf(t *testing.T, root string) map[string]string {
	t.Helper()
	tree := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		switch {
		case entry.IsDir():
			tree[rel] = info.Mode().String()
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			tree[rel] = info.Mode().String() + " -> " + target
		default:
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			tree[rel] = info.Mode().String() + " " + digest(data)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", root, err)
	}
	return tree
}

func treeDelta(before, after map[string]string) []string {
	var delta []string
	for path, state := range after {
		switch was, known := before[path]; {
		case !known:
			delta = append(delta, "+ "+path+" "+state)
		case was != state:
			delta = append(delta, "~ "+path+" "+was+" -> "+state)
		}
	}
	for path := range before {
		if _, known := after[path]; !known {
			delta = append(delta, "- "+path)
		}
	}
	slices.Sort(delta)
	return delta
}

func TestDaemonAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	program := filepath.Join(home, "bin", "daemon")
	tests := []struct {
		name    string
		daemon  Daemon
		want    launchd.Agent
		refused bool
	}{
		{
			name: "every field derives from the daemon",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Args:    []string{"daemon", "--serve"},
				Log:     filepath.Join(home, "custom.log"),
				Restart: RestartAlways,
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				Args:          []string{"daemon", "--serve"},
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.RestartAlways,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "the shutdown grace is the plist's exit timeout",
			daemon: Daemon{
				Label:    "com.example.ensure",
				Program:  Program{policy: bundled{file: program}},
				Log:      filepath.Join(home, "custom.log"),
				Shutdown: Grace(90 * time.Second),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   90 * time.Second,
			},
		},
		{
			name: "a sub-second grace rounds up rather than cutting the drain short",
			daemon: Daemon{
				Label:    "com.example.ensure",
				Program:  Program{policy: bundled{file: program}},
				Log:      filepath.Join(home, "custom.log"),
				Shutdown: Grace(1500 * time.Millisecond),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   2 * time.Second,
			},
		},
		{
			name: "an unset log sinks to the state directory",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Restart: RestartOnFailure,
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "com.example.ensure", "daemon.log"),
				RestartPolicy: launchd.RestartOnFailure,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "the zero restart never relaunches",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Log:     filepath.Join(home, "custom.log"),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "an unknown restart policy is refused",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Restart: Restart(9),
			},
			refused: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, err := tt.daemon.agent()
			if tt.refused {
				if err == nil {
					t.Fatal("agent() error = nil, want a refusal")
				}
				return
			}
			if err != nil {
				t.Fatalf("agent() error = %v", err)
			}
			if agent.Label != tt.want.Label || agent.Program != tt.want.Program ||
				agent.LogPath != tt.want.LogPath || agent.RestartPolicy != tt.want.RestartPolicy ||
				agent.ExitTimeOut != tt.want.ExitTimeOut {
				t.Fatalf("agent() = %+v, want %+v", agent, tt.want)
			}
			if len(agent.Args) != len(tt.want.Args) {
				t.Fatalf("agent() args = %q, want %q", agent.Args, tt.want.Args)
			}
			for i, arg := range tt.want.Args {
				if agent.Args[i] != arg {
					t.Fatalf("agent() args = %q, want %q", agent.Args, tt.want.Args)
				}
			}
		})
	}
}

func TestRepairWedgedAddressesTheRecordedIdentity(t *testing.T) {
	recorded, owner := settleFixture(t)
	path := recorded.RecordPath()
	noRecord := filepath.Join(t.TempDir(), "absent.records")
	readErr := errors.New("record file is corrupt")
	tests := []struct {
		name       string
		recordPath string
		target     incumbent
		readOwner  func(string) (proc.Owner, bool, error)
		probe      func(int) (proc.Identity, error)
		killErr    error
		wantSignal bool
		wantErr    error
	}{
		{
			name:       "a record naming another build is not signalled",
			recordPath: path,
			target:     incumbent{build: "b2", generation: owner.Generation},
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "a record naming another instance is not signalled",
			recordPath: path,
			target:     incumbent{build: owner.Build, generation: owner.Generation + 1},
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "no owner record names nobody to signal",
			recordPath: noRecord,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrUnrecorded,
		},
		{
			name:       "an unreadable record propagates",
			recordPath: path,
			readOwner:  func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, readErr },
			probe:      proc.ProbeIdentity,
			wantErr:    readErr,
		},
		{
			name:       "a departed incumbent is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      func(int) (proc.Identity, error) { return proc.Identity{}, proc.ErrNoProcess },
		},
		{
			name:       "a reused pid is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe: func(pid int) (proc.Identity, error) {
				return proc.Identity{PID: pid, Start: owner.Start + 1, Boot: owner.Boot}, nil
			},
		},
		{
			name:       "a cross-boot pid is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe: func(pid int) (proc.Identity, error) {
				return proc.Identity{PID: pid, Start: owner.Start, Boot: owner.Boot + 1}, nil
			},
		},
		{
			name:       "the matching identity is terminated",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantSignal: true,
		},
		{
			name:       "a race to exit is not a failure",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			killErr:    syscall.ESRCH,
			wantSignal: true,
		},
		{
			name:       "a refused signal is reported",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			killErr:    syscall.EPERM,
			wantSignal: true,
			wantErr:    syscall.EPERM,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signalled := 0
			kill := func(pid int, sig syscall.Signal) error {
				signalled++
				if pid != owner.PID {
					t.Fatalf("signalled pid %d, want the recorded %d", pid, owner.PID)
				}
				if sig != syscall.SIGTERM {
					t.Fatalf("signalled %v, want SIGTERM", sig)
				}
				return tt.killErr
			}
			target := tt.target
			if target == (incumbent{}) {
				target = incumbent{build: owner.Build, generation: owner.Generation}
			}
			err := repairWedged(tt.recordPath, target, tt.readOwner, tt.probe, kill)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("repairWedged() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("repairWedged() error = %v", err)
			}
			if want := map[bool]int{true: 1, false: 0}[tt.wantSignal]; signalled != want {
				t.Fatalf("delivered %d signals, want %d", signalled, want)
			}
		})
	}
}

// realPath is a path in the form the kernel reports an executable, which is
// what an executable-scoped inventory compares against.
func realPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", path, err)
	}
	return resolved
}

func selfPath(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}
	return self
}

// liveAt answers the executable-scoped inventory as the kernel would for a
// process table holding exactly one process, running live. Every other path is
// clear, so a gate that queries the wrong path is caught rather than covered by
// whatever else the machine happens to be running.
func liveAt(live string) func(string) (proc.Report, error) {
	return func(path string) (proc.Report, error) {
		if path != live {
			return proc.Report{}, nil
		}
		return proc.Report{Matched: []proc.Identity{{PID: 4242, Start: 1, Boot: 1, Executable: path}}}, nil
	}
}

// huskAt answers the inventory as the kernel would for a process table whose
// only live process is one nothing can name: no query path matches it, and its
// pin is all there is to go on.
func huskAt(pin proc.Identity) func(string) (proc.Report, error) {
	return func(string) (proc.Report, error) {
		return proc.Report{Unnameable: []proc.Identity{pin}}, nil
	}
}

const inventoryLabel = Label("com.example.inventory")

func TestInventoryClearProvesAbsenceOverTheProcessTable(t *testing.T) {
	unrun := filepath.Join(t.TempDir(), "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	idle := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: unrun}}})
	idle.identities = liveAt(realPath(t, selfPath(t)))
	if err := idle.inventoryClear(proc.Identity{}); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory", err)
	}
	running := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: realPath(t, selfPath(t))}}})
	if err := running.inventoryClear(proc.Identity{}); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("inventoryClear() over this very process = %v, want ErrUnsettled", err)
	}
}

// TestInventoryClearHoldsOnlyItsOwnUnnameableHusk is the gate's precision in
// both directions. A live process nothing could name belongs to this daemon
// exactly when it is one the ladder observed on its way here: its own husk — a
// daemon whose binary was unlinked under it — still refuses, while the
// long-lived stranger every machine carries is not this daemon's to answer for
// and cannot brick the gate forever.
func TestInventoryClearHoldsOnlyItsOwnUnnameableHusk(t *testing.T) {
	unrun := filepath.Join(t.TempDir(), "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	husk := proc.Identity{PID: 4242, Start: 77, Boot: 9}
	tests := []struct {
		name     string
		observed proc.Identity
		wantErr  error
	}{
		{
			name:     "the husk this ladder observed",
			observed: husk,
			wantErr:  ErrUnsettled,
		},
		{
			name: "a pass that observed nothing",
		},
		{
			name:     "an observation naming another instance at the same pid",
			observed: proc.Identity{PID: husk.PID, Start: husk.Start + 1, Boot: husk.Boot},
		},
		{
			name:     "an observation from another boot session",
			observed: proc.Identity{PID: husk.PID, Start: husk.Start, Boot: husk.Boot + 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: unrun}}})
			client.identities = huskAt(husk)
			err := client.inventoryClear(tt.observed)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("inventoryClear() = %v, want a clear inventory", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("inventoryClear() = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "pid 4242 (unnameable, start 77, boot 9)") {
				t.Fatalf("inventoryClear() = %v, want the surviving pin named", err)
			}
		})
	}
}

// TestInventoryClearQueriesTheProgramPathAlone pins the query set to this
// daemon's own program. Every daemonkit consumer places its program under one
// shared root, so a sibling there is another product's daemon by construction
// and a gate that queried one would hold that product's live daemon against
// this one. What covers a live build this path does not name is the recorded
// identity, not a guessed path.
func TestInventoryClearQueriesTheProgramPathAlone(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	label := Label("com.example.mine")
	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	if _, err := program.place(mustElement(t, label)); err != nil {
		t.Fatalf("place() error = %v", err)
	}
	stranger := filepath.Join(home, ".daemonkit", "bin", "com.example.other")
	if err := os.WriteFile(stranger, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("place the stranger: %v", err)
	}
	var queried []string
	client := openClient(t, Daemon{Label: label, Program: program})
	client.identities = func(path string) (proc.Report, error) {
		queried = append(queried, path)
		return proc.Report{}, nil
	}
	if err := client.inventoryClear(proc.Identity{}); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory", err)
	}
	if want := []string{realPath(t, programPath(t, client.daemon))}; !slices.Equal(queried, want) {
		t.Fatalf("inventoryClear queried %q, want %q", queried, want)
	}
}

// TestInventoryClearNeverPassesOnAnUnresolvedProgram is the fail-open gate's
// regression: the kernel reports a fully symlink-resolved executable, so an
// unresolved program path matches nothing and reports a clear inventory for a
// process that is very much running. os.Executable() is exactly such a path on
// darwin — /var/folders/… for a binary the kernel calls /private/var/folders/….
func TestInventoryClearNeverPassesOnAnUnresolvedProgram(t *testing.T) {
	self := selfPath(t)
	linked := filepath.Join(t.TempDir(), "daemon")
	if err := os.Symlink(realPath(t, self), linked); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	tests := []struct {
		name    string
		program string
		wantErr error
	}{
		{"the unresolved path of this very process", self, ErrUnsettled},
		{"a symlink to this very process", linked, ErrUnsettled},
		{"a program that resolves to nothing", filepath.Join(t.TempDir(), "absent"), os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: tt.program}}})
			client.identities = liveAt(realPath(t, self))
			if err := client.inventoryClear(proc.Identity{}); !errors.Is(err, tt.wantErr) {
				t.Fatalf("inventoryClear() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestLaunchctlReportsExitCodesAsAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, code, err := launchctl(ctx, "/bin/sh", "-c", "echo spoke; exit 37")
	if err != nil {
		t.Fatalf("launchctl() error = %v, want an exit code instead", err)
	}
	if code != 37 {
		t.Fatalf("launchctl() code = %d, want 37", code)
	}
	if out != "spoke\n" {
		t.Fatalf("launchctl() out = %q, want %q", out, "spoke\n")
	}
	_, code, err = launchctl(ctx, filepath.Join(t.TempDir(), "absent"))
	if err == nil {
		t.Fatal("launchctl() of a missing binary reported no error")
	}
	if code >= 0 {
		t.Fatalf("launchctl() that never ran reported status %d, want no status at all", code)
	}
}

func TestAttachCadenceDerivesFromTheCallerDeadline(t *testing.T) {
	tests := []struct {
		name    string
		budget  time.Duration
		wantMax time.Duration
	}{
		{"a generous budget is capped", time.Hour, maxObservationCadence},
		{"a tight budget is a fraction of itself", 640 * time.Millisecond, 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.budget)
			defer cancel()
			if got := attachCadence(ctx); got <= 0 || got > tt.wantMax {
				t.Fatalf("attachCadence() = %v, want (0, %v]", got, tt.wantMax)
			}
		})
	}
}

func TestAttachCadenceStaysPositiveOnAnExpiredContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	if got := attachCadence(ctx); got <= 0 {
		t.Fatalf("attachCadence() = %v, want a positive interval", got)
	}
}

// ladderHome stands up the passwd home every launchd path derives from, with
// the LaunchAgents directory a plist is written into already present. It is
// short because the daemon socket underneath it must fit sun_path.
func ladderHome(t *testing.T) string {
	t.Helper()
	home := shortHome(t)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	return home
}

// ladderAgent is the one LaunchAgent an Ensure ladder converges on. Its program
// is a real symlink-free executable: launchd refuses to consider an agent
// applied whose program is not one, and every temp dir on darwin sits behind
// the /var symlink.
func ladderAgent(t *testing.T, home string) launchd.Agent {
	t.Helper()
	return launchd.Agent{
		Label:         "com.example.ladder",
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(home, "daemon.log"),
		RestartPolicy: launchd.NoRestart,
		ExitTimeOut:   30 * time.Second,
	}
}

// launchctlRecorder answers launchctl and records every verb and every target,
// so a test can assert both what the ladder asked launchd to do and that it
// named no label but its own.
type launchctlRecorder struct {
	loaded  bool
	refuse  string
	verbs   []string
	targets []string
}

func (r *launchctlRecorder) run(_ context.Context, _ string, args ...string) (string, int, error) {
	r.verbs = append(r.verbs, args[0])
	r.targets = append(r.targets, args[len(args)-1])
	switch {
	case args[0] == "print" && !r.loaded:
		return "Could not find service", 3, errors.New("exit status 3")
	case args[0] == r.refuse:
		return args[0] + " failed: 1: Operation not permitted", 1, errors.New("exit status 1")
	}
	return "", 0, nil
}

func servedReport(phase wire.Phase, build string, generation uint64) wire.HealthReport {
	return wire.HealthReport{
		Phase:      phase,
		Protocol:   wire.ProtocolVersion,
		Generation: generation,
		PID:        4242,
		Build:      build,
	}
}

type observation struct {
	report wire.HealthReport
	pinned proc.Identity
	err    error
}

// servingScript answers each observation from the script in turn and repeats
// its last entry, so a test states only the transitions it cares about.
func servingScript(script []observation, seen *int) func(context.Context) (wire.HealthReport, proc.Identity, error) {
	return func(context.Context) (wire.HealthReport, proc.Identity, error) {
		step := script[min(*seen, len(script)-1)]
		*seen++
		return step.report, step.pinned, step.err
	}
}

func TestSettleObservesUntilTheDecisionIsKnowable(t *testing.T) {
	const want = "wanted"
	tests := []struct {
		name        string
		budget      time.Duration
		script      []observation
		wantAction  Action
		wantServing bool
		wantSeen    int
		wantErr     error
	}{
		{
			name:       "an absent listener is a start",
			script:     []observation{{err: ErrAbsent}},
			wantAction: ActionStarted,
			wantSeen:   1,
		},
		{
			name:       "a draining incumbent is a start",
			script:     []observation{{err: ErrDraining}},
			wantAction: ActionStarted,
			wantSeen:   1,
		},
		{
			name:     "an untrusted server is returned, never waited out",
			script:   []observation{{err: ErrUntrusted}},
			wantSeen: 1,
			wantErr:  ErrUntrusted,
		},
		{
			name:        "the wanted build, ready, is nothing",
			script:      []observation{{report: servedReport(wire.PhaseReady, want, 7)}},
			wantAction:  ActionNothing,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name:        "another build is an upgrade whatever its phase",
			script:      []observation{{report: servedReport(wire.PhaseStarting, "stale", 7)}},
			wantAction:  ActionUpgraded,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name:        "a failed runtime is a restart",
			script:      []observation{{report: servedReport(wire.PhaseFailed, want, 7)}},
			wantAction:  ActionRestarted,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name: "a starting incumbent is re-observed until it settles",
			script: []observation{
				{report: servedReport(wire.PhaseStarting, want, 7)},
				{report: servedReport(wire.PhaseStarting, want, 7)},
				{report: servedReport(wire.PhaseReady, want, 7)},
			},
			wantAction:  ActionNothing,
			wantServing: true,
			wantSeen:    3,
		},
		{
			name:        "an incumbent still transitioning at the end of the share is replaced",
			budget:      40 * time.Millisecond,
			script:      []observation{{report: servedReport(wire.PhaseDraining, want, 7)}},
			wantAction:  ActionRestarted,
			wantServing: true,
		},
		{
			name:     "an incumbent that cannot name itself is refused",
			script:   []observation{{report: servedReport(wire.PhaseReady, want, 0)}},
			wantSeen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := ladderHome(t)
			agent := ladderAgent(t, home)
			seen := 0
			client := openClient(t, Daemon{Label: Label(agent.Label)})
			client.serving = servingScript(tt.script, &seen)
			client.readOwner = func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }
			client.launchctl = (&launchctlRecorder{}).run
			budget := tt.budget
			if budget == 0 {
				budget = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			world, action, err := client.settle(ctx, want, agent)
			if tt.wantErr != nil || tt.wantAction == actionInvalid {
				if err == nil {
					t.Fatalf("settle() action = %v, want a refusal", action)
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Fatalf("settle() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("settle() error = %v", err)
			}
			if action != tt.wantAction {
				t.Fatalf("settle() action = %v, want %v", action, tt.wantAction)
			}
			if world.Serving() != tt.wantServing {
				t.Fatalf("settle() serving = %v, want %v", world.Serving(), tt.wantServing)
			}
			if tt.wantSeen != 0 && seen != tt.wantSeen {
				t.Fatalf("settle() observed %d times, want %d", seen, tt.wantSeen)
			}
		})
	}
}

// ensureOnceHarness drives the whole ladder against injected boundaries: the
// socket observation, the owner record, the process table, the signal, and
// launchctl. Nothing it stands up can block past the caller's budget.
type ensureOnceHarness struct {
	client      *Client
	agent       launchd.Agent
	launchd     *launchctlRecorder
	signals     []int
	inventoried []string
	settling    bool
}

func newEnsureOnceHarness(t *testing.T, serving []observation, owners func(string) (proc.Owner, bool, error)) *ensureOnceHarness {
	t.Helper()
	home := ladderHome(t)
	unrun := filepath.Join(home, "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	h := &ensureOnceHarness{launchd: &launchctlRecorder{}}
	h.agent = ladderAgent(t, home)
	h.client = openClient(t, Daemon{Label: Label(h.agent.Label), Program: Program{policy: bundled{file: unrun}}})
	seen := 0
	h.client.serving = servingScript(serving, &seen)
	h.client.readOwner = owners
	h.client.probe = func(pid int) (proc.Identity, error) { return proc.Identity{PID: pid, Start: 1, Boot: 1}, nil }
	h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
		if h.settling {
			return 0, false, nil
		}
		return proc.ReapAbsent, true, nil
	}
	h.client.identities = func(path string) (proc.Report, error) {
		h.inventoried = append(h.inventoried, path)
		return proc.Report{}, nil
	}
	h.client.kill = func(pid int, sig syscall.Signal) error {
		if sig != syscall.SIGTERM {
			t.Fatalf("delivered %v, want SIGTERM", sig)
		}
		h.signals = append(h.signals, pid)
		return nil
	}
	h.client.launchctl = h.launchd.run
	return h
}

// installPlist writes the exact plist launchd would read for the harness agent,
// which is half of what makes an agent applied; the other half is the recorder
// reporting the job loaded.
func (h *ensureOnceHarness) installPlist(t *testing.T) {
	t.Helper()
	plist, err := h.agent.Plist()
	if err != nil {
		t.Fatalf("Plist() error = %v", err)
	}
	path, err := h.agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
}

// driftPlist installs the harness agent's plist with one byte appended: still
// daemonkit's own job at the label, no longer the bytes this agent renders.
func (h *ensureOnceHarness) driftPlist(t *testing.T) {
	t.Helper()
	h.installPlist(t)
	path, err := h.agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
}

func recordedOwner(build string, generation uint64) proc.Owner {
	return proc.Owner{PID: 4242, Start: 1, Boot: 1, Generation: generation, Build: build}
}

func TestEnsureOnceDoesNothingWhenTheWantedBuildIsReadyAndApplied(t *testing.T) {
	h := newEnsureOnceHarness(
		t,
		[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
		func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
	)
	h.launchd.loaded = true
	h.installPlist(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ensured, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
	if err != nil {
		t.Fatalf("ensureOnce() error = %v", err)
	}
	if ensured.Did != ActionNothing {
		t.Fatalf("Did = %v, want %v", ensured.Did, ActionNothing)
	}
	if !reflect.DeepEqual(ensured.Before, ensured.After) || ensured.After.Generation != 7 {
		t.Fatalf("Ensured = %+v, want Before restated as After", ensured)
	}
	if !slices.Equal(h.launchd.verbs, []string{"print"}) {
		t.Fatalf("launchctl verbs = %q, want only the applied-state observation", h.launchd.verbs)
	}
	if len(h.signals) != 0 {
		t.Fatalf("delivered %d signals, want none", len(h.signals))
	}
}

// TestEnsureConvergesOnAnUpgradedBundle is the bundled policy's whole drift
// semantics, and it is not copied's. A .app is upgraded out of band, under a
// Client that outlives the upgrade — the shape every long-lived launcher in the
// fleet has. Nothing daemonkit owns deployed those bytes, so the build the
// launcher wants is whatever is at the path launchd execs, re-read; a digest
// frozen when the Program was constructed makes want permanently unreachable,
// and every later Ensure evicts a healthy daemon, re-applies, and fails on a
// build that exists nowhere — forever, on every pass.
func TestEnsureConvergesOnAnUpgradedBundle(t *testing.T) {
	home := ladderHome(t)
	app := filepath.Join(realPath(t, t.TempDir()), "Fake.app")
	exe := filepath.Join(app, "Contents", "MacOS", "fake")
	if err := os.MkdirAll(filepath.Dir(exe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	program, err := InBundle(app, filepath.Join("Contents", "MacOS", "fake"))
	if err != nil {
		t.Fatalf("InBundle() error = %v", err)
	}
	daemon := Daemon{Label: "com.example.bundled", Program: program}
	client := openClient(t, daemon)

	upgraded := []byte("#!/bin/sh\nexit 1\n")
	if err := os.WriteFile(exe, upgraded, 0o700); err != nil {
		t.Fatalf("upgrade the bundle: %v", err)
	}
	agent, err := daemon.agent()
	if err != nil {
		t.Fatalf("agent() error = %v", err)
	}
	plist, err := agent.Plist()
	if err != nil {
		t.Fatalf("Plist() error = %v", err)
	}
	plistPath, err := agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	if err := os.WriteFile(plistPath, plist, 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, "Library", "LaunchAgents")); err != nil {
		t.Fatal(err)
	}

	recorder := &launchctlRecorder{loaded: true}
	client.launchctl = recorder.run
	seen := 0
	client.serving = servingScript(
		[]observation{{report: servedReport(wire.PhaseReady, digest(upgraded), 7)}},
		&seen,
	)
	client.readOwner = func(string) (proc.Owner, bool, error) {
		return recordedOwner(digest(upgraded), 7), true, nil
	}
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.kill = func(pid int, _ syscall.Signal) error {
		t.Fatalf("signalled pid %d, want a healthy daemon left alone", pid)
		return nil
	}

	for pass := 1; pass <= 2; pass++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ensured, err := client.Ensure(ctx)
		cancel()
		if err != nil {
			t.Fatalf("pass %d: Ensure() error = %v, want the upgraded bundle already converged", pass, err)
		}
		if ensured.Did != ActionNothing {
			t.Fatalf("pass %d: Did = %v, want %v", pass, ensured.Did, ActionNothing)
		}
	}
	if !slices.Equal(recorder.verbs, []string{"print", "print"}) {
		t.Fatalf("launchctl verbs = %q, want only the applied-state observations", recorder.verbs)
	}
}

// TestEnsureOnceRestartsWhatItPlacedOver is the pass that replaced the program
// bytes: the incumbent reports the wanted build and launchd runs exactly the
// agent, and it is still evicted, because a daemon whose startup straddled the
// replace digests the new bytes while executing the old ones and reports a
// build no launcher can tell apart. What was placed is what gets started.
func TestEnsureOnceRestartsWhatItPlacedOver(t *testing.T) {
	h := newEnsureOnceHarness(
		t,
		[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
		func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
	)
	h.launchd.loaded = true
	h.launchd.refuse = "kickstart"
	h.installPlist(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, true); err == nil ||
		!strings.Contains(err.Error(), "daemonkit: apply") {
		t.Fatalf("ensureOnce() error = %v, want the apply refusal past the eviction", err)
	}
	if !slices.Contains(h.launchd.verbs, "kickstart") {
		t.Fatalf("launchctl verbs = %q, want the agent re-applied over the placed bytes", h.launchd.verbs)
	}
}

// TestEnsureOnceReAppliesUnlessLaunchdRunsExactlyTheAgent drives the ladder's
// whole read of the launchd surface. Applied is launchd's own answer about this
// one label, and every way it comes back false — a plist that was never
// written, one whose bytes drifted, and a byte-exact one launchd never
// bootstrapped — evicts the incumbent and re-applies rather than repairing the
// agent underneath a daemon that is already the wanted build.
func TestEnsureOnceReAppliesUnlessLaunchdRunsExactlyTheAgent(t *testing.T) {
	tests := []struct {
		name   string
		write  func(h *ensureOnceHarness, t *testing.T)
		loaded bool
	}{
		{name: "no plist where launchd reads it", loaded: true},
		{name: "the plist bytes drifted", write: (*ensureOnceHarness).driftPlist, loaded: true},
		{name: "byte-exact but never bootstrapped", write: (*ensureOnceHarness).installPlist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnsureOnceHarness(
				t,
				[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
				func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
			)
			h.launchd.loaded = tt.loaded
			h.launchd.refuse = "bootstrap"
			if tt.write != nil {
				tt.write(h, t)
			}
			evicted := 0
			h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
				evicted++
				return proc.ReapAbsent, true, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
			if err == nil || !strings.Contains(err.Error(), "daemonkit: apply") {
				t.Fatalf("ensureOnce() error = %v, want the apply refusal", err)
			}
			if !slices.Contains(h.launchd.verbs, "bootstrap") {
				t.Fatalf("launchctl verbs = %q, want the agent re-applied", h.launchd.verbs)
			}
			if evicted == 0 {
				t.Fatal("the incumbent was never proven out of the process table before the apply")
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at an incumbent that left on its own", h.signals)
			}
		})
	}
}

// TestEnsureOnceFailsWhenLaunchdCannotBeAsked pins the ladder to launchd's own
// answer about the label. A launchctl that could not be run at all is not an
// unapplied agent to repair, and reading that silence as drift would evict and
// restart a daemon that is already exactly the wanted build.
func TestEnsureOnceFailsWhenLaunchdCannotBeAsked(t *testing.T) {
	unreachable := errors.New("launchctl is not on this machine")
	h := newEnsureOnceHarness(
		t,
		[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
		func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
	)
	h.client.launchctl = func(context.Context, string, ...string) (string, int, error) {
		return "", -1, unreachable
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, false); !errors.Is(err, unreachable) {
		t.Fatalf("ensureOnce() error = %v, want %v", err, unreachable)
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no eviction on an unobservable agent", h.signals)
	}
}

func TestEnsureOnceTouchesNoLabelButItsOwn(t *testing.T) {
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil },
	)
	h.launchd.refuse = "bootstrap"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, false); err == nil {
		t.Fatal("ensureOnce() error = nil, want the apply refusal")
	}
	if len(h.launchd.targets) == 0 {
		t.Fatal("launchctl was never asked anything")
	}
	for i, target := range h.launchd.targets {
		if target != h.agent.Label && !strings.HasSuffix(target, "/"+h.agent.Label) &&
			!strings.HasSuffix(target, "/"+h.agent.Label+".plist") {
			t.Fatalf("launchctl %s named %q, want only %q", h.launchd.verbs[i], target, h.agent.Label)
		}
	}
}

// TestEnsureOnceNeverSignalsARecordItDidNotObserve is the unpinned-settlement
// regression. Nothing serves, so the ladder has only the owner record to act
// on — and that record is same-UID writable. A record rewritten between the
// observation and the settlement names a runtime this Ensure never saw, and
// settling against it would deliver SIGTERM to whatever PID it now carries.
func TestEnsureOnceNeverSignalsARecordItDidNotObserve(t *testing.T) {
	reads := 0
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) {
			reads++
			if reads == 1 {
				return recordedOwner("observed", 7), true, nil
			}
			return recordedOwner("a stranger", 99), true, nil
		},
	)
	h.settling = true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
	if !errors.Is(err, ErrWrongIncumbent) {
		t.Fatalf("ensureOnce() error = %v, want ErrWrongIncumbent", err)
	}
	if !moved(err) {
		t.Fatal("the refusal does not re-observe: Ensure would abort instead of re-deciding")
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no signal at a record this Ensure never observed", h.signals)
	}
	if slices.Contains(h.launchd.verbs, "bootstrap") {
		t.Fatalf("launchctl verbs = %q, want no apply past a refused settlement", h.launchd.verbs)
	}
}

func TestEnsureOnceSignalsAWedgedIncumbentAtItsRecordedIdentity(t *testing.T) {
	owner := recordedOwner("observed", 7)
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return owner, true, nil },
	)
	h.settling = true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, false); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("ensureOnce() error = %v, want ErrUnsettled", err)
	}
	if !slices.Equal(h.signals, []int{owner.PID}) {
		t.Fatalf("signalled %v, want exactly the recorded pid %d", h.signals, owner.PID)
	}
}

// TestProveLeavesBudgetPastAWedgedIncumbent pins the proof ladder's shares.
// Observing departure, signalling, and observing again are each a slice of the
// proof's own budget, so an incumbent that never leaves cannot spend the whole
// Ensure on being watched and starve the apply and the readiness wait after it.
func TestProveLeavesBudgetPastAWedgedIncumbent(t *testing.T) {
	owner := recordedOwner("observed", 7)
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return owner, true, nil },
	)
	h.settling = true
	const budget = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, false); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("ensureOnce() error = %v, want ErrUnsettled", err)
	}
	if spent := time.Since(start); spent > budget*9/10 {
		t.Fatalf("the proof spent %v of a %v budget, leaving nothing to apply and wait with", spent, budget)
	}
	if !slices.Equal(h.signals, []int{owner.PID}) {
		t.Fatalf("signalled %v, want exactly the recorded pid %d", h.signals, owner.PID)
	}
}

// TestEnsureOnceHoldsTheHuskItObserved pins the husk correlation where it is
// actually reachable. The ladder observed a recorded incumbent and the record
// was gone by the time it settled, so the gate runs with no record left to read
// and the identity the observation named is the whole of what says whose an
// unnameable process is: the husk it names refuses, and one it never saw does
// not brick the gate.
func TestEnsureOnceHoldsTheHuskItObserved(t *testing.T) {
	owner := recordedOwner("observed", 7)
	tests := []struct {
		name    string
		husk    proc.Identity
		wantErr error
	}{
		{
			name:    "the husk the observation named",
			husk:    owner.Identity(),
			wantErr: ErrUnsettled,
		},
		{
			name: "a husk this ladder never observed",
			husk: proc.Identity{PID: owner.PID + 1, Start: owner.Start, Boot: owner.Boot},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			h := newEnsureOnceHarness(
				t,
				[]observation{{err: ErrAbsent}},
				func(string) (proc.Owner, bool, error) {
					reads++
					return owner, reads == 1, nil
				},
			)
			h.client.identities = func(string) (proc.Report, error) {
				return proc.Report{Unnameable: []proc.Identity{tt.husk}}, nil
			}
			h.launchd.refuse = "bootstrap"
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
			if tt.wantErr == nil {
				if err == nil || errors.Is(err, ErrUnsettled) {
					t.Fatalf("ensureOnce() = %v, want a cleared gate and the apply refusal past it", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ensureOnce() = %v, want %v", err, tt.wantErr)
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at a record that names nobody", h.signals)
			}
		})
	}
}

// TestEvictHoldsTheHuskItObservedWithoutASession is the same correlation on the
// eviction arm, where it was forgotten. A daemon served the observation, so the
// ladder named an incumbent — but the drain attach finds no listener and the
// record is gone by the time the proof reads it, which leaves what the
// observation pinned as the whole of what says whose an unnameable process is.
func TestEvictHoldsTheHuskItObservedWithoutASession(t *testing.T) {
	owner := recordedOwner("stale", 7)
	tests := []struct {
		name    string
		husk    proc.Identity
		wantErr error
	}{
		{
			name:    "the husk the observation named",
			husk:    owner.Identity(),
			wantErr: ErrUnsettled,
		},
		{
			name: "a husk this ladder never observed",
			husk: proc.Identity{PID: owner.PID + 1, Start: owner.Start, Boot: owner.Boot},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			h := newEnsureOnceHarness(
				t,
				[]observation{{report: servedReport(wire.PhaseReady, owner.Build, owner.Generation)}},
				func(string) (proc.Owner, bool, error) {
					reads++
					return owner, reads == 1, nil
				},
			)
			h.client.identities = func(string) (proc.Report, error) {
				return proc.Report{Unnameable: []proc.Identity{tt.husk}}, nil
			}
			h.launchd.refuse = "bootstrap"
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
			if tt.wantErr == nil {
				if err == nil || errors.Is(err, ErrUnsettled) {
					t.Fatalf("ensureOnce() = %v, want a cleared gate and the apply refusal past it", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ensureOnce() = %v, want %v", err, tt.wantErr)
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at a record that names nobody", h.signals)
			}
		})
	}
}

// TestEnsureOnceHandsTheObservationsPinToTheEviction is that same correlation
// one step earlier, on the pass that genuinely observed a live peer. The attach
// that read Health pinned the process answering for it, while the owner record
// beside it names nobody — which is what an upgrade that unlinked the daemon's
// bytes and cleared its record looks like — so the pin is the whole of what says
// whose the husk left behind is. Handing the record's zero identity down instead
// left the one path that observed a live peer with nothing to correlate.
func TestEnsureOnceHandsTheObservationsPinToTheEviction(t *testing.T) {
	pinned := proc.Identity{PID: 4242, Start: 1, Boot: 1}
	tests := []struct {
		name    string
		husk    proc.Identity
		wantErr error
	}{
		{
			name:    "the husk this pass pinned",
			husk:    pinned,
			wantErr: ErrUnsettled,
		},
		{
			name: "a husk this pass never pinned",
			husk: proc.Identity{PID: pinned.PID + 1, Start: pinned.Start, Boot: pinned.Boot},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnsureOnceHarness(
				t,
				[]observation{{report: servedReport(wire.PhaseReady, "stale", 7), pinned: pinned}},
				func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil },
			)
			h.client.identities = func(string) (proc.Report, error) {
				return proc.Report{Unnameable: []proc.Identity{tt.husk}}, nil
			}
			h.launchd.refuse = "bootstrap"
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent, false)
			if tt.wantErr == nil {
				if err == nil || errors.Is(err, ErrUnsettled) {
					t.Fatalf("ensureOnce() = %v, want a cleared gate and the apply refusal past it", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ensureOnce() = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "unnameable") {
				t.Fatalf("ensureOnce() = %v, want the pinned husk named", err)
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at a record that names nobody", h.signals)
			}
		})
	}
}

// TestEvictHandsTheSessionsPinToTheProof is the eviction's other arm, the one
// that held a session. The drain lands but its exit is never observed, so the
// proof runs — and it runs with nothing observed on the way in, which leaves the
// peer this attach pinned as the whole of what can attribute a husk once the
// record names nobody.
//
// The eviction takes the only control session this test opens. A closed
// session's lane slot is freed when the daemon's own read loop returns, so a
// warm-up attach closed a moment earlier would race the eviction's for it —
// which is why the incumbent is named out of the record it wrote before binding
// rather than read off a session.
func TestEvictHandsTheSessionsPinToTheProof(t *testing.T) {
	tests := []struct {
		name    string
		label   string
		husk    func(proc.Identity) proc.Identity
		wantErr error
	}{
		{
			name:    "the husk this attach pinned",
			label:   "dkevictown",
			husk:    func(id proc.Identity) proc.Identity { return id },
			wantErr: ErrUnsettled,
		},
		{
			name:  "a husk this attach never pinned",
			label: "dkevictother",
			husk: func(id proc.Identity) proc.Identity {
				return proc.Identity{PID: id.PID + 1, Start: id.Start, Boot: id.Boot}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := shortHome(t)
			unrun := filepath.Join(home, "never-executed")
			if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
				t.Fatalf("write program: %v", err)
			}
			startControlChild(t, tt.label)
			client := openClient(t, Daemon{
				Label:    Label(tt.label),
				Program:  Program{policy: bundled{file: unrun}},
				Schemas:  []Schema{"test.v1"},
				Shutdown: Grace(5 * time.Second),
			})
			awaitListener(t, tt.label)
			owner, recorded, err := proc.ReadOwner(client.daemon.RecordPath())
			if err != nil || !recorded {
				t.Fatalf("ReadOwner() = %+v, %v, %v; want the child's own record", owner, recorded, err)
			}
			client.observe = func(proc.Identity) (proc.Reap, bool, error) { return 0, false, nil }
			client.readOwner = func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }
			client.identities = func(string) (proc.Report, error) {
				return proc.Report{Unnameable: []proc.Identity{tt.husk(owner.Identity())}}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			before := Health{Build: owner.Build, Generation: owner.Generation, PID: owner.PID}
			err = client.evict(ctx, before, proc.Identity{})
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("evict() = %v, want a cleared gate", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("evict() = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "unnameable") {
				t.Fatalf("evict() = %v, want the pinned husk named", err)
			}
		})
	}
}

// awaitListener waits for the child's socket to accept without taking a lane
// slot: a connection that sends no hello never reaches the lane's capacity gate.
// The owner record is written before the bind, so a socket that accepts has one.
func awaitListener(t *testing.T, label string) {
	t.Helper()
	socket, err := paths.Socket(label)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %q = %v", socket, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEnsureOnceProvesAbsenceByInventoryWhenNothingIsRecorded pins the one path
// with no incumbent to name: absence is the kernel's answer over the executable,
// never a settlement against whatever the record file says next.
func TestEnsureOnceProvesAbsenceByInventoryWhenNothingIsRecorded(t *testing.T) {
	settled := 0
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil },
	)
	h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
		settled++
		return proc.ReapAbsent, true, nil
	}
	h.launchd.refuse = "bootstrap"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent, false); err == nil {
		t.Fatal("ensureOnce() error = nil, want the apply refusal")
	}
	if settled != 0 {
		t.Fatalf("observed the process table %d times, want the inventory instead", settled)
	}
	if want := []string{realPath(t, programPath(t, h.client.daemon))}; !slices.Equal(h.inventoried, want) {
		t.Fatalf("inventoried %q, want %q", h.inventoried, want)
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no signal with nobody recorded", h.signals)
	}
}

func TestPinRefusesAnIncompleteIncumbent(t *testing.T) {
	tests := []struct {
		name       string
		build      string
		generation uint64
		refused    bool
	}{
		{name: "both named", build: "b1", generation: 7},
		{name: "no build", generation: 7, refused: true},
		{name: "no generation", build: "b1", refused: true},
		{name: "neither", refused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := pin(tt.build, tt.generation)
			if tt.refused {
				if err == nil {
					t.Fatalf("pin() = %+v, want a refusal", target)
				}
				return
			}
			if err != nil {
				t.Fatalf("pin() error = %v", err)
			}
			if want := (Expect{Build: tt.build, Generation: tt.generation}); target.expect() != want {
				t.Fatalf("expect() = %+v, want %+v", target.expect(), want)
			}
		})
	}
}

func TestMovedNamesTheRacesEnsureReObserves(t *testing.T) {
	control := &Control{pinned: proc.Identity{PID: 11}, generation: 7}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nothing went wrong", err: nil},
		{name: "the expectation disagreed", err: ErrWrongIncumbent, want: true},
		{name: "a wrapped expectation disagreed", err: fmt.Errorf("drain: %w", ErrWrongIncumbent), want: true},
		{name: "the pinned peer moved", err: control.pinnedBy(servedReport(wire.PhaseReady, "b", 9)), want: true},
		{name: "nothing is listening", err: ErrAbsent},
		{name: "the incumbent did not leave", err: ErrUnsettled},
		{name: "the peer is untrusted", err: ErrUntrusted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := moved(tt.err); got != tt.want {
				t.Fatalf("moved(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestEnsureReObservesWhenTheIncumbentMovesUnderIt drives the whole verb over a
// record that names a different runtime on every read: each pass refuses rather
// than evicting a stranger, and Ensure re-observes on its cadence until the
// caller's deadline instead of spinning on the lost race.
func TestEnsureReObservesWhenTheIncumbentMovesUnderIt(t *testing.T) {
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	client := openClient(t, Daemon{Label: Label(agent.Label), Program: Program{policy: bundled{file: agent.Program}}})
	client.serving = servingScript([]observation{{report: servedReport(wire.PhaseReady, "stale", 7)}}, &seen)
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.kill = func(pid int, _ syscall.Signal) error {
		t.Fatalf("signalled pid %d, want no signal at a runtime Ensure never observed", pid)
		return nil
	}
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := client.Ensure(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ensure() error = %v, want the deadline joined in", err)
	}
	if !moved(err) {
		t.Fatalf("Ensure() error = %v, want the race it kept losing named", err)
	}
	if seen < 2 {
		t.Fatalf("observed %d times, want Ensure to have re-observed", seen)
	}
	if seen > 200 {
		t.Fatalf("observed %d times in 400ms: the retry spins instead of re-observing on a cadence", seen)
	}
}

// TestEnsureOnceReportsTheRaceWhenItsBudgetIsGone drives one pass on a budget
// that is already spent. Every deadline it derives is in the past, so the dial
// never leaves the process and net answers with its own i/o timeout — a fact
// about the clock and none about the incumbent. The pass reports the race it
// lost, not the transport's timeout.
func TestEnsureOnceReportsTheRaceWhenItsBudgetIsGone(t *testing.T) {
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	client := openClient(t, Daemon{Label: Label(agent.Label), Program: Program{policy: bundled{file: agent.Program}}})
	client.serving = servingScript([]observation{{report: servedReport(wire.PhaseReady, "stale", 7)}}, &seen)
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.kill = func(pid int, _ syscall.Signal) error {
		t.Fatalf("signalled pid %d, want no signal at a runtime this pass never observed", pid)
		return nil
	}
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	_, err := client.ensureOnce(ctx, "wanted", agent, false)
	if !errors.Is(err, ErrWrongIncumbent) || !moved(err) {
		t.Fatalf("ensureOnce() error = %v, want the race the pass lost", err)
	}
}

// TestSpentNamesTheBudgetRatherThanThePeer pins the classification the tail of
// the ladder hangs on: an i/o timeout raised because the deadline was already
// gone is the budget ending, and never an answer about whatever was on the
// socket. The same timeout with budget still left is the peer's.
func TestSpentNamesTheBudgetRatherThanThePeer(t *testing.T) {
	dial := wire.UnixDialer(filepath.Join(t.TempDir(), "absent.sock"))
	expired, cancelExpired := context.WithTimeout(context.Background(), -time.Second)
	defer cancelExpired()
	live, cancelLive := context.WithTimeout(context.Background(), time.Minute)
	defer cancelLive()
	if _, err := dial(expired); !spent(expired, err) {
		t.Fatalf("spent(%v) = false, want the dial that never left the process named as the budget", err)
	}
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"a socket read past its own deadline", expired, os.ErrDeadlineExceeded, true},
		{"the same timeout with budget still left", live, os.ErrDeadlineExceeded, false},
		{"a refusal at the end of the budget", expired, ErrUntrusted, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := spent(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("spent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestEnsureRefusesAPassItCannotFinish pins the tail of the ladder. A slice too
// small to fund a pass funds none of the deadlines the pass derives from it, so
// re-entering on one observes nothing and evicts nothing — Ensure waits out the
// budget it was given and reports the race instead.
func TestEnsureRefusesAPassItCannotFinish(t *testing.T) {
	const budget = 400 * time.Millisecond
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	var last time.Duration
	client := openClient(t, Daemon{Label: Label(agent.Label), Program: Program{policy: bundled{file: agent.Program}}})
	client.serving = func(ctx context.Context) (wire.HealthReport, proc.Identity, error) {
		seen++
		last = left(ctx)
		return servedReport(wire.PhaseReady, "stale", 7), proc.Identity{}, nil
	}
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := client.Ensure(ctx); !moved(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ensure() error = %v, want the race joined with the deadline", err)
	}
	if seen < 2 {
		t.Fatalf("observed %d times, want Ensure to have re-observed", seen)
	}
	if last < minPassSlice/4 {
		t.Fatalf("the last pass began with %v of a %v budget left, want at least %v", last, budget, minPassSlice/4)
	}
}

// TestEnsurePlacesTheProgramOnlyUnderTheStartLock is the second consequence of
// a constructor that writes: the write is decoupled from the one lock that
// serializes every transition of the live daemon, so two launchers racing the
// same fixed path both land bytes and the loser reaches "came up as build X" —
// an error moved() does not name, so Ensure hard-errors against a healthy
// daemon instead of re-observing. Under the lock the loser places nothing and
// waits its turn.
func TestEnsurePlacesTheProgramOnlyUnderTheStartLock(t *testing.T) {
	home := ladderHome(t)
	label := Label("com.example.race")
	statePaths := paths.Paths{App: string(label)}
	if err := statePaths.EnsureLockDir(); err != nil {
		t.Fatalf("create lock dir: %v", err)
	}
	held, err := flock.Spec{
		Path:     statePaths.StartLockPath(),
		Mode:     flock.Exclusive,
		Deadline: 2 * time.Second,
	}.Acquire(t.Context())
	if err != nil {
		t.Fatalf("hold the start lock: %v", err)
	}
	defer func() { _ = held.Close() }()

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	client := openClient(t, Daemon{Label: label, Program: program})
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := client.Ensure(ctx); err == nil {
		t.Fatal("Ensure() succeeded while another launcher held the start lock")
	}

	target := filepath.Join(home, ".daemonkit", "bin", string(label))
	if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the program was placed at %q while another launcher held the start lock (stat: %v)", target, err)
	}
}

func programPath(t *testing.T, d Daemon) string {
	t.Helper()
	path, err := d.Program.path(mustElement(t, d.Label))
	if err != nil {
		t.Fatalf("Program.path() error = %v", err)
	}
	return path
}

// mustElement is the Label rule run for a fixture that is meant to clear it, so
// a test that means to exercise a path never states the path element itself.
func mustElement(t *testing.T, label Label) element {
	t.Helper()
	el, err := label.element()
	if err != nil {
		t.Fatalf("element() error = %v", err)
	}
	return el
}
