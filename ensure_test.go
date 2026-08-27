package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
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
	"github.com/yasyf/daemonkit/internal/state"
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
				LogPath:       filepath.Join(home, ".daemonkit", "a", "com.example.ensure", "daemon.log"),
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
	unreadable := filepath.Join(t.TempDir(), "unreadable.records")
	if err := os.MkdirAll(unreadable, 0o700); err != nil {
		t.Fatal(err)
	}
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(sigterm) })
	tests := []struct {
		name       string
		recordPath string
		target     incumbent
		wantErr    error
		wantAnyErr bool
	}{
		{
			name:       "a record naming another build is not signalled",
			recordPath: path,
			target:     incumbent{build: "b2", generation: owner.Generation},
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "a record naming another instance is not signalled",
			recordPath: path,
			target:     incumbent{build: owner.Build, generation: owner.Generation + 1},
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "no owner record names nobody to signal",
			recordPath: noRecord,
			wantErr:    ErrUnrecorded,
		},
		{
			name:       "an unreadable record propagates",
			recordPath: unreadable,
			wantAnyErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.target
			if target == (incumbent{}) {
				target = incumbent{build: owner.Build, generation: owner.Generation}
			}
			err := repairWedged(tt.recordPath, target)
			switch {
			case tt.wantAnyErr:
				if err == nil || errors.Is(err, ErrUnrecorded) || errors.Is(err, ErrWrongIncumbent) {
					t.Fatalf("repairWedged() error = %v, want the record read propagated", err)
				}
			default:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("repairWedged() error = %v, want %v", err, tt.wantErr)
				}
			}
			select {
			case sig := <-sigterm:
				t.Fatalf("repairWedged delivered %v to pid %d, which the record does not name as the target", sig, owner.PID)
			default:
			}
		})
	}
}

// notifySIGTERM catches the one signal daemonkit sends, so a signal addressed
// to this process is an assertion failure instead of a dead test binary.
func notifySIGTERM(t *testing.T) <-chan os.Signal {
	t.Helper()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(sigterm) })
	return sigterm
}

// selfSpared proves the signal went elsewhere. The wait is bounded rather than
// instantaneous because a signal delivered to this process reaches the channel
// through the runtime's own goroutine.
func selfSpared(t *testing.T, sigterm <-chan os.Signal) {
	t.Helper()
	select {
	case sig := <-sigterm:
		t.Fatalf("repairWedged delivered %v to this process %d, which no record under test names", sig, os.Getpid())
	case <-time.After(200 * time.Millisecond):
	}
}

// TestRepairWedgedTerminatesTheLiveRecordedIncumbent is the arm the refusals
// are refusals of: a record that still names the target and still names a live
// process is the wedged daemon repairWedged exists to end. The incumbent is a
// real daemon child that recorded itself, so the address under test is the
// kernel's answer for that child and the departure is its actual exit.
func TestRepairWedgedTerminatesTheLiveRecordedIncumbent(t *testing.T) {
	d, child, owner := liveOwnerFixture(t, "dkwedged")
	if want := ownerAt(probed(t, child.Process.Pid), owner.Build, owner.Generation); owner != want {
		t.Fatalf("the record names %+v, want the live child's identity %+v", owner, want)
	}
	sigterm := notifySIGTERM(t)
	if err := repairWedged(d.RecordPath(), incumbent{build: owner.Build, generation: owner.Generation}); err != nil {
		t.Fatalf("repairWedged() error = %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("child exit = %v, want the clean exit a drained SIGTERM leaves", err)
		}
	case <-time.After(30 * time.Second):
		_ = child.Process.Kill()
		<-exited
		t.Fatalf("the recorded incumbent %d is still running: repairWedged addressed nobody", owner.PID)
	}
	selfSpared(t, sigterm)
}

// TestRepairWedgedHoldsTheRecordedPinAgainstTheKernel drives the two gates that
// stand between a record naming the target and the signal: the recorded pid
// must still be live, and the {start, boot} riding with it must still be the
// kernel's answer for that pid. Every case names a pid this process may not
// signal away — its own, or a real reaped one — so a gate that stopped holding
// is a SIGTERM at the test binary.
func TestRepairWedgedHoldsTheRecordedPinAgainstTheKernel(t *testing.T) {
	self := probed(t, os.Getpid())
	reused := self
	reused.Start++
	crossBoot := self
	crossBoot.Boot++
	sigterm := notifySIGTERM(t)
	tests := []struct {
		name  string
		owner proc.Owner
	}{
		{"a departed incumbent is not signalled", ownerAt(departedIdentity(t), "b1", 7)},
		{"a reused pid is not signalled", ownerAt(reused, "b1", 7)},
		{"a cross-boot pid is not signalled", ownerAt(crossBoot, "b1", 7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := forgeOwnerRecord(t, tt.owner)
			target := incumbent{build: tt.owner.Build, generation: tt.owner.Generation}
			if err := repairWedged(record, target); err != nil {
				t.Fatalf("repairWedged() error = %v", err)
			}
			selfSpared(t, sigterm)
		})
	}
}

// ownerRecordSchema is the schema internal/proc frames its record file with;
// the round-trip in forgeOwnerRecord fails loudly if it ever moves.
const ownerRecordSchema state.Schema = 1

type forgedOwner struct {
	Owner proc.Owner `json:"owner"`
}

func (forgedOwner) Cores() []state.Core { return nil }

// forgeOwnerRecord writes an owner block the store never would. The record file
// is same-UID writable input — that is the whole reason an eviction re-probes
// what it names — and a store only ever records the process that opens it, so a
// record naming any other identity has to be written here.
func forgeOwnerRecord(t *testing.T, owner proc.Owner) string {
	t.Helper()
	record := filepath.Join(t.TempDir(), "records.dkstate")
	if err := state.New[forgedOwner](record, ownerRecordSchema).Store(forgedOwner{Owner: owner}); err != nil {
		t.Fatalf("forge an owner record at %q: %v", record, err)
	}
	read, ok, err := proc.ReadOwner(record)
	if err != nil || !ok || read != owner {
		t.Fatalf("ReadOwner(%q) = (%+v, %t, %v), want the forged %+v", record, read, ok, err, owner)
	}
	return record
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

const inventoryLabel = Label("com.example.inventory")

func TestInventoryClearProvesAbsenceOverTheProcessTable(t *testing.T) {
	unrun := filepath.Join(t.TempDir(), "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	idle := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: unrun}}})
	if err := idle.inventoryClear(proc.Identity{}); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory", err)
	}
	running := openClient(t, Daemon{Label: inventoryLabel, Program: Program{policy: bundled{file: realPath(t, selfPath(t))}}})
	if err := running.inventoryClear(proc.Identity{}); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("inventoryClear() over this very process = %v, want ErrUnsettled", err)
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
	live(t, stranger)
	client := openClient(t, Daemon{Label: label, Program: program})
	if err := client.inventoryClear(proc.Identity{}); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory: the stranger is another product's daemon", err)
	}
	sibling := openClient(t, Daemon{Label: Label("com.example.other"), Program: Program{policy: bundled{file: stranger}}})
	if err := sibling.inventoryClear(proc.Identity{}); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("inventoryClear() over the stranger's own program = %v, want ErrUnsettled", err)
	}
}

// live starts a real process on program and blocks until the kernel's own
// process table reports it under that exact path. The program is ad-hoc signed
// because a bare copy of a system binary carries a platform-binary signature
// the kernel honours only on the system volume.
func live(t *testing.T, program string) {
	t.Helper()
	body, err := os.ReadFile("/bin/sleep")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(program, body, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("/usr/bin/codesign", "--sign", "-", "--force", program).CombinedOutput(); err != nil {
		t.Fatalf("codesign %q: %v\n%s", program, err, out)
	}
	child := exec.Command(program, "600")
	if err := child.Start(); err != nil {
		t.Fatalf("start %q: %v", program, err)
	}
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	resolved := realPath(t, program)
	deadline := time.Now().Add(20 * time.Second)
	for {
		found, err := proc.ExecutableIdentities(resolved)
		if err != nil {
			t.Fatalf("ExecutableIdentities(%q): %v", resolved, err)
		}
		if slices.ContainsFunc(found.Matched, func(id proc.Identity) bool { return id.PID == child.Process.Pid }) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the process at %q never reached the process table", program)
		}
		time.Sleep(10 * time.Millisecond)
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

// probed is the kernel's own answer for pid. The eviction re-probes whatever
// identity a record names, so an identity a test states instead of probing
// clears the PID-reuse gate no matter what the process table holds.
func probed(t *testing.T, pid int) proc.Identity {
	t.Helper()
	id, err := proc.ProbeIdentity(pid)
	if err != nil {
		t.Fatalf("ProbeIdentity(%d) error = %v", pid, err)
	}
	return id
}

// departedIdentity is a real process's identity, probed while it was alive and
// then reaped. That the reaped pid probes as ErrNoProcess is asserted rather
// than assumed: a pid the machine has already handed on answers with a live
// identity, which proves the reuse branch and not the departure one.
func departedIdentity(t *testing.T) proc.Identity {
	t.Helper()
	helper := exec.Command("/bin/sleep", "600")
	if err := helper.Start(); err != nil {
		t.Fatalf("start the departed helper: %v", err)
	}
	id := probed(t, helper.Process.Pid)
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill the departed helper: %v", err)
	}
	_ = helper.Wait()
	if _, err := proc.ProbeIdentity(id.PID); !errors.Is(err, proc.ErrNoProcess) {
		t.Fatalf("ProbeIdentity(%d) error = %v after the helper was reaped, want %v", id.PID, err, proc.ErrNoProcess)
	}
	return id
}

func ownerAt(id proc.Identity, build string, generation uint64) proc.Owner {
	return proc.Owner{PID: id.PID, Start: id.Start, Boot: id.Boot, Generation: generation, Build: build}
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

func ladderHome(t *testing.T) string {
	t.Helper()
	home := shortHome(t)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	return home
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

// TestEnsurePlacesTheProgramOnlyUnderTheStartLock is the second consequence of
// a constructor that writes: the write is decoupled from the one lock that
// serializes every transition of the live daemon, so two launchers racing the
// same fixed path both land bytes and the loser reaches "came up as build X" —
// an error moved() does not name, so Ensure hard-errors against a healthy
// daemon instead of re-observing. Under the lock the loser places nothing and
// waits its turn.
func TestEnsurePlacesTheProgramOnlyUnderTheStartLock(t *testing.T) {
	ladderHome(t)
	label := Label("com.example.race")
	statePaths := paths.Agent(string(label))
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

	target := programPath(t, client.daemon)
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
