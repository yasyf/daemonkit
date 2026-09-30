package proc

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestParseStat(t *testing.T) {
	const tail = " 1 4242 4200 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 3 0 987654 1000 10 18446744073709551615"
	tests := []struct {
		name    string
		raw     string
		want    taskStat
		zombie  bool
		wantErr bool
	}{
		{
			name: "a plain comm",
			raw:  "77 (sleep) S" + tail,
			want: taskStat{comm: "sleep", state: 'S', group: 4242, session: 4200, flags: 4194560, threads: 3, start: 987654},
		},
		{
			name: "a comm carrying spaces and parentheses",
			raw:  "77 (a) R (b c)) T" + tail,
			want: taskStat{comm: "a) R (b c)", state: 'T', group: 4242, session: 4200, flags: 4194560, threads: 3, start: 987654},
		},
		{
			name:   "a wholly exited process",
			raw:    "77 (sh) Z 1 4242 4200 0 -1 4 0 0 0 0 0 0 0 0 20 0 1 0 987654 0 0",
			want:   taskStat{comm: "sh", state: 'Z', group: 4242, session: 4200, flags: 4, threads: 1, start: 987654},
			zombie: true,
		},
		{
			name: "a leader that exited ahead of its threads is a live process",
			raw:  "77 (sh) Z 1 4242 4200 0 -1 4 0 0 0 0 0 0 0 0 20 0 2 0 987654 0 0",
			want: taskStat{comm: "sh", state: 'Z', group: 4242, session: 4200, flags: 4, threads: 2, start: 987654},
		},
		{name: "no comm", raw: "77 sleep S" + tail, wantErr: true},
		{name: "too few fields", raw: "77 (sleep) S 1 4242 4200", wantErr: true},
		{name: "a state wider than one character", raw: "77 (sleep) SS" + tail, wantErr: true},
		{name: "a start time that is not a number", raw: "77 (sleep) S 1 4242 4200 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 soon 0 0", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseStat([]byte(test.raw))
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseStat(%q) = %+v, want a refusal", test.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStat(%q) = %v", test.raw, err)
			}
			if got != test.want {
				t.Fatalf("parseStat(%q) = %+v, want %+v", test.raw, got, test.want)
			}
			if got.zombie() != test.zombie {
				t.Fatalf("zombie() = %v, want %v", got.zombie(), test.zombie)
			}
		})
	}
}

func TestTaskStatInfoReadsTheStopAndExitBits(t *testing.T) {
	tests := []struct {
		name string
		stat taskStat
		want procInfo
	}{
		{"running", taskStat{state: 'R', threads: 1, start: 7}, procInfo{start: 7}},
		{"job-control stopped", taskStat{state: 'T', threads: 1, start: 7}, procInfo{start: 7, stopped: true}},
		{"stopped under a tracer is not the suspended state", taskStat{state: 't', threads: 1, start: 7}, procInfo{start: 7}},
		{"exiting", taskStat{state: 'R', flags: taskExiting, threads: 1, start: 7}, procInfo{start: 7, exiting: true}},
		{"reaped-in-waiting", taskStat{state: 'Z', flags: taskExiting, threads: 1, start: 7}, procInfo{start: 7, zombie: true, exiting: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.stat.info(); got != test.want {
				t.Fatalf("info() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestProbeProcPinsThisProcess(t *testing.T) {
	info, err := probeProc(os.Getpid())
	if err != nil {
		t.Fatalf("probeProc(self) = %v", err)
	}
	if info.start == 0 || info.zombie || info.stopped {
		t.Fatalf("probeProc(self) = %+v, want a live, running process with a start stamp", info)
	}
	if info.group != unix.Getpgrp() {
		t.Fatalf("group = %d, want %d", info.group, unix.Getpgrp())
	}
	session, err := unix.Getsid(0)
	if err != nil {
		t.Fatal(err)
	}
	if info.session != session {
		t.Fatalf("session = %d, want %d", info.session, session)
	}
	again, err := probeProc(os.Getpid())
	if err != nil {
		t.Fatalf("probeProc(self) again = %v", err)
	}
	if again.start != info.start {
		t.Fatalf("start moved between two probes of one instance: %d then %d", info.start, again.start)
	}
	if _, err := probeProc(1 << 30); !errors.Is(err, errNoProc) {
		t.Fatalf("probeProc(a pid the kernel cannot hold) = %v, want errNoProc", err)
	}
}

func TestBootSessionIsStableAndNonZero(t *testing.T) {
	first, err := bootSession()
	if err != nil {
		t.Fatalf("bootSession() = %v", err)
	}
	second, err := bootSession()
	if err != nil {
		t.Fatalf("bootSession() again = %v", err)
	}
	if first == 0 || first != second {
		t.Fatalf("bootSession() = %d then %d, want one non-zero stamp", first, second)
	}
}

// TestProbeTellsAZombieFromALiveProcess reads both answers off the real
// kernel: an exited child this process has not reaped is a zombie, and a
// session enumeration never counts it as a member.
func TestProbeTellsAZombieFromALiveProcess(t *testing.T) {
	child := exec.Command("/bin/sh", "-c", "exit 0")
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	t.Cleanup(func() { _ = child.Wait() })
	deadline := time.Now().Add(20 * time.Second)
	for {
		info, err := probeProc(pid)
		if err != nil {
			t.Fatalf("probeProc(%d) = %v", pid, err)
		}
		if info.zombie {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d never read as a zombie: %+v", pid, info)
		}
		time.Sleep(10 * time.Millisecond)
	}
	members, err := probeGroupMembers(pid)
	if err != nil {
		t.Fatalf("probeGroupMembers(%d) = %v", pid, err)
	}
	if len(members) != 0 {
		t.Fatalf("members = %+v, want a session whose only process is a zombie to be empty", members)
	}
	if _, err := ExecutablePath(pid); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("ExecutablePath(zombie %d) = %v, want ErrNoProcess: a zombie runs nothing", pid, err)
	}
}

// TestHoldNeverSignalsAPIDsSuccessor is the exactness a pidfd buys. The hold is
// taken on one process; once that process is reaped, a signal through the hold
// is ESRCH whatever the PID names next, where kill(2) by number would reach it.
func TestHoldNeverSignalsAPIDsSuccessor(t *testing.T) {
	child := exec.Command("/bin/sleep", "600")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	target, err := sysSignaler{}.hold(pid)
	if err != nil {
		t.Fatalf("hold(%d) = %v", pid, err)
	}
	defer target.release()
	if err := target.signal(0); err != nil {
		t.Fatalf("signal 0 through the hold of a live process = %v", err)
	}
	if err := target.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL through the hold = %v", err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("the held child exited cleanly, want it killed through the hold")
	}
	if err := target.signal(syscall.SIGKILL); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("signal through the hold of a reaped process = %v, want ESRCH", err)
	}
	gone, err := heldGone(target, syscall.SIGTERM)
	if err != nil || !gone {
		t.Fatalf("heldGone() = %v, %v, want the reaped process reported gone", gone, err)
	}
}

func TestHoldOfAnAbsentPIDSignalsNobody(t *testing.T) {
	target, err := sysSignaler{}.hold(1 << 30)
	if err != nil {
		t.Fatalf("hold(a pid the kernel cannot hold) = %v", err)
	}
	defer target.release()
	if err := target.signal(syscall.SIGTERM); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("signal = %v, want ESRCH", err)
	}
}

// TestDriverHoldsTheExitedChildUnreapedUntilItSettles pins the two-phase wait:
// the driver learns the exit with the zombie still in the table, so the PID it
// might signal cannot have been handed to a stranger, and reaps only once it
// has published the terminal.
func TestDriverHoldsTheExitedChildUnreapedUntilItSettles(t *testing.T) {
	child := exec.Command("/bin/sh", "-c", "exit 7")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	terminal := awaitExitUnreaped(pid)
	if terminal.err != nil || terminal.code != 7 || terminal.signal != 0 {
		t.Fatalf("awaitExitUnreaped() = %+v, want exit status 7", terminal)
	}
	info, err := probeProc(pid)
	if err != nil {
		t.Fatalf("probeProc(%d) after the unreaped wait = %v, want the zombie still in the table", pid, err)
	}
	if !info.zombie {
		t.Fatalf("probeProc(%d) = %+v, want a zombie", pid, info)
	}
	if reaped := awaitExit(pid); reaped.err != nil || reaped.code != 7 {
		t.Fatalf("awaitExit() = %+v, want the same exit status 7", reaped)
	}
	if _, err := probeProc(pid); !errors.Is(err, errNoProc) {
		t.Fatalf("probeProc(%d) after the reap = %v, want errNoProc", pid, err)
	}
}

func TestAwaitExitUnreapedReportsAFatalSignal(t *testing.T) {
	child := exec.Command("/bin/sleep", "600")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	terminal := awaitExitUnreaped(pid)
	awaitExit(pid)
	if terminal.err != nil || terminal.code != -1 || terminal.signal != syscall.SIGKILL {
		t.Fatalf("awaitExitUnreaped() = %+v, want code -1 under SIGKILL", terminal)
	}
}

// TestStartChildStopsBeforeTheFirstInstruction is the suspended spawn read off
// the kernel directly: the child is in a plain job-control stop with no tracer
// left on it, in its own session when asked, and it runs only once released.
func TestStartChildStopsBeforeTheFirstInstruction(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := startChild(
		Cmd{Path: "/bin/sh", Args: []string{"-c", "touch " + marker}, Session: true},
		spawnFiles{stdin: devNull, stdout: devNull, stderr: devNull},
	)
	_ = devNull.Close()
	if err != nil {
		t.Fatalf("startChild() = %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	stat, err := readStat(pid)
	if err != nil {
		t.Fatalf("readStat(%d) = %v", pid, err)
	}
	if stat.state != 'T' || stat.session != pid {
		t.Fatalf("suspended child = state %q session %d, want a job-control stop leading session %d", stat.state, stat.session, pid)
	}
	status, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(strings.Split(string(status), "\n"), "TracerPid:\t0") {
		t.Fatalf("the suspended child is still traced:\n%s", status)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("the suspended child executed an instruction: %v", err)
	}
	if err := releaseChild(pid); err != nil {
		t.Fatalf("releaseChild() = %v", err)
	}
	if terminal := awaitExit(pid); terminal.err != nil || terminal.code != 0 {
		t.Fatalf("released child = %+v, want a clean exit", terminal)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the released child never ran: %v", err)
	}
}

func TestStartChildReportsAnExecThatCannotHappen(t *testing.T) {
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = devNull.Close() }()
	absent := filepath.Join(t.TempDir(), "absent")
	pid, err := startChild(Cmd{Path: absent}, spawnFiles{stdin: devNull, stdout: devNull, stderr: devNull})
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("startChild(%q) = %d, %v, want ENOENT", absent, pid, err)
	}
}
