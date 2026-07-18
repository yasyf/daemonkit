package proc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

const fakeHolderEnv = "FUSEKIT_PROC_TEST_FAKE_HOLDER"

var childArgs = func(socket string) []string { return []string{"mount-holder", "--socket", socket} }

func alwaysHost() error { return nil }

func dialAvailable(socket string) func() bool {
	return func() bool {
		conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}
}

// shortSockDir avoids t.TempDir(), whose paths exceed macOS's 104-byte sun_path cap.
func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-proc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestMain doubles as the spawned child: childCmd execs THIS test binary;
// fakeHolderEnv makes it fast-fail instead of re-running the suite (fork bomb).
func TestMain(m *testing.M) {
	if os.Getenv(fakeHolderEnv) == "1" {
		fmt.Fprintln(os.Stderr, "fake holder: exiting without serving")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestChildCmd(t *testing.T) {
	socket := "/tmp/ccp-test/m.sock"
	logPath := filepath.Join(t.TempDir(), "holder.log")

	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}.childCmd()
	if err != nil {
		t.Fatalf("childCmd: %v", err)
	}
	defer logFile.Close()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := []string{exe, "mount-holder", "--socket", socket}
	if !reflect.DeepEqual(cmd.Args, wantArgs) {
		t.Errorf("argv = %q, want %q", cmd.Args, wantArgs)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Errorf("SysProcAttr = %+v, want Setsid", cmd.SysProcAttr)
	}
	if cmd.Stdin != nil {
		t.Errorf("Stdin = %v, want nil", cmd.Stdin)
	}
	if cmd.Stdout != logFile || cmd.Stderr != logFile {
		t.Errorf("Stdout/Stderr = %v/%v, want the log file %v", cmd.Stdout, cmd.Stderr, logFile)
	}
	if logFile.Name() != logPath {
		t.Errorf("log file = %q, want %q", logFile.Name(), logPath)
	}
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("log perms = %o, want 0600", got)
	}
}

func TestChildCmdUnopenableLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	socket := "/tmp/ccp-test/m.sock"
	if _, _, err := (Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}).childCmd(); err == nil {
		t.Fatal("childCmd with an unopenable log path succeeded, want error")
	}
}

func TestSpawnTimeoutDefault(t *testing.T) {
	if got := (Spawn{}).timeout(); got != DefaultSpawnTimeout {
		t.Errorf("zero Timeout = %v, want %v", got, DefaultSpawnTimeout)
	}
	if got := (Spawn{Timeout: time.Second}).timeout(); got != time.Second {
		t.Errorf("explicit Timeout = %v, want 1s", got)
	}
}

func TestEnsureRunningShortCircuitsWhenAvailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Unopenable log path makes a spawn fail loudly, so a nil return proves the short-circuit ran.
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	err = Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   func() error { t.Fatal("CanHost consulted despite a live socket"); return nil },
	}.EnsureRunning(context.Background())
	if err != nil {
		t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Errorf("log file stat = %v, want not-exist (no spawn)", err)
	}
}

func TestEnsureRunningCanHostRefusalUnwrapped(t *testing.T) {
	refusal := errors.New("this binary cannot host: install the fuse build")
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   func() error { return refusal },
	}.EnsureRunning(context.Background())
	if !errors.Is(err, refusal) {
		t.Errorf("error = %v, want the CanHost refusal returned as-is", err)
	}
	if errors.Is(err, ErrChildUnavailable) {
		t.Errorf("error = %v, want the CanHost refusal NOT wrapped in ErrChildUnavailable", err)
	}
	if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
		t.Errorf("log file stat = %v, want not-exist (no spawn attempted)", statErr)
	}
}

func TestEnsureRunningGateAdmitsEveryLaunchOnly(t *testing.T) {
	t.Run("gate refusal withholds the launch", func(t *testing.T) {
		parked := errors.New("launch parked")
		socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
		logPath := filepath.Join(t.TempDir(), "holder.log")
		gates := 0
		err := Spawn{
			Socket:    socket,
			LogPath:   logPath,
			Args:      childArgs(socket),
			Timeout:   time.Second,
			Available: dialAvailable(socket),
			CanHost:   alwaysHost,
			Gate:      func(context.Context) error { gates++; return parked },
		}.EnsureRunning(context.Background())
		if !errors.Is(err, parked) {
			t.Errorf("error = %v, want the gate refusal returned as-is", err)
		}
		if gates != 1 {
			t.Errorf("gate calls = %d, want 1", gates)
		}
		if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
			t.Errorf("log file stat = %v, want not-exist (no launch behind a refused gate)", statErr)
		}
	})
	t.Run("available skips the gate", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "m.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		err = Spawn{
			Socket:    socket,
			LogPath:   filepath.Join(t.TempDir(), "holder.log"),
			Args:      childArgs(socket),
			Timeout:   time.Second,
			Available: dialAvailable(socket),
			CanHost:   alwaysHost,
			Gate:      func(context.Context) error { t.Fatal("gate consulted despite a live socket"); return nil },
		}.EnsureRunning(context.Background())
		if err != nil {
			t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
		}
	})
}

func TestEnsureRunningSpawnFailureClassifiedHolderUnavailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")

	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   alwaysHost,
	}.EnsureRunning(context.Background())
	if err == nil {
		t.Fatal("EnsureRunning with an unopenable log path succeeded, want error")
	}
	if !errors.Is(err, ErrChildUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrChildUnavailable", err)
	}
}

func TestEnsureRunningSpawnTimesOutOnFastFailingChild(t *testing.T) {
	t.Setenv(fakeHolderEnv, "1")
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")

	// A fake clock drives the socket poll to its deadline without a real wait.
	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   alwaysHost,
		clock:     newFakeClock(),
	}.EnsureRunning(context.Background())
	if err == nil {
		t.Fatal("EnsureRunning with a child that dies before serving succeeded, want timeout error")
	}
	if !errors.Is(err, ErrChildUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrChildUnavailable", err)
	}
	if !strings.Contains(err.Error(), "did not come up on "+socket) {
		t.Errorf("error = %q, want the did-not-come-up copy naming the socket", err)
	}
	if !strings.Contains(err.Error(), "check "+logPath) {
		t.Errorf("error = %q, want it to point at the log %s", err, logPath)
	}
	// Poll for the fake child's stderr line: the detached child races us.
	deadline := time.Now().Add(5 * time.Second)
	var logData []byte
	for time.Now().Before(deadline) {
		logData, _ = os.ReadFile(logPath)
		if strings.Contains(string(logData), "fake holder") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(string(logData), "fake holder") {
		t.Errorf("child log = %q, want the fake child's stderr line", logData)
	}
}

// A zombie stays signalable: kill(pid, 0) reports ESRCH only once the child
// is waited out.
func TestSpawnedChildReaped(t *testing.T) {
	t.Setenv(fakeHolderEnv, "1")
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")
	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}.childCmd()
	if err != nil {
		t.Fatalf("childCmd: %v", err)
	}
	defer logFile.Close()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid
	reap(cmd)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return // reaped
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("spawned child pid %d still in the process table: exited child not reaped (zombie)", pid)
}

// errStrategy is an in-package LaunchStrategy double that records invocation
// and hands back a fixed error, so a test can observe the launch seam without
// exec'ing a child.
type errStrategy struct {
	called *bool
	err    error
}

func (e errStrategy) launch(Spawn) (*exec.Cmd, *os.File, error) {
	*e.called = true
	return nil, nil, e.err
}

func TestEnsureRunningLaunchStrategy(t *testing.T) {
	ctx := context.Background()

	t.Run("available short-circuits without CanHost or the strategy", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "m.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		launched, canHostCalled := false, false
		err = Spawn{
			Socket:    socket,
			Args:      childArgs(socket),
			Available: dialAvailable(socket),
			CanHost:   func() error { canHostCalled = true; return nil },
			Launch:    errStrategy{called: &launched, err: errors.New("strategy should not run")},
		}.EnsureRunning(ctx)
		if err != nil {
			t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
		}
		if launched {
			t.Error("strategy ran despite the Available short-circuit")
		}
		if canHostCalled {
			t.Error("CanHost consulted despite the Available short-circuit")
		}
	})

	t.Run("unavailable consults CanHost then the strategy, wrapping its error", func(t *testing.T) {
		sentinel := errors.New("strategy drove the launch")
		socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
		logPath := filepath.Join(t.TempDir(), "holder.log")

		launched, canHostCalled := false, false
		err := Spawn{
			Socket:    socket,
			LogPath:   logPath,
			Args:      childArgs(socket),
			Timeout:   time.Second,
			Available: dialAvailable(socket),
			CanHost:   func() error { canHostCalled = true; return nil },
			Launch:    errStrategy{called: &launched, err: sentinel},
		}.EnsureRunning(ctx)
		if !errors.Is(err, sentinel) {
			t.Fatalf("error = %v, want the strategy error", err)
		}
		if !errors.Is(err, ErrChildUnavailable) {
			t.Errorf("error = %v, want the strategy error wrapped in ErrChildUnavailable", err)
		}
		if !canHostCalled {
			t.Error("CanHost not consulted before the strategy — no strategy may bypass it")
		}
		if !launched {
			t.Error("strategy not invoked on the unavailable path")
		}
	})

	t.Run("CanHost refusal skips the strategy", func(t *testing.T) {
		refusal := errors.New("this binary cannot host")
		socket := filepath.Join(shortSockDir(t), "m.sock")

		launched := false
		err := Spawn{
			Socket:    socket,
			Args:      childArgs(socket),
			Available: dialAvailable(socket),
			CanHost:   func() error { return refusal },
			Launch:    errStrategy{called: &launched, err: errors.New("strategy should not run")},
		}.EnsureRunning(ctx)
		if !errors.Is(err, refusal) {
			t.Fatalf("error = %v, want the CanHost refusal", err)
		}
		if errors.Is(err, ErrChildUnavailable) {
			t.Errorf("error = %v, want the refusal NOT wrapped in ErrChildUnavailable", err)
		}
		if launched {
			t.Error("strategy ran despite a CanHost refusal")
		}
	})
}

func TestSpawnStrategyDefault(t *testing.T) {
	if _, ok := (Spawn{}).strategy().(ExecLaunch); !ok {
		t.Errorf("nil Launch strategy = %T, want ExecLaunch", (Spawn{}).strategy())
	}
	app := AppLaunchNew{App: "/Applications/Foo.app"}
	got, ok := (Spawn{Launch: app}).strategy().(AppLaunchNew)
	if !ok || got.App != app.App {
		t.Errorf("explicit Launch strategy = %#v, want %#v", (Spawn{Launch: app}).strategy(), app)
	}
}
