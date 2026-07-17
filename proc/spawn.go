package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// DefaultSpawnTimeout bounds the wait for a freshly spawned child's socket.
const DefaultSpawnTimeout = 5 * time.Second

// Spawn ensures a detached child process is serving Socket, spawning one in
// its own session when needed. Racing spawns are harmless: the child refuses
// to start if the socket is already owned. HARD CONTRACT: the child inherits
// every non-CLOEXEC parent descriptor (fork+exec), and pure Go offers no
// fork hook to sweep them spawner-side — so any long-lived child binary MUST
// call CloseInheritedFDs before any other work in main (fusekit's cmd/holder
// complies); a leaked session-lease fd would otherwise stay pinned for the
// child's lifetime.
type Spawn struct {
	// Socket is the child's unix socket path.
	Socket string
	// LogPath receives a spawned child's stdout and stderr.
	LogPath string
	// Args is the child's argv after the executable.
	Args []string
	// Timeout bounds waiting for a freshly spawned child's socket. Zero means
	// DefaultSpawnTimeout.
	Timeout time.Duration
	// ExecPath, when set, is the binary the child execs instead of os.Executable().
	ExecPath string
	// Available reports whether a child is already serving Socket. Required.
	Available func() bool
	// CanHost gates the spawn: a non-nil error is a permanent refusal, returned
	// unwrapped by EnsureRunning. Required.
	CanHost func() error
	// Launch selects how the child is started; nil means ExecLaunch (direct
	// exec). No strategy bypasses the Available/CanHost gates, the RLIMIT_NPROC
	// child cap, or reaping.
	Launch LaunchStrategy

	clock clock
}

// LaunchStrategy builds the detached child's command. EnsureRunning applies the
// RLIMIT_NPROC cap, reaping, and socket poll to whatever it returns, so no
// strategy can skip them. Sealed to the two package implementations, ExecLaunch
// and AppLaunchNew.
type LaunchStrategy interface {
	launch(s Spawn) (*exec.Cmd, *os.File, error)
}

// ExecLaunch is the default LaunchStrategy: it direct-execs the child binary
// (Spawn.ExecPath or os.Executable) in its own session.
type ExecLaunch struct{}

func (ExecLaunch) launch(s Spawn) (*exec.Cmd, *os.File, error) { return s.childCmd() }

// EnsureRunning ensures a child serves Socket, spawning a detached one and
// waiting for its socket when needed. Every could-not-start-or-reach failure
// wraps ErrChildUnavailable (transient; drivers retry); a CanHost refusal and a
// strategy's ErrAppLaunchUnsupported are returned unwrapped (permanent). The
// socket poll honors ctx; the detached child outlives it.
func (s Spawn) EnsureRunning(ctx context.Context) error {
	if s.Available() {
		return nil
	}
	if err := s.CanHost(); err != nil {
		return err
	}
	cmd, logFile, err := s.strategy().launch(s)
	if errors.Is(err, ErrAppLaunchUnsupported) {
		return err
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrChildUnavailable, err)
	}
	// The child holds its own descriptor; this one is ours.
	defer logFile.Close()
	// Cap the child subtree's RLIMIT_NPROC across the fork so a runaway re-spawn
	// loop starves at EAGAIN instead of fork-bombing the host (darwin only).
	if err := withChildNprocCap(cmd.Start); err != nil {
		return fmt.Errorf("%w: spawn child: %w", ErrChildUnavailable, err)
	}
	reap(cmd)

	timeout := s.timeout()
	clk := clockOrReal(s.clock)
	deadline := clk.Now().Add(timeout)
	for clk.Now().Before(deadline) {
		if s.Available() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for child on %s: %w", ErrChildUnavailable, s.Socket, ctx.Err())
		case <-clk.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w: child did not come up on %s within %s; check %s", ErrChildUnavailable, s.Socket, timeout, s.LogPath)
}

func (s Spawn) strategy() LaunchStrategy {
	if s.Launch == nil {
		return ExecLaunch{}
	}
	return s.Launch
}

func (s Spawn) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultSpawnTimeout
}

func (s Spawn) childCmd() (*exec.Cmd, *os.File, error) {
	exe := s.ExecPath
	if exe == "" {
		var err error
		exe, err = os.Executable()
		if err != nil {
			return nil, nil, fmt.Errorf("resolve executable: %w", err)
		}
	}
	logFile, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open child log: %w", err)
	}
	cmd := exec.Command(exe, s.Args...)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd, logFile, nil
}

// reap waits out the child so its exit never strands a zombie: Setsid detaches
// the session, not the parent-child link, and Process.Release would not reap.
func reap(cmd *exec.Cmd) {
	go func() { _ = cmd.Wait() }()
}
