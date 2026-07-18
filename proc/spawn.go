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
// its own session when needed; racing spawns lose to socket ownership.
// Long-lived children MUST call CloseInheritedFDs first in main — fork+exec
// leaks every non-CLOEXEC parent fd and Go has no spawner-side sweep.
type Spawn struct {
	Socket  string
	LogPath string
	Args    []string
	// Timeout bounds the socket wait; zero means DefaultSpawnTimeout.
	Timeout time.Duration
	// ExecPath, when set, is the binary the child execs instead of os.Executable().
	ExecPath string
	// Available reports whether a child is already serving Socket. Required.
	Available func() bool
	// CanHost gates the spawn; a non-nil error is a permanent refusal returned unwrapped. Required.
	CanHost func() error
	// Gate, when set, admits each actual child launch and records it durably before the launch; an error withholds the launch and is returned unwrapped.
	Gate func(ctx context.Context) error
	// Launch selects how the child is started; nil means ExecLaunch. No strategy bypasses the gates, the RLIMIT_NPROC cap, or reaping.
	Launch LaunchStrategy

	clock clock
}

// LaunchStrategy builds the detached child's command; EnsureRunning applies the
// RLIMIT_NPROC cap, reaping, and socket poll to whatever it returns.
type LaunchStrategy interface {
	launch(s Spawn) (*exec.Cmd, *os.File, error)
}

// ExecLaunch is the default LaunchStrategy: it direct-execs the child binary
// (Spawn.ExecPath or os.Executable) in its own session.
type ExecLaunch struct{}

func (ExecLaunch) launch(s Spawn) (*exec.Cmd, *os.File, error) { return s.childCmd() }

// EnsureRunning ensures a child serves Socket, spawning a detached one when needed;
// a CanHost refusal and ErrAppLaunchUnsupported return unwrapped, else wraps ErrChildUnavailable.
func (s Spawn) EnsureRunning(ctx context.Context) error {
	if s.Available() {
		return nil
	}
	if err := s.CanHost(); err != nil {
		return err
	}
	if s.Gate != nil {
		if err := s.Gate(ctx); err != nil {
			return err
		}
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
	// Cap the child subtree's RLIMIT_NPROC so a runaway re-spawn loop starves at EAGAIN instead of fork-bombing the host (darwin only).
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

// Setsid detaches the session, not the parent-child link: without this wait the exit strands a zombie.
func reap(cmd *exec.Cmd) {
	go func() { _ = cmd.Wait() }()
}
