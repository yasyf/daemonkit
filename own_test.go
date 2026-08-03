package daemonkit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/paths"
)

func ownedScope(t *testing.T) *Owned {
	t.Helper()
	return ownedScopeAt(t, filepath.Join(t.TempDir(), "daemon.records"))
}

func ownedScopeAt(t *testing.T, path string) *Owned {
	t.Helper()
	owned, err := OwnProcesses(bounded(t, 30*time.Second), path)
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = owned.Close(closeCtx)
	})
	return owned
}

func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// TestOwnProcessesExcludesASecondScope is D3's first half: the lock identity is
// the record path, so a second owner of one record cannot open and cannot then
// reclaim the first owner's children.
func TestOwnProcessesExcludesASecondScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.records")
	first := ownedScopeAt(t, path)

	busy, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err := OwnProcesses(busy, path)
	if !errors.Is(err, durable.ErrLockBusy) {
		t.Fatalf("second OwnProcesses() = %v, want durable.ErrLockBusy", err)
	}
	if errors.Is(err, ErrBusy) {
		t.Fatal("contention reported ErrBusy, which means a live incumbent owns the socket — the wrong register")
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelClose()
	if err := first.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := OwnProcesses(bounded(t, 20*time.Second), path); err != nil {
		t.Fatalf("OwnProcesses() after the first scope closed = %v", err)
	}
}

// TestOwnProcessesAgainstAServingDaemonIsRefused is D3's second half, and the
// hazard the directive names: Daemon.RecordPath is exported, so the call is
// constructible; the lock is what makes the reclaim that would kill the live
// daemon's children unreachable.
func TestOwnProcessesAgainstAServingDaemonIsRefused(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkown", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	product := &stubProduct{}
	done := make(chan error, 1)
	go func() {
		_, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return product, nil })
		done <- err
	}()
	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)
	ctx := bounded(t, 20*time.Second)
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}

	busy, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	scope, err := OwnProcesses(busy, d.RecordPath())
	if err == nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		_ = scope.Close(closeCtx)
		cancelClose()
		t.Fatal("OwnProcesses opened a scope over a serving daemon's record; its reclaim would kill the daemon's live children")
	}
	if !errors.Is(err, durable.ErrLockBusy) {
		t.Fatalf("OwnProcesses() = %v, want durable.ErrLockBusy", err)
	}

	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
}

func TestOwnProcessesRequiresADeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.records")
	if _, err := OwnProcesses(context.Background(), path); err == nil {
		t.Fatal("OwnProcesses() accepted a context without a deadline")
	}
}

func TestOwnedRunReportsStreamsExitAndTruncation(t *testing.T) {
	owned := ownedScope(t)

	t.Run("clean run", func(t *testing.T) {
		result, err := owned.Run(bounded(t, 20*time.Second), Cmd{
			Path:  "/bin/cat",
			Stdin: []byte("through the pipe"),
			Exec:  ServingSameUser(),
		})
		if err != nil {
			t.Fatalf("Run() = %v", err)
		}
		if !bytes.Equal(result.Stdout, []byte("through the pipe")) {
			t.Fatalf("Stdout = %q", result.Stdout)
		}
		if result.Exit.Code != 0 || result.Exit.Signal != 0 {
			t.Fatalf("Exit = %+v, want a clean exit", result.Exit)
		}
	})

	t.Run("nonzero exit is an ExitError with the streams", func(t *testing.T) {
		result, err := owned.Run(bounded(t, 20*time.Second), Cmd{
			Path: "/bin/sh",
			Args: []string{"-c", "echo out; echo err >&2; exit 3"},
			Exec: ServingSameUser(),
		})
		var exitErr *ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("Run() = %v, want an *ExitError", err)
		}
		if exitErr.Exit.Code != 3 {
			t.Fatalf("ExitError.Exit = %+v, want code 3", exitErr.Exit)
		}
		if strings.TrimSpace(string(result.Stdout)) != "out" || strings.TrimSpace(string(result.Stderr)) != "err" {
			t.Fatalf("streams were discarded on failure: stdout=%q stderr=%q", result.Stdout, result.Stderr)
		}
	})

	t.Run("overflow is ErrTruncated with the capped bytes", func(t *testing.T) {
		result, err := owned.Run(bounded(t, 20*time.Second), Cmd{
			Path:      "/bin/sh",
			Args:      []string{"-c", "printf 0123456789abcdef"},
			MaxOutput: 8,
			Exec:      ServingSameUser(),
		})
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("Run() = %v, want ErrTruncated", err)
		}
		if !bytes.Equal(result.Stdout, []byte("01234567")) {
			t.Fatalf("Stdout = %q, want the capped prefix retained beside the error", result.Stdout)
		}
	})

	t.Run("deadline surfaces as context.DeadlineExceeded", func(t *testing.T) {
		result, err := owned.Run(bounded(t, 2*time.Second), Cmd{
			Path: "/bin/sleep",
			Args: []string{"60"},
			Exec: ServingSameUser(),
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() = %v, want an error matching context.DeadlineExceeded", err)
		}
		if result.Exit.Reap != ReapTerminated {
			t.Fatalf("Exit.Reap = %d, want ReapTerminated", result.Exit.Reap)
		}
	})

	t.Run("no deadline is refused", func(t *testing.T) {
		_, err := owned.Run(context.Background(), Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()})
		if err == nil {
			t.Fatal("Run() accepted a context without a deadline")
		}
		if !strings.HasPrefix(err.Error(), "daemonkit: ") {
			t.Fatalf("Run() = %q, want the daemonkit register, not proc's", err)
		}
	})
}

func TestOwnedRunRestatesTheBoundaryInThisPackagesRegister(t *testing.T) {
	owned := ownedScope(t)
	_, err := owned.Run(bounded(t, 5*time.Second), Cmd{Path: "bin/echo", Exec: ServingSameUser()})
	if err == nil {
		t.Fatal("Run() accepted a relative path")
	}
	if !strings.HasPrefix(err.Error(), "daemonkit: ") {
		t.Fatalf("Run() = %q, want the daemonkit register, not proc's", err)
	}
}

func TestSpawnAndAdoptRequireADeadline(t *testing.T) {
	owned := ownedScope(t)
	ctx := owned.Ctx(context.Background())
	undeadlined := map[string]func() error{
		"Owned.Spawn": func() error {
			_, err := owned.Spawn(context.Background(), Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()}, ChannelNone, nil)
			return err
		},
		"Owned.Adopt": func() error { _, err := owned.Adopt(context.Background(), os.Getpid()); return err },
		"Ctx.Spawn": func() error {
			_, err := ctx.Spawn(context.Background(), Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()}, ChannelNone, nil)
			return err
		},
		"Ctx.Adopt": func() error { _, err := ctx.Adopt(context.Background(), os.Getpid()); return err },
	}
	for verb, call := range undeadlined {
		err := call()
		if err == nil {
			t.Fatalf("%s() accepted a context without a deadline", verb)
		}
		if !strings.HasPrefix(err.Error(), "daemonkit: ") {
			t.Fatalf("%s() = %q, want the daemonkit register", verb, err)
		}
	}
}

func TestZeroCtxRefusesTheVerbs(t *testing.T) {
	var zero Ctx
	if _, err := zero.Run(bounded(t, time.Second), Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()}); !errors.Is(err, errZeroCtx) {
		t.Fatalf("Ctx.Run() = %v, want the zero-Ctx refusal", err)
	}
	if _, err := zero.Spawn(bounded(t, time.Second), Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()}, ChannelNone, nil); !errors.Is(err, errZeroCtx) {
		t.Fatalf("Ctx.Spawn() = %v, want the zero-Ctx refusal", err)
	}
	if _, err := zero.Adopt(bounded(t, time.Second), os.Getpid()); !errors.Is(err, errZeroCtx) {
		t.Fatalf("Ctx.Adopt() = %v, want the zero-Ctx refusal", err)
	}
}

// TestOwnedCtxRunsProductCodeUnchanged is D15: a Ctx minted from a CLI-owned
// scope answers the same verbs Serve's does, so product code has one shape.
func TestOwnedCtxRunsProductCodeUnchanged(t *testing.T) {
	owned := ownedScope(t)
	ctx := owned.Ctx(context.Background())
	if ctx.Context == nil || ctx.Report == nil || ctx.Stop == nil {
		t.Fatalf("Owned.Ctx() = %+v, want every field populated", ctx)
	}
	ctx.Report([]byte("no health lane here"))

	result, err := ctx.Run(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/echo",
		Args: []string{"from a CLI scope"},
		Exec: ServingSameUser(),
	})
	if err != nil {
		t.Fatalf("Ctx.Run() = %v", err)
	}
	if strings.TrimSpace(string(result.Stdout)) != "from a CLI scope" {
		t.Fatalf("Stdout = %q", result.Stdout)
	}

	ctx.Stop(nil)
	select {
	case <-ctx.Context.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Ctx.Stop did not cancel the minted Context")
	}
}

// TestOwnedCloseSettlesEverythingItOwns is the CLI scope's whole contract: the
// caller's "did everything drain" answer is err == nil.
func TestOwnedCloseSettlesEverythingItOwns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.records")
	owned, err := OwnProcesses(bounded(t, 20*time.Second), path)
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sleep",
		Args: []string{"600"},
		Exec: ServingSameUser(),
	}, ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	pid := child.PID()

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := owned.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want every child settled", err)
	}
	if alive(pid) {
		t.Fatalf("pid %d survived the scope's Close", pid)
	}

	reopened, err := OwnProcesses(bounded(t, 20*time.Second), path)
	if err != nil {
		t.Fatalf("OwnProcesses() after Close = %v; the lock was not released", err)
	}
	if len(reopened.Reclaimed()) != 0 {
		t.Fatalf("Reclaimed() = %v, want nothing left for the next generation", reopened.Reclaimed())
	}
	reopenCtx, cancelReopen := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelReopen()
	if err := reopened.Close(reopenCtx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// TestServeSettlesChildrenSpawnedThroughCtx is StageChildren's guarantee: the
// product spawns and never stops, and Serve's own ladder is what proves the
// child gone before the process leaves.
func TestServeSettlesChildrenSpawnedThroughCtx(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkctxspawn", Schemas: []Schema{"test.v1"}, Shutdown: Grace(10 * time.Second)}
	spawned := make(chan int, 1)
	done := make(chan Drained, 1)
	go func() {
		drained, _ := Serve(context.Background(), d, func(x Ctx) (Product, error) {
			child, err := x.Spawn(bounded(t, 20*time.Second), Cmd{
				Path: "/bin/sleep",
				Args: []string{"600"},
				Exec: ServingSameUser(),
			}, ChannelNone, nil)
			if err != nil {
				return nil, err
			}
			spawned <- child.PID()
			return &stubProduct{}, nil
		})
		done <- drained
	}()

	var pid int
	select {
	case pid = <-spawned:
	case <-time.After(20 * time.Second):
		t.Fatal("the product never spawned through Ctx")
	}
	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)
	ctx := bounded(t, 20*time.Second)
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case drained := <-done:
		if len(drained.Abandoned) != 0 {
			t.Fatalf("Abandoned = %v, want nothing abandoned", drained.Abandoned)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
	if alive(pid) {
		t.Fatalf("the Ctx-spawned child %d survived Serve's shutdown ladder", pid)
	}
}

// parkedRun publishes its own pid and then parks. exec replaces the shell, so
// the published pid is the process settlement must prove gone.
func parkedRun(pidFile string) Cmd {
	return Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "echo $$ > " + pidFile + "; exec sleep 600"},
		Exec: ServingSameUser(),
	}
}

func awaitPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && strings.HasSuffix(string(raw), "\n") {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if convErr == nil && pid > 0 {
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the run's child never published its pid to %s", path)
	return 0
}

// TestOwnedCloseSettlesAnInFlightRun is Close's contract over the one verb
// that returns nothing to hold: a Run still executing is a live child, so a
// clean-drain answer over it would be a lie.
func TestOwnedCloseSettlesAnInFlightRun(t *testing.T) {
	dir := t.TempDir()
	owned, err := OwnProcesses(bounded(t, 20*time.Second), filepath.Join(dir, "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	pidFile := filepath.Join(dir, "run.pid")
	runCtx := bounded(t, 120*time.Second)
	type outcome struct {
		result RunResult
		err    error
	}
	ran := make(chan outcome, 1)
	go func() {
		result, runErr := owned.Run(runCtx, parkedRun(pidFile))
		ran <- outcome{result: result, err: runErr}
	}()
	pid := awaitPID(t, pidFile)

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := owned.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want every live child settled", err)
	}
	if alive(pid) {
		t.Fatalf("the in-flight Run's child %d survived a Close that reported a clean drain", pid)
	}

	select {
	case got := <-ran:
		var exitErr *ExitError
		if !errors.As(got.err, &exitErr) {
			t.Fatalf("the settled Run returned %v, want an *ExitError naming the fatal signal", got.err)
		}
		if exitErr.Exit.Signal != syscall.SIGTERM {
			t.Fatalf("ExitError.Exit = %+v, want the settlement ladder's SIGTERM", exitErr.Exit)
		}
		if got.result.Exit.Reap != ReapTerminated {
			t.Fatalf("RunResult.Exit.Reap = %d, want ReapTerminated", got.result.Exit.Reap)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the in-flight Run never returned after its child was settled")
	}
}

// runForksADescendant's child forks a descendant onto its own streams — the
// run's pipes stay with the leader — publishes both pids, then parks. Leader
// and descendant both keep the marker in their argv — neither execs it away —
// so a survivor is identified rather than merely observed at a pid. Neither
// process ignores SIGTERM, so what the descendant's survival measures is
// scope, not ladder patience.
func runForksADescendant(pidFile, holderFile, marker string) Cmd {
	publish := func(path, pid string) string {
		return "echo " + pid + " > " + path + ".tmp; mv " + path + ".tmp " + path + "; "
	}
	holder := "/bin/sh -c 'while :; do sleep 60; done # " + marker + "-descendant' >/dev/null 2>&1 & "
	script := holder + publish(holderFile, "$!") + publish(pidFile, "$$") + "wait # " + marker + "-leader"
	return Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", script},
		Exec: ServingSameUser(),
	}
}

// TestOwnedCloseDrainsARunChildsDescendants is Close's "every live child …
// terminated and proven gone" over the shape that made the claim false. A Run
// child that inherited the caller's process group has no scope of its own, so
// terminating it leaves its own fork running under nobody's ownership — and
// Close still answers nil, which is the lie. Run now spawns into a dedicated
// session, so the descendant is inside the only scope the kernel offers.
func TestOwnedCloseDrainsARunChildsDescendants(t *testing.T) {
	dir := t.TempDir()
	owned, err := OwnProcesses(bounded(t, 20*time.Second), filepath.Join(dir, "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	pidFile := filepath.Join(dir, "run.pid")
	holderFile := filepath.Join(dir, "holder.pid")
	marker := fmt.Sprintf("dkdrain%d", time.Now().UnixNano())
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		_, _ = owned.Run(bounded(t, 120*time.Second), runForksADescendant(pidFile, holderFile, marker))
	}()
	pid := awaitPID(t, pidFile)
	holder := awaitPID(t, holderFile)

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := owned.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want every live child settled", err)
	}
	if aliveAs(pid, marker+"-leader") {
		t.Fatalf("the in-flight Run's child %d survived Close", pid)
	}
	if aliveAs(holder, marker+"-descendant") {
		t.Fatalf("the Run child's descendant %d survived a Close that answered a clean drain", holder)
	}
	select {
	case <-ran:
	case <-time.After(30 * time.Second):
		t.Fatal("the settled Run never returned")
	}
}

// forkedHolderRun's child forks a descendant that inherits the run's stdout
// pipe and outlives it, then parks ignoring SIGTERM so settlement runs its
// whole ladder — the ignored disposition survives the descendant's own exec.
// Leader and descendant both keep the marker in their argv, so a survivor is
// identified rather than merely observed at a pid.
func forkedHolderRun(pidFile, holderFile, marker string) Cmd {
	publish := func(path, pid string) string {
		return "echo " + pid + " > " + path + ".tmp; mv " + path + ".tmp " + path + "; "
	}
	holder := "/bin/sh -c 'while :; do sleep 60; done # " + marker + "-descendant' & "
	script := "trap '' TERM; " + holder + publish(holderFile, "$!") + publish(pidFile, "$$") + "wait # " + marker + "-leader"
	return Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", script},
		Exec: ServingSameUser(),
	}
}

// TestOwnedCloseDoesNotPinAnInFlightRunToItsOwnDeadline is the other half of
// settling a Run in flight, and the budget a Run child costs. Close terminates
// the child on the scope's budget and then settles its dedicated session on
// the settlement grace no caller deadline covers, so a scope whose Close is
// worth answering nil over is sized for both — and neither the descendant that
// inherited the stdout pipe nor the drain behind it may hold the caller for
// the rest of the run's own, far longer, deadline.
func TestOwnedCloseDoesNotPinAnInFlightRunToItsOwnDeadline(t *testing.T) {
	dir := t.TempDir()
	owned, err := OwnProcesses(bounded(t, 20*time.Second), filepath.Join(dir, "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	pidFile := filepath.Join(dir, "run.pid")
	holderFile := filepath.Join(dir, "holder.pid")
	marker := fmt.Sprintf("dkpipe%d", time.Now().UnixNano())
	ran := make(chan time.Time, 1)
	go func() {
		_, _ = owned.Run(bounded(t, 60*time.Second), forkedHolderRun(pidFile, holderFile, marker))
		ran <- time.Now()
	}()
	pid := awaitPID(t, pidFile)
	holder := awaitPID(t, holderFile)

	settleBy := time.Now().Add(10*time.Second + proc.SettleGrace)
	closeCtx, cancel := context.WithDeadline(context.Background(), settleBy)
	defer cancel()
	if err := owned.Close(closeCtx); err != nil {
		t.Fatalf("Close() = %v, want every live child settled", err)
	}
	if aliveAs(pid, marker+"-leader") {
		t.Fatalf("the in-flight Run's child %d survived Close", pid)
	}
	if aliveAs(holder, marker+"-descendant") {
		t.Fatalf("the descendant %d holding the run's pipe survived Close", holder)
	}

	select {
	case at := <-ran:
		if slack := at.Sub(settleBy); slack > 2*time.Second {
			t.Errorf("the settled Run returned %v past the scope's settlement deadline; the descendant holding the pipe pinned it to the run's own 60s budget", slack)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the settled Run never returned: the descendant holding the pipe pinned the drain")
	}
}

// TestServeSettlesAnInFlightCtxRun is the same contract one layer up: a
// product goroutine inside Ctx.Run when the drain reaches StageChildren is a
// live child, and Drained is what cc-orchestrate gates service on.
func TestServeSettlesAnInFlightCtxRun(t *testing.T) {
	shortHome(t)
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "run.pid")
	d := Daemon{Label: "dkctxrun", Schemas: []Schema{"test.v1"}, Shutdown: Grace(10 * time.Second)}
	ran := make(chan error, 1)
	done := make(chan Drained, 1)
	go func() {
		drained, _ := Serve(context.Background(), d, func(x Ctx) (Product, error) {
			runCtx := bounded(t, 120*time.Second)
			go func() {
				_, err := x.Run(runCtx, parkedRun(pidFile))
				ran <- err
			}()
			return &stubProduct{}, nil
		})
		done <- drained
	}()
	pid := awaitPID(t, pidFile)

	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)
	ctx := bounded(t, 20*time.Second)
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}

	select {
	case drained := <-done:
		if len(drained.Abandoned) != 0 {
			t.Fatalf("Abandoned = %v, want nothing abandoned", drained.Abandoned)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
	if alive(pid) {
		t.Fatalf("the Ctx.Run child %d survived a shutdown ladder that published a clean drain", pid)
	}
	select {
	case err := <-ran:
		var exitErr *ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("the settled Ctx.Run returned %v, want an *ExitError naming the fatal signal", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the in-flight Ctx.Run never returned after its child was settled")
	}
}

// TestOwnedCloseFaultsOnAChildWhoseExitWasNeverProven is the whole point of
// the clean-drain answer. A settlement whose budget is already spent runs the
// ladder to its end and publishes with nothing proved — the shape a child in
// uninterruptible sleep reaches on any deadline — and a Close that answered
// nil over that terminal would tell the caller everything drained.
func TestOwnedCloseFaultsOnAChildWhoseExitWasNeverProven(t *testing.T) {
	dir := t.TempDir()
	owned, err := OwnProcesses(bounded(t, 20*time.Second), filepath.Join(dir, "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	// The ladder can win the race against its own expired timer, so the
	// unproven terminal is retried until it arises rather than assumed — the
	// shape unprovenChild reaches the same way. A proven exit settles its child
	// and drops it from the scope, so only an unproven one is left for Close.
	unproven := false
	for attempt := range 20 {
		child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
			Path: "/bin/sleep",
			Args: []string{"600"},
			Exec: ServingSameUser(),
		}, ChannelNone, nil)
		if err != nil {
			t.Fatalf("Spawn() attempt %d = %v", attempt, err)
		}
		spent, cancelSpent := context.WithTimeout(context.Background(), time.Nanosecond)
		_, stopErr := child.Stop(spent)
		cancelSpent()
		if !errors.Is(stopErr, ErrUnsettled) {
			t.Fatalf("Stop() = %v, want ErrUnsettled", stopErr)
		}
		if exit := <-child.Done(); exit.Reap == ReapUndetermined {
			unproven = true
			break
		}
	}
	if !unproven {
		t.Fatal("20 settlements on a spent budget each proved their exit, so there is no unproven terminal to close over")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if closeErr := owned.Close(closeCtx); !errors.Is(closeErr, ErrUnsettled) {
		t.Fatalf("Close() = %v over a child whose exit was never proven, want ErrUnsettled", closeErr)
	}
}

// publishingRun publishes its pid and exits at once, so a verb that was
// refused and a verb that quietly ran are told apart by the file rather than
// by the error alone.
func publishingRun(pidFile string) Cmd {
	return Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "echo $$ > " + pidFile},
		Exec: ServingSameUser(),
	}
}

func pidWithin(path string, within time.Duration) int {
	deadline := time.Now().Add(within)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw))); convErr == nil && pid > 0 {
				return pid
			}
		}
		if !time.Now().Before(deadline) {
			return 0
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// parkedHelper is a live process outside this scope for Adopt to name. It is
// never os.Getpid(): a refusal that failed to refuse would record the test
// process itself as this generation's child, and the settle behind it would
// then stop the test.
func parkedHelper(t *testing.T) int {
	t.Helper()
	helper := exec.Command("/bin/sleep", "600")
	if err := helper.Start(); err != nil {
		t.Fatalf("start the adopt helper: %v", err)
	}
	t.Cleanup(func() {
		_ = helper.Process.Kill()
		_ = helper.Wait()
	})
	return helper.Process.Pid
}

// TestSettlingScopeRefusesEveryVerbBeforeItStartsAnything is the scope's own
// boundary, stated over both shapes a settling scope has. A closed scope
// refuses in this package's register rather than leaking the record store's
// wording. A scope that has only settled — Serve's drain order, where the
// store stays open behind the children tail — refuses on the same terms and
// for a harder reason: nothing else there would refuse at all, so a verb
// admitted past this boundary would spawn, register, and be missed.
func TestSettlingScopeRefusesEveryVerbBeforeItStartsAnything(t *testing.T) {
	states := []struct {
		name  string
		enter func(*testing.T, *Owned)
	}{
		{"closed", func(t *testing.T, o *Owned) {
			closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := o.Close(closeCtx); err != nil {
				t.Fatalf("Close() = %v", err)
			}
		}},
		{"settled with the record store still open", func(t *testing.T, o *Owned) {
			settleCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := o.settle(settleCtx); err != nil {
				t.Fatalf("settle() = %v", err)
			}
		}},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			dir := t.TempDir()
			owned := ownedScopeAt(t, filepath.Join(dir, "daemon.records"))
			helper := parkedHelper(t)
			state.enter(t, owned)

			verbs := []struct {
				name string
				call func(pidFile string) error
			}{
				{"Owned.Run", func(pidFile string) error {
					_, err := owned.Run(bounded(t, 10*time.Second), publishingRun(pidFile))
					return err
				}},
				{"Owned.Spawn", func(pidFile string) error {
					child, err := owned.Spawn(bounded(t, 10*time.Second), publishingRun(pidFile), ChannelNone, nil)
					if child != nil {
						t.Errorf("Owned.Spawn() returned child %d on a settling scope", child.PID())
					}
					return err
				}},
				{"Owned.Adopt", func(string) error {
					tracked, err := owned.Adopt(bounded(t, 10*time.Second), helper)
					if tracked != nil {
						t.Errorf("Owned.Adopt() tracked %d on a settling scope", tracked.PID())
					}
					return err
				}},
			}
			for _, verb := range verbs {
				pidFile := filepath.Join(dir, verb.name+".pid")
				err := verb.call(pidFile)
				if !errors.Is(err, errScopeSettling) {
					t.Errorf("%s() on a %s scope = %v, want the scope's own refusal", verb.name, state.name, err)
				}
				if err == nil || !strings.HasPrefix(err.Error(), "daemonkit: ") {
					t.Errorf("%s() = %v, want the daemonkit register, not proc's", verb.name, err)
				}
				if pid := pidWithin(pidFile, 2*time.Second); pid != 0 {
					_ = syscall.Kill(-pid, syscall.SIGKILL)
					_ = syscall.Kill(pid, syscall.SIGKILL)
					t.Errorf("%s() on a %s scope started pid %d; the refusal has to land before anything runs", verb.name, state.name, pid)
				}
			}
			if !alive(helper) {
				t.Errorf("the adopt helper %d was stopped by a scope that refused to adopt it", helper)
			}
		})
	}
}

// TestSpentContextIsRefusedInThisPackagesRegister is the same class one ctx
// away: a context whose budget is already gone is refused at the boundary,
// before a child exists to abort, and never as the record store's wording.
func TestSpentContextIsRefusedInThisPackagesRegister(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.records")
	owned := ownedScopeAt(t, path)
	spent, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-spent.Done()

	verbs := map[string]func() error{
		"Owned.Run": func() error {
			_, err := owned.Run(spent, Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()})
			return err
		},
		"Owned.Spawn": func() error {
			_, err := owned.Spawn(spent, Cmd{Path: "/usr/bin/true", Exec: ServingSameUser()}, ChannelNone, nil)
			return err
		},
		"Owned.Adopt":  func() error { _, err := owned.Adopt(spent, os.Getpid()); return err },
		"OwnProcesses": func() error { _, err := OwnProcesses(spent, path); return err },
	}
	for verb, call := range verbs {
		err := call()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s() on a spent context = %v, want an error matching context.DeadlineExceeded", verb, err)
			continue
		}
		if !strings.HasPrefix(err.Error(), "daemonkit: ") {
			t.Errorf("%s() = %q, want the daemonkit register, not the record store's", verb, err)
		}
	}
}

// sessionHolderCmd is a dedicated-session leader that forks a descendant into
// its own session, publishes both pids, and parks ignoring SIGTERM.
func sessionHolderCmd(dir string, attempt int) (c Cmd, pidFile, holderFile string) {
	pidFile = filepath.Join(dir, "leader"+strconv.Itoa(attempt)+".pid")
	holderFile = filepath.Join(dir, "holder"+strconv.Itoa(attempt)+".pid")
	publish := func(path, pid string) string {
		return "echo " + pid + " > " + path + ".tmp; mv " + path + ".tmp " + path + "; "
	}
	return Cmd{
		Path:    "/bin/sh",
		Args:    []string{"-c", "trap '' TERM; sleep 600 & " + publish(holderFile, "$!") + publish(pidFile, "$$") + "exec sleep 600"},
		Session: true,
		Exec:    ServingSameUser(),
	}, pidFile, holderFile
}

// unprovenChild leaves the scope holding a child whose terminal proved nothing
// and whose session survivor is still live: a settlement demanded on a spent
// budget runs the whole ladder and publishes ReapUndetermined, and drive skips
// session settlement entirely when the leader proved nothing, so the fork is
// never signalled. The ladder can still win the race against its own expired
// timer, so the shape is retried rather than assumed.
func unprovenChild(spawn func(Cmd) (*Child, error), dir string) (holder int, err error) {
	for attempt := range 20 {
		c, pidFile, holderFile := sessionHolderCmd(dir, attempt)
		child, spawnErr := spawn(c)
		if spawnErr != nil {
			return 0, fmt.Errorf("spawn: %w", spawnErr)
		}
		leader, leaderErr := readPID(pidFile)
		if leaderErr != nil {
			return 0, leaderErr
		}
		holder, err = readPID(holderFile)
		if err != nil {
			return 0, err
		}
		spent, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		_, stopErr := child.Stop(spent)
		cancel()
		if !errors.Is(stopErr, ErrUnsettled) {
			return 0, fmt.Errorf("Stop() = %v, want ErrUnsettled", stopErr)
		}
		if exit := <-child.Done(); exit.Reap == ReapUndetermined {
			return holder, nil
		}
		_ = syscall.Kill(-leader, syscall.SIGKILL)
	}
	return 0, errors.New("the settlement ladder proved every exit inside a spent budget")
}

func readPID(path string) (int, error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			if pid, convErr := strconv.Atoi(string(raw[:max(len(raw)-1, 0)])); convErr == nil && pid > 0 {
				return pid, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("%s never carried a pid", path)
}

// TestOwnedCloseFaultsOverALiveSessionSurvivor is the positive control one
// layer down: the scope answers the same shape honestly.
func TestOwnedCloseFaultsOverALiveSessionSurvivor(t *testing.T) {
	dir := t.TempDir()
	owned, err := OwnProcesses(bounded(t, 20*time.Second), filepath.Join(dir, "daemon.records"))
	if err != nil {
		t.Fatalf("OwnProcesses() = %v", err)
	}
	holder, err := unprovenChild(func(c Cmd) (*Child, error) {
		return owned.Spawn(bounded(t, 20*time.Second), c, ChannelNone, nil)
	}, dir)
	if err != nil {
		t.Fatalf("stage an unproven child: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(holder, syscall.SIGKILL) })

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	closeErr := owned.Close(closeCtx)
	if !alive(holder) {
		t.Fatalf("the session survivor %d was gone, so this run proved nothing", holder)
	}
	if !errors.Is(closeErr, ErrUnsettled) {
		t.Fatalf("Close() = %v with the session survivor %d live, want ErrUnsettled", closeErr, holder)
	}
}

// TestServeAbandonsChildrenItCouldNotProveGone is the same shape one layer up.
// StageChildren's error IS the "did everything drain" answer, so a ladder that
// classified it by budget alone published every stage settled — and released
// the flock — over a process still in the table.
func TestServeAbandonsChildrenItCouldNotProveGone(t *testing.T) {
	shortHome(t)
	dir := t.TempDir()
	d := Daemon{Label: "dkunproven", Schemas: []Schema{"test.v1"}, Shutdown: Grace(10 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	staged := make(chan int, 1)
	done := serveInBackground(ctx, t, d, func(x Ctx) (Product, error) {
		holder, err := unprovenChild(func(c Cmd) (*Child, error) {
			return x.Spawn(bounded(t, 20*time.Second), c, ChannelNone, nil)
		}, dir)
		if err != nil {
			return nil, err
		}
		staged <- holder
		return &stubProduct{}, nil
	})

	var holder int
	select {
	case holder = <-staged:
	case <-time.After(60 * time.Second):
		t.Fatal("the product never reached an unproven terminal")
	}
	t.Cleanup(func() { _ = syscall.Kill(holder, syscall.SIGKILL) })
	cancel()

	select {
	case out := <-done:
		t.Fatalf("Serve returned %+v (err %v) with the unproven child's session survivor %d alive=%t: the ladder published a clean drain over it",
			out.drained, out.err, holder, alive(holder))
	case <-time.After(5 * time.Second):
	}
	if !alive(holder) {
		t.Fatalf("the session survivor %d was gone, so this run proved nothing", holder)
	}

	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal self: %v", err)
	}
	select {
	case out := <-done:
		if len(out.drained.Abandoned) != 1 || out.drained.Abandoned[0] != StageChildren {
			t.Fatalf("Abandoned = %v, want [StageChildren]", out.drained.Abandoned)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the parked process did not leave on SIGTERM")
	}
}

// gatedServing parks a verb inside its verification window: the exec posture
// runs against the suspended child, after its durable record and before its
// release, which is exactly the instant a process this scope started exists
// and the verb has not yet registered it. Blocking there holds that instant
// open for as long as a test needs it.
type gatedServing struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedServing) requirement() *Requirement {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return nil
}

func newGatedServing() *gatedServing {
	return &gatedServing{entered: make(chan struct{}), release: make(chan struct{})}
}

// TestOwnedCloseWaitsOutAVerbStillStartingAChild is the admission/registration
// atomicity. A verb is admitted before it can spawn, so a Close whose snapshot
// lands while that verb is mid-spawn observes the admission itself and waits
// the verb out rather than answering nil over the process it already started.
func TestOwnedCloseWaitsOutAVerbStillStartingAChild(t *testing.T) {
	verbs := []struct {
		name  string
		start func(o *Owned, c Cmd) error
	}{
		{"Run", func(o *Owned, c Cmd) error {
			_, err := o.Run(bounded(t, 120*time.Second), c)
			return err
		}},
		{"Spawn", func(o *Owned, c Cmd) error {
			_, err := o.Spawn(bounded(t, 120*time.Second), c, ChannelNone, nil)
			return err
		}},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "daemon.records")
			owned, err := OwnProcesses(bounded(t, 30*time.Second), path)
			if err != nil {
				t.Fatalf("OwnProcesses() = %v", err)
			}
			gate := newGatedServing()
			cmd := parkedRun(filepath.Join(dir, "child.pid"))
			cmd.Exec = Serving{policy: gate}
			started := make(chan error, 1)
			go func() { started <- verb.start(owned, cmd) }()
			<-gate.entered

			closed := make(chan error, 1)
			go func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				closed <- owned.Close(closeCtx)
			}()
			select {
			case err := <-closed:
				close(gate.release)
				t.Fatalf("Close() = %v while %s was still starting a child that already exists", err, verb.name)
			case <-time.After(time.Second):
			}

			close(gate.release)
			if err := <-closed; err != nil {
				t.Fatalf("Close() = %v, want the child the released %s registered settled", err, verb.name)
			}
			next := ownedScopeAt(t, path)
			if left := next.Reclaimed(); len(left) != 0 {
				t.Fatalf("the generation after a nil Close reclaimed %+v; the %s's child outlived the Close that answered over it", left, verb.name)
			}
			select {
			case err := <-started:
				t.Logf("%s returned %v", verb.name, err)
			case <-time.After(60 * time.Second):
				t.Fatalf("the settled %s never returned", verb.name)
			}
		})
	}
}

// aliveAs is alive() made identity-safe. kill(pid, 0) succeeds on a zombie and
// on whatever process inherited a recycled pid, so a survivor counts only when
// the process table still shows that pid running the exact command this test
// started.
func aliveAs(pid int, marker string) bool {
	if pid <= 0 {
		return false
	}
	out, err := exec.Command("/bin/ps", "-p", strconv.Itoa(pid), "-o", "stat=,args=").Output()
	if err != nil {
		return false
	}
	state, args, ok := strings.Cut(strings.TrimSpace(string(out)), " ")
	return ok && !strings.HasPrefix(state, "Z") && strings.Contains(args, marker)
}

// racingRun's leader and the descendant it forks both keep the marker in their
// argv — neither execs it away — so a survivor is identified rather than
// merely observed at a pid. Neither traps SIGTERM: a child this scope leaked
// is never signalled at all, so kill-hardness would only lengthen the run
// without changing what a survivor proves.
func racingRun(pidFile, holderFile, marker string) Cmd {
	publish := func(path, pid string) string {
		return "echo " + pid + " > " + path + ".tmp; mv " + path + ".tmp " + path + "; "
	}
	holder := "/bin/sh -c 'while :; do sleep 60; done # " + marker + "-descendant' & "
	script := publish(pidFile, "$$") + holder + publish(holderFile, "$!") + "wait # " + marker + "-leader"
	return Cmd{Path: "/bin/sh", Args: []string{"-c", script}, Exec: ServingSameUser()}
}

// TestOwnedCloseNeverAnswersCleanOverAChildAVerbStarted is the same property
// as TestOwnedCloseWaitsOutAVerbStillStartingAChild with nothing held open by
// a test: real concurrent verbs, real spawn latency, and a Close whose
// snapshot lands wherever the scheduler puts it. The sweep walks the delay
// across the spawn window, and a nil Close over any live leader or descendant
// — or over a record the next generation still has to reclaim — is the defect.
func TestOwnedCloseNeverAnswersCleanOverAChildAVerbStarted(t *testing.T) {
	const attempts = 12
	const runners = 8
	clean := 0
	for attempt := range attempts {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.records")
		marker := fmt.Sprintf("dkrace%d-%d", time.Now().UnixNano(), attempt)
		owned, err := OwnProcesses(bounded(t, 30*time.Second), path)
		if err != nil {
			t.Fatalf("OwnProcesses() = %v", err)
		}
		leaders := make([]string, runners)
		holders := make([]string, runners)
		errs := make([]error, runners)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range runners {
			leaders[i] = filepath.Join(dir, fmt.Sprintf("leader%d.pid", i))
			holders[i] = filepath.Join(dir, fmt.Sprintf("holder%d.pid", i))
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, errs[i] = owned.Run(bounded(t, 120*time.Second), racingRun(leaders[i], holders[i], marker))
			}()
		}
		close(start)
		time.Sleep(time.Duration(attempt)*4*time.Millisecond + 2*time.Millisecond)
		started := time.Now()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := owned.Close(closeCtx)
		elapsed := time.Since(started)
		cancel()

		type survivor struct {
			runner int
			kind   string
			pid    int
		}
		doomed := make([]int, 0, 2*runners)
		var survivors []survivor
		for i := range runners {
			leader, holder := pidWithin(leaders[i], 0), pidWithin(holders[i], 0)
			doomed = append(doomed, leader, holder)
			if aliveAs(leader, marker+"-leader") {
				survivors = append(survivors, survivor{i, "leader", leader})
			}
			if aliveAs(holder, marker+"-descendant") {
				survivors = append(survivors, survivor{i, "descendant", holder})
			}
		}
		var left []Reclaimed
		if closeErr == nil {
			clean++
			next, openErr := OwnProcesses(bounded(t, 30*time.Second), path)
			if openErr != nil {
				t.Fatalf("OwnProcesses() after a clean Close = %v", openErr)
			}
			left = next.Reclaimed()
			closeNext, cancelNext := context.WithTimeout(context.Background(), 20*time.Second)
			_ = next.Close(closeNext)
			cancelNext()
		}
		wg.Wait()
		t.Logf("attempt %d: Close = %v in %v; live at that instant = %d", attempt, closeErr, elapsed.Round(10*time.Millisecond), len(survivors))
		if closeErr == nil {
			for _, s := range survivors {
				t.Errorf("attempt %d: Close() = nil, yet run%d's %s pid=%d was alive at that instant; that Run returned %v",
					attempt, s.runner, s.kind, s.pid, errs[s.runner])
			}
			if len(left) != 0 {
				t.Errorf("attempt %d: Close() = nil, yet the next generation reclaimed %+v", attempt, left)
			}
		}
		wg.Wait()
		for _, pid := range doomed {
			if pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
	if clean == 0 {
		t.Fatalf("no attempt of %d produced a clean Close, so this run proved nothing about what a clean Close means", attempts)
	}
	t.Logf("clean closes = %d of %d attempts", clean, attempts)
}

// TestOwnedCloseReportsAVerbThatNeverFinishedStarting is the other half of
// waiting a verb out: the wait is bounded by the caller's own deadline, and a
// verb still starting when that runs out is a fault naming the verb rather
// than a nil. Whatever it started is durably recorded before it can run, so
// the honest report is that the next generation has it.
func TestOwnedCloseReportsAVerbThatNeverFinishedStarting(t *testing.T) {
	verbs := []struct {
		name  string
		start func(o *Owned, c Cmd) error
	}{
		{"Run", func(o *Owned, c Cmd) error {
			_, err := o.Run(bounded(t, 120*time.Second), c)
			return err
		}},
		{"Spawn", func(o *Owned, c Cmd) error {
			_, err := o.Spawn(bounded(t, 120*time.Second), c, ChannelNone, nil)
			return err
		}},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "daemon.records")
			owned, err := OwnProcesses(bounded(t, 30*time.Second), path)
			if err != nil {
				t.Fatalf("OwnProcesses() = %v", err)
			}
			gate := newGatedServing()
			cmd := publishingRun(filepath.Join(dir, "child.pid"))
			cmd.Exec = Serving{policy: gate}
			started := make(chan error, 1)
			go func() { started <- verb.start(owned, cmd) }()
			<-gate.entered

			closeCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			closeErr := owned.Close(closeCtx)
			cancel()
			close(gate.release)
			if !errors.Is(closeErr, ErrUnsettled) {
				t.Fatalf("Close() = %v over a %s that never finished starting, want ErrUnsettled", closeErr, verb.name)
			}
			if !strings.Contains(closeErr.Error(), verb.name) {
				t.Errorf("Close() = %q, want the fault to name the verb still starting", closeErr)
			}

			select {
			case err := <-started:
				t.Logf("%s returned %v", verb.name, err)
			case <-time.After(60 * time.Second):
				t.Fatalf("the %s never returned", verb.name)
			}
			next := ownedScopeAt(t, path)
			if left := next.Reclaimed(); len(left) != 1 {
				t.Fatalf("the next generation reclaimed %+v, want the one record the unfinished %s left it", left, verb.name)
			}
		})
	}
}

// adoptHelpers starts count live processes outside this scope, each identifiable
// by marker, for Adopt to name.
func adoptHelpers(t *testing.T, count int, marker string) []int {
	t.Helper()
	pids := make([]int, count)
	for i := range count {
		helper := exec.Command("/bin/sh", "-c", "while :; do sleep 60; done # "+marker)
		if err := helper.Start(); err != nil {
			t.Fatalf("start the adopt helper: %v", err)
		}
		t.Cleanup(func() {
			_ = helper.Process.Kill()
			_ = helper.Wait()
		})
		pids[i] = helper.Process.Pid
	}
	return pids
}

// TestARefusedVerbLeavesNoDurableRecord is the durable half of a refusal, and
// the one half a live process can outlive. The record write is handed to the
// store's one writer and outlives the verb's own deadline — the queued write
// lands whether or not the caller is still waiting — so a verb that reports the
// expiry has to retire what it may have written. Otherwise the scope holds a
// durable record of a process the caller was just told it does not own: Close
// settles only what it tracks and answers nil over it, and the next generation
// reclaims the record by terminating a process nothing in this API ever
// admitted. A burst of adoptions is what saturates the writer, so the shape is
// reached on an ordinary deadline rather than a contrived one.
func TestARefusedVerbLeavesNoDurableRecord(t *testing.T) {
	const burst = 32
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.records")
	owned := ownedScopeAt(t, path)
	marker := fmt.Sprintf("dkrefused%d", time.Now().UnixNano())
	helpers := adoptHelpers(t, burst, marker)

	errs := make([]error, burst)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range burst {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			_, errs[i] = owned.Adopt(ctx, helpers[i])
		}()
	}
	close(start)
	wg.Wait()

	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the record store: %v", err)
	}
	refused := 0
	for i := range burst {
		if errs[i] == nil {
			continue
		}
		refused++
		if strings.Contains(string(blob), strconv.Itoa(helpers[i])) {
			t.Errorf("Adopt(%d) = %v, yet the record store still names that pid; a refusal that records is a process nobody admitted",
				helpers[i], errs[i])
		}
	}
	if refused == 0 {
		t.Fatalf("every one of %d concurrent Adopts landed inside its 60ms budget; the burst is what saturates the store's one writer, and a run where none expires exercises no refusal at all", burst)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	closeErr := owned.Close(closeCtx)
	cancel()
	if closeErr != nil {
		t.Fatalf("Close() = %v", closeErr)
	}
	next := ownedScopeAt(t, path)
	if left := next.Reclaimed(); len(left) != 0 {
		t.Fatalf("Close() = nil, yet the next generation reclaimed %+v — the records %d refused Adopts left behind", left, refused)
	}
	for i := range burst {
		if errs[i] != nil && !aliveAs(helpers[i], marker) {
			t.Errorf("helper %d was terminated by a generation that had told the caller Adopt(%d) failed", helpers[i], helpers[i])
		}
	}
	t.Logf("%d of %d concurrent Adopts were refused by the 60ms budget; none left a record", refused, burst)
}
