package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

type controllerRuntimeStub struct {
	mu sync.Mutex

	events     *[]string
	startErr   error
	run        func(context.Context, worker.CommandRequest) (worker.CommandResult, error)
	close      func(context.Context) error
	closeCalls int
}

func (r *controllerRuntimeStub) Start(context.Context) error {
	r.record("start")
	return r.startErr
}

func (r *controllerRuntimeStub) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.record("run:" + strings.Join(task.Args, " "))
	if r.run != nil {
		return r.run(ctx, task)
	}
	return worker.CommandResult{}, nil
}

func (r *controllerRuntimeStub) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()
	r.record("close-runtime")
	if r.close != nil {
		return r.close(ctx)
	}
	return nil
}

func (r *controllerRuntimeStub) record(event string) {
	if r.events != nil {
		*r.events = append(*r.events, event)
	}
}

type controllerStoreStub struct {
	state      controllerState
	events     *[]string
	replaceErr error
	setErr     error
	closeCalls int
}

func (s *controllerStoreStub) Load(context.Context) (controllerState, error) {
	s.record("load")
	return controllerState{
		Desired: copyAgents(s.state.Desired), Applied: copyAgents(s.state.Applied),
		Replacement:       copyReplacement(s.state.Replacement),
		ReplacementCommit: copyReplacementCommit(s.state.ReplacementCommit),
		ReplacementAck:    copyReplacementCommit(s.state.ReplacementAck),
	}, nil
}

func (s *controllerStoreStub) ReplaceDesired(
	_ context.Context,
	desired map[string]Agent,
) (controllerState, error) {
	s.record("replace-desired")
	prior := controllerState{
		Desired:           copyAgents(s.state.Desired),
		Applied:           copyAgents(s.state.Applied),
		Replacement:       copyReplacement(s.state.Replacement),
		ReplacementCommit: copyReplacementCommit(s.state.ReplacementCommit),
		ReplacementAck:    copyReplacementCommit(s.state.ReplacementAck),
	}
	if s.replaceErr != nil {
		return controllerState{}, s.replaceErr
	}
	s.state.Desired = copyAgents(desired)
	return prior, nil
}

func (s *controllerStoreStub) SetReplacement(
	_ context.Context,
	desired map[string]Agent,
	replacement *replacementState,
	commit *replacementCommit,
	acknowledged *replacementCommit,
) (controllerState, error) {
	s.record("set-replacement")
	if s.replaceErr != nil {
		return controllerState{}, s.replaceErr
	}
	s.state.Desired = copyAgents(desired)
	s.state.Replacement = copyReplacement(replacement)
	s.state.ReplacementCommit = copyReplacementCommit(commit)
	s.state.ReplacementAck = copyReplacementCommit(acknowledged)
	return controllerState{
		Desired: copyAgents(s.state.Desired), Applied: copyAgents(s.state.Applied),
		Replacement:       copyReplacement(s.state.Replacement),
		ReplacementCommit: copyReplacementCommit(s.state.ReplacementCommit),
		ReplacementAck:    copyReplacementCommit(s.state.ReplacementAck),
	}, nil
}

func (s *controllerStoreStub) SetApplied(_ context.Context, label string, agent *Agent) error {
	s.record("set-applied:" + label)
	if s.setErr != nil {
		return s.setErr
	}
	if s.state.Applied == nil {
		s.state.Applied = make(map[string]Agent)
	}
	if agent == nil {
		delete(s.state.Applied, label)
	} else {
		s.state.Applied[label] = *agent
	}
	return nil
}

func (s *controllerStoreStub) Close() error {
	s.closeCalls++
	s.record("close-store")
	return nil
}

func (s *controllerStoreStub) record(event string) {
	if s.events != nil {
		*s.events = append(*s.events, event)
	}
}

type controllerReceiptsStub struct {
	events *[]string
	calls  int
	ids    []proc.RecoveryID
	err    error
}

func (r *controllerReceiptsStub) RecoverReapReceipts(
	_ context.Context,
	id proc.RecoveryID,
	_ func(context.Context, proc.ReapReceipt) error,
) (proc.ReapReceiptFloor, error) {
	if id != proc.RecoveryServiceID && id != proc.RecoveryStopControlID {
		return proc.ReapReceiptFloor{}, fmt.Errorf("receipt id = %q", id)
	}
	r.calls++
	r.ids = append(r.ids, id)
	if r.events != nil {
		*r.events = append(*r.events, fmt.Sprintf("recover-receipts:%s", id))
	}
	return proc.ReapReceiptFloor{RecoveryID: id}, r.err
}

func controllerConfig(t *testing.T) ControllerConfig {
	t.Helper()
	dir := t.TempDir()
	return ControllerConfig{
		StatePath:   filepath.Join(dir, "services.db"),
		ProcessPath: filepath.Join(dir, "workers.db"),
		WorkerLimit: 2,
	}
}

func controllerAgent(t *testing.T, label string) Agent {
	t.Helper()
	return Agent{
		Label: label, Program: "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), label+".log"),
		RestartPolicy: RestartAlways,
	}
}

func controllerExecutable(t *testing.T, name string) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func launchctlExit(code int) error { return &worker.ExitError{ExitCode: code} }

func launchctlStub(fn func([]string) (string, error)) func(context.Context, worker.CommandRequest) (worker.CommandResult, error) {
	return func(_ context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
		out, err := fn(task.Args)
		return worker.CommandResult{Stdout: []byte(out)}, err
	}
}

func newTestController(
	t *testing.T,
	state controllerState,
	run func(context.Context, worker.CommandRequest) (worker.CommandResult, error),
	events *[]string,
) (*Controller, *controllerRuntimeStub, *controllerStoreStub, *controllerReceiptsStub) {
	t.Helper()
	runtime := &controllerRuntimeStub{events: events, run: run}
	store := &controllerStoreStub{state: state, events: events}
	receipts := &controllerReceiptsStub{events: events}
	controller, err := newControllerWithRuntime(
		context.Background(), controllerConfig(t), runtime, receipts, store,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := controller.Close(context.Background()); err != nil &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close() = %v", err)
		}
	})
	return controller, runtime, store, receipts
}

func TestControllerConfigRequiresExactDistinctPathsAndCapacity(t *testing.T) {
	valid := controllerConfig(t)
	tests := []struct {
		name string
		edit func(*ControllerConfig)
	}{
		{"relative state", func(c *ControllerConfig) { c.StatePath = "state.db" }},
		{"unclean state", func(c *ControllerConfig) { c.StatePath += "/../state.db" }},
		{"relative process", func(c *ControllerConfig) { c.ProcessPath = "workers.db" }},
		{"same paths", func(c *ControllerConfig) { c.ProcessPath = c.StatePath }},
		{"zero capacity", func(c *ControllerConfig) { c.WorkerLimit = 0 }},
		{"negative capacity", func(c *ControllerConfig) { c.WorkerLimit = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.edit(&config)
			if err := config.validate(); err == nil {
				t.Fatal("validate() accepted invalid config")
			}
		})
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("validate() = %v", err)
	}
}

func TestControllerStatusReportsAbsentWithoutRuntimeConnection(t *testing.T) {
	runtime := &controllerRuntimeStub{run: launchctlStub(func(args []string) (string, error) {
		if !reflect.DeepEqual(args, []string{"print", serviceTarget("com.example.absent")}) {
			t.Fatalf("launchctl args = %v", args)
		}
		return "not found", launchctlExit(launchctlNotLoadedExit)
	})}
	controller := &Controller{
		runtime:   runtime,
		state:     controllerState{Desired: map[string]Agent{}, Applied: map[string]Agent{}},
		closeDone: make(chan struct{}),
	}
	status, err := controller.Status(t.Context(), "com.example.absent")
	if err != nil {
		t.Fatal(err)
	}
	if status != (Status{Label: "com.example.absent"}) {
		t.Fatalf("Status = %#v", status)
	}
}

func TestControllerStatusTreatsPrintNotFoundExitAsAbsent(t *testing.T) {
	runtime := &controllerRuntimeStub{run: launchctlStub(func(args []string) (string, error) {
		if !reflect.DeepEqual(args, []string{"print", serviceTarget("com.example.absent")}) {
			t.Fatalf("launchctl args = %v", args)
		}
		return "Could not find service", launchctlExit(launchctlNotFoundExit)
	})}
	controller := &Controller{
		runtime:   runtime,
		state:     controllerState{Desired: map[string]Agent{}, Applied: map[string]Agent{}},
		closeDone: make(chan struct{}),
	}
	status, err := controller.Status(t.Context(), "com.example.absent")
	if err != nil {
		t.Fatal(err)
	}
	if status != (Status{Label: "com.example.absent"}) {
		t.Fatalf("Status = %#v", status)
	}
}

func TestControllerStatusRequiresExactDesiredAppliedLoadedState(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.exact")
	plist, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := &controllerRuntimeStub{run: launchctlStub(func([]string) (string, error) {
		return "loaded", nil
	})}
	controller := &Controller{
		runtime: runtime,
		state: controllerState{
			Desired: map[string]Agent{agent.Label: agent},
			Applied: map[string]Agent{agent.Label: agent},
		},
		closeDone: make(chan struct{}),
	}
	status, err := controller.Status(t.Context(), agent.Label)
	if err != nil {
		t.Fatal(err)
	}
	want := Status{Label: agent.Label, Desired: true, Applied: true, Loaded: true, Exact: true}
	if status != want {
		t.Fatalf("Status = %#v, want %#v", status, want)
	}
}

func TestControllerRecoveryConvergesBeforeAcknowledgingReceipts(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.recover")
	var events []string
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, store, receipts := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent},
		Applied: map[string]Agent{},
	}, run, &events)
	_ = controller
	if receipts.calls != 2 {
		t.Fatalf("receipt recovery calls = %d, want 2", receipts.calls)
	}
	if got := store.state.Applied[agent.Label]; !reflect.DeepEqual(got, agent) {
		t.Fatalf("applied agent = %#v, want %#v", got, agent)
	}
	wantLast := []string{
		"set-applied:" + agent.Label,
		fmt.Sprintf("recover-receipts:%s", proc.RecoveryServiceID),
		fmt.Sprintf("recover-receipts:%s", proc.RecoveryStopControlID),
	}
	if len(events) < len(wantLast) || !reflect.DeepEqual(events[len(events)-len(wantLast):], wantLast) {
		t.Fatalf("events = %v, want suffix %v", events, wantLast)
	}
}

func TestControllerRecoveryDoesNotAcknowledgeBeforeConvergence(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.fail")
	runtime := &controllerRuntimeStub{run: launchctlStub(func([]string) (string, error) {
		return "denied", errors.New("denied")
	})}
	store := &controllerStoreStub{state: controllerState{
		Desired: map[string]Agent{agent.Label: agent}, Applied: map[string]Agent{},
	}}
	receipts := &controllerReceiptsStub{}
	if _, err := newControllerWithRuntime(
		context.Background(), controllerConfig(t), runtime, receipts, store,
	); err == nil {
		t.Fatal("newControllerWithRuntime() succeeded")
	}
	if receipts.calls != 0 {
		t.Fatalf("receipt recovery calls = %d, want 0", receipts.calls)
	}
	if runtime.closeCalls != 1 || store.closeCalls != 1 {
		t.Fatalf("constructor cleanup = close %d store %d", runtime.closeCalls, store.closeCalls)
	}
}

func TestControllerRecoveryVerifiesExactAgentWithoutRelaunch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	agent := controllerAgent(t, "com.example.recover-exact")
	plist, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatal(err)
	}
	var events []string
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] != "print" && args[0] != "enable" {
			return "", fmt.Errorf("unexpected launchctl mutation: %v", args)
		}
		return "loaded", nil
	})
	controller, _, store, receipts := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent},
		Applied: map[string]Agent{agent.Label: agent},
	}, run, &events)
	_ = controller
	want := []string{
		"start", "load", "run:print " + serviceTarget(agent.Label),
		"run:enable " + serviceTarget(agent.Label),
		fmt.Sprintf("recover-receipts:%s", proc.RecoveryServiceID),
		fmt.Sprintf("recover-receipts:%s", proc.RecoveryStopControlID),
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("recovery events = %v, want %v", events, want)
	}
	if receipts.calls != 2 {
		t.Fatalf("receipt recovery calls = %d, want 2", receipts.calls)
	}
	if got := store.state.Applied[agent.Label]; !reflect.DeepEqual(got, agent) {
		t.Fatalf("applied agent changed: %#v", got)
	}
}

func TestControllerRejectsUnsafeProgramTreeBeforeEffects(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	realDir := filepath.Join(base, "real")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(realDir, "executable")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	nonExecutable := filepath.Join(realDir, "non-executable")
	if err := os.WriteFile(nonExecutable, []byte("no"), 0o600); err != nil {
		t.Fatal(err)
	}
	programLink := filepath.Join(realDir, "program-link")
	if err := os.Symlink(executable, programLink); err != nil {
		t.Fatal(err)
	}
	ancestorLink := filepath.Join(base, "ancestor-link")
	if err := os.Symlink(realDir, ancestorLink); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		program string
	}{
		{"symlink program", programLink},
		{"symlink ancestor", filepath.Join(ancestorLink, "executable")},
		{"directory program", realDir},
		{"non-executable program", nonExecutable},
		{"missing program", filepath.Join(realDir, "missing")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := controllerAgent(t, "com.example.unsafe")
			agent.Program = test.program
			var events []string
			controller, _, store, _ := newTestController(t, controllerState{
				Desired: map[string]Agent{}, Applied: map[string]Agent{},
			}, launchctlStub(func(args []string) (string, error) {
				return "", fmt.Errorf("unexpected launchctl effect: %v", args)
			}), &events)
			events = nil
			if err := controller.Converge(context.Background(), []Agent{agent}); err == nil {
				t.Fatal("Converge() accepted unsafe program")
			}
			if len(events) != 0 || len(store.state.Desired) != 0 {
				t.Fatalf("unsafe program reached durable state: events=%v state=%#v", events, store.state)
			}
			path, err := agent.PlistPath()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe program plist exists or stat failed unexpectedly: %v", err)
			}
		})
	}
}

func TestControllerReconcileAndVerifyRejectNonMissingProgramErrors(t *testing.T) {
	t.Run("symlink program", func(t *testing.T) {
		target := controllerExecutable(t, "target")
		program := filepath.Join(filepath.Dir(target), "program-link")
		if err := os.Symlink(target, program); err != nil {
			t.Fatal(err)
		}
		agent := controllerAgent(t, "com.example.symlink-program")
		agent.Program = program
		assertReconcileAndVerifyProgramError(t, agent, nil)
	})

	t.Run("inaccessible ancestor", func(t *testing.T) {
		base, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		directory := filepath.Join(base, "blocked")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		program := filepath.Join(directory, "program")
		if err := os.WriteFile(program, []byte("#!/bin/sh\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Errorf("restore directory permissions: %v", err)
			}
		})
		if err := os.Chmod(directory, 0o000); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(program); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("Lstat() error = %v, want EACCES", err)
		}
		agent := controllerAgent(t, "com.example.inaccessible-program")
		agent.Program = program
		assertReconcileAndVerifyProgramError(t, agent, syscall.EACCES)
	})
}

func assertReconcileAndVerifyProgramError(t *testing.T, agent Agent, target error) {
	t.Helper()
	controller := &Controller{}
	err := controller.reconcile(t.Context(), map[string]Agent{}, map[string]Agent{agent.Label: agent})
	if err == nil || target != nil && !errors.Is(err, target) {
		t.Fatalf("reconcile() error = %v, want non-missing program error %v", err, target)
	}
	verified, err := controller.verify(t.Context(), agent)
	if err == nil || target != nil && !errors.Is(err, target) {
		t.Fatalf("verify() = (%t, %v), want non-missing program error %v", verified, err, target)
	}
}

func TestControllerRejectsEmptyProgramBeforePersistence(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.empty-program")
	agent.Program = ""
	var events []string
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, launchctlStub(func(args []string) (string, error) {
		return "", fmt.Errorf("unexpected launchctl effect: %v", args)
	}), &events)
	events = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err == nil ||
		!strings.Contains(err.Error(), "program path") {
		t.Fatalf("Converge error = %v, want program path rejection", err)
	}
	if len(events) != 0 || len(store.state.Desired) != 0 {
		t.Fatalf("empty program reached durable state: events=%v state=%#v", events, store.state)
	}
}

func TestControllerRecoveryToleratesStaleAppliedProgramAndReconverges(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.stale-recovery")
	stale.Program = controllerExecutable(t, "stale")
	if err := os.Remove(stale.Program); err != nil {
		t.Fatal(err)
	}
	var events []string
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, store, receipts := newTestController(t, controllerState{
		Desired: map[string]Agent{stale.Label: stale},
		Applied: map[string]Agent{stale.Label: stale},
	}, run, &events)
	if len(calls) != 0 {
		t.Fatalf("stale recovery launchctl calls = %v, want none", calls)
	}
	if receipts.calls != 2 {
		t.Fatalf("receipt recovery calls = %d, want 2", receipts.calls)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "run:") {
			t.Fatalf("stale recovery invoked launchctl: %v", events)
		}
	}
	if got := store.state.Applied[stale.Label]; !reflect.DeepEqual(got, stale) {
		t.Fatalf("stale applied agent = %#v, want %#v", got, stale)
	}

	fresh := stale
	fresh.Program = controllerExecutable(t, "fresh")
	calls = nil
	if err := controller.Converge(t.Context(), []Agent{fresh}); err != nil {
		t.Fatal(err)
	}
	path, err := fresh.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"bootout", serviceTarget(fresh.Label)},
		{"enable", serviceTarget(fresh.Label)},
		{"bootstrap", domainTarget(), path},
		{"kickstart", serviceTarget(fresh.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("reconverge launchctl calls = %v, want %v", calls, want)
	}
	if got := store.state.Desired[fresh.Label]; !reflect.DeepEqual(got, fresh) {
		t.Fatalf("reconverged desired agent = %#v, want %#v", got, fresh)
	}
	if got := store.state.Applied[fresh.Label]; !reflect.DeepEqual(got, fresh) {
		t.Fatalf("reconverged applied agent = %#v, want %#v", got, fresh)
	}
}

func TestControllerRecoveryInstallsFreshDesiredOverStaleApplied(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.recovery-upgrade")
	stale.Program = controllerExecutable(t, "stale")
	if err := os.Remove(stale.Program); err != nil {
		t.Fatal(err)
	}
	fresh := stale
	fresh.Program = controllerExecutable(t, "fresh")
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	_, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{fresh.Label: fresh},
		Applied: map[string]Agent{stale.Label: stale},
	}, run, nil)
	path, err := fresh.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"bootout", serviceTarget(fresh.Label)},
		{"enable", serviceTarget(fresh.Label)},
		{"bootstrap", domainTarget(), path},
		{"kickstart", serviceTarget(fresh.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("recovery launchctl calls = %v, want %v", calls, want)
	}
	if got := store.state.Applied[fresh.Label]; !reflect.DeepEqual(got, fresh) {
		t.Fatalf("recovered applied agent = %#v, want %#v", got, fresh)
	}
}

func TestControllerRecoverySkipsStaleLabelAndVerifiesHealthyLabel(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.a-stale")
	stale.Program = controllerExecutable(t, "stale")
	if err := os.Remove(stale.Program); err != nil {
		t.Fatal(err)
	}
	healthy := controllerAgent(t, "com.example.b-healthy")
	plist, err := healthy.Plist()
	if err != nil {
		t.Fatal(err)
	}
	path, err := healthy.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		if slices.Contains(args, serviceTarget(stale.Label)) {
			return "", fmt.Errorf("stale label reached launchctl: %v", args)
		}
		return "loaded", nil
	})
	_, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{stale.Label: stale, healthy.Label: healthy},
		Applied: map[string]Agent{stale.Label: stale, healthy.Label: healthy},
	}, run, nil)
	want := [][]string{
		{"print", serviceTarget(healthy.Label)},
		{"enable", serviceTarget(healthy.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("recovery launchctl calls = %v, want %v", calls, want)
	}
	if !reflect.DeepEqual(store.state.Applied, map[string]Agent{
		stale.Label: stale, healthy.Label: healthy,
	}) {
		t.Fatalf("recovery changed applied state: %#v", store.state.Applied)
	}
}

func TestControllerConvergeRemovesStalePersistedAgent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.remove-stale")
	stale.Program = controllerExecutable(t, "stale")
	if err := os.Remove(stale.Program); err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		return "not loaded", launchctlExit(launchctlNotLoadedExit)
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{stale.Label: stale},
		Applied: map[string]Agent{stale.Label: stale},
	}, run, nil)
	calls = nil
	if err := controller.Converge(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"bootout", serviceTarget(stale.Label)}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("uninstall launchctl calls = %v, want %v", calls, want)
	}
	if len(store.state.Desired) != 0 || len(store.state.Applied) != 0 {
		t.Fatalf("stale agent remains durable: %#v", store.state)
	}
}

func TestControllerStatusReportsStaleLoadedAgentAsDrift(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.status-stale")
	stale.Program = controllerExecutable(t, "stale")
	if err := os.Remove(stale.Program); err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		return "loaded", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{stale.Label: stale},
		Applied: map[string]Agent{stale.Label: stale},
	}, run, nil)
	calls = nil
	status, err := controller.Status(t.Context(), stale.Label)
	if err != nil {
		t.Fatal(err)
	}
	want := Status{Label: stale.Label, Desired: true, Applied: true, Loaded: true}
	if status != want {
		t.Fatalf("Status() = %#v, want %#v", status, want)
	}
	wantCalls := [][]string{{"print", serviceTarget(stale.Label)}}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("Status launchctl calls = %v, want %v", calls, wantCalls)
	}
}

func TestControllerPersistsDesiredBeforeEffectsAndResumesAfterFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.persist")
	var events []string
	fail := true
	run := launchctlStub(func(args []string) (string, error) {
		if fail {
			return "denied", errors.New("denied")
		}
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, &events)
	events = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err == nil {
		t.Fatal("Converge() succeeded despite launchctl failure")
	}
	if len(events) < 2 || events[0] != "replace-desired" || !strings.HasPrefix(events[1], "run:") {
		t.Fatalf("events = %v, desired must commit before first effect", events)
	}
	if got := store.state.Desired[agent.Label]; !reflect.DeepEqual(got, agent) {
		t.Fatalf("durable desired = %#v, want %#v", got, agent)
	}
	if _, ok := store.state.Applied[agent.Label]; ok {
		t.Fatal("failed effect was marked applied")
	}
	fail = false
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("resumed Converge() = %v", err)
	}
	if got := store.state.Applied[agent.Label]; !reflect.DeepEqual(got, agent) {
		t.Fatalf("resumed applied = %#v, want %#v", got, agent)
	}
}

func TestControllerInstallUsesExactLaunchctlOrder(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.install-order")
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("Converge() = %v", err)
	}
	want := [][]string{
		{"bootout", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
		{"bootstrap", domainTarget(), path},
		{"kickstart", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("launchctl calls = %v, want %v", calls, want)
	}
}

func TestControllerInstallEnableFailureStopsAndRetryRestartsAtBootout(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.enable-failure")
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	errEnable := launchctlExit(launchctlInFluxExit)
	failEnable := true
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "bootout":
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		case "enable":
			if failEnable {
				return "in flux", errEnable
			}
		}
		return "", nil
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)
	if err := controller.Converge(context.Background(), []Agent{agent}); !errors.Is(err, errEnable) {
		t.Fatalf("Converge() = %v, want enable failure", err)
	}
	wantFailure := [][]string{
		{"bootout", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, wantFailure) {
		t.Fatalf("failed launchctl calls = %v, want %v", calls, wantFailure)
	}
	if _, ok := store.state.Applied[agent.Label]; ok {
		t.Fatal("enable failure was marked applied")
	}

	failEnable = false
	calls = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("retry Converge() = %v", err)
	}
	wantRetry := [][]string{
		{"bootout", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
		{"bootstrap", domainTarget(), path},
		{"kickstart", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, wantRetry) {
		t.Fatalf("retry launchctl calls = %v, want %v", calls, wantRetry)
	}
}

func TestControllerInstallRetryRepeatsSequenceBeforeKickstart(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.bootstrap-retry")
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	bootstrapCalls := 0
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "bootout":
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		case "bootstrap":
			bootstrapCalls++
			if bootstrapCalls == 1 {
				return "in flux", launchctlExit(launchctlInFluxExit)
			}
		}
		return "", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)
	controller.retryWait = func(context.Context, time.Duration) error { return nil }
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("Converge() = %v", err)
	}
	want := [][]string{
		{"bootout", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
		{"bootstrap", domainTarget(), path},
		{"bootout", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
		{"bootstrap", domainTarget(), path},
		{"kickstart", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("launchctl calls = %v, want %v", calls, want)
	}
}

func TestControllerExactSetRemovesStaleBeforeInstallingDesired(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stale := controllerAgent(t, "com.example.stale")
	desired := controllerAgent(t, "com.example.desired")
	var events []string
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, &events)
	store.state.Applied[stale.Label] = stale
	controller.state.Applied[stale.Label] = stale
	events = nil
	if err := controller.Converge(context.Background(), []Agent{desired}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(events, "\n")
	remove := strings.Index(joined, "run:bootout "+serviceTarget(stale.Label))
	install := strings.Index(joined, "run:bootout "+serviceTarget(desired.Label))
	if remove < 0 || install < 0 || remove >= install {
		t.Fatalf("events = %v, stale removal must precede install", events)
	}
	if _, ok := store.state.Applied[stale.Label]; ok {
		t.Fatal("stale agent remains applied")
	}
	if !reflect.DeepEqual(store.state.Applied[desired.Label], desired) {
		t.Fatal("desired agent is not applied")
	}
	if err := controller.Converge(context.Background(), nil); err != nil {
		t.Fatalf("Converge(nil) = %v", err)
	}
	if len(store.state.Desired) != 0 || len(store.state.Applied) != 0 {
		t.Fatalf("nil did not converge exact empty set: %#v", store.state)
	}
}

func TestControllerSameSetVerifiesAndRepairsDrift(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.verify")
	var events []string
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		return "", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent}, Applied: map[string]Agent{agent.Label: agent},
	}, run, &events)
	events = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatal(err)
	}
	if want := []string{
		"run:print " + serviceTarget(agent.Label),
		"run:enable " + serviceTarget(agent.Label),
	}; !reflect.DeepEqual(events, want) {
		t.Fatalf("exact-state events = %v, want %v", events, want)
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	events = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[0] != "run:bootout "+serviceTarget(agent.Label) {
		t.Fatalf("missing-plist events = %v, want reinstall", events)
	}
}

func TestControllerVerifyEnablesExactLoadedAgentForRelaunch(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.verify-disabled")
	disabled := false
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "bootout":
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		case "enable":
			disabled = false
		}
		return "", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent}, Applied: map[string]Agent{agent.Label: agent},
	}, run, nil)
	disabled = true
	calls = nil

	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("Converge() = %v", err)
	}
	want := [][]string{
		{"print", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("launchctl calls = %v, want %v", calls, want)
	}
	if disabled {
		t.Fatal("exact loaded agent remains disabled after convergence")
	}
}

func TestControllerVerifyEnableFailureIsNotConverged(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.verify-enable-failure")
	errEnable := launchctlExit(77)
	failEnable := false
	var calls [][]string
	run := launchctlStub(func(args []string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "bootout":
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		case "enable":
			if failEnable {
				return "denied", errEnable
			}
		}
		return "", nil
	})
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent}, Applied: map[string]Agent{agent.Label: agent},
	}, run, nil)
	failEnable = true
	calls = nil

	err := controller.Converge(context.Background(), []Agent{agent})
	if !errors.Is(err, errEnable) || !strings.Contains(err.Error(), "verify agent") {
		t.Fatalf("Converge() = %v, want enable verification failure", err)
	}
	want := [][]string{
		{"print", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("launchctl calls = %v, want %v", calls, want)
	}
	if got := store.state.Applied[agent.Label]; !reflect.DeepEqual(got, agent) {
		t.Fatalf("applied agent changed after verification failure: %#v", got)
	}

	failEnable = false
	calls = nil
	if err := controller.Converge(context.Background(), []Agent{agent}); err != nil {
		t.Fatalf("retry Converge() = %v", err)
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("retry launchctl calls = %v, want %v", calls, want)
	}
}

func TestControllerVerifyPropagatesUnexpectedLaunchctlFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.verify-error")
	var failPrint bool
	run := launchctlStub(func(args []string) (string, error) {
		if args[0] == "bootout" {
			return "not loaded", launchctlExit(launchctlNotLoadedExit)
		}
		if args[0] == "print" && failPrint {
			return "denied", launchctlExit(77)
		}
		return "", nil
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent}, Applied: map[string]Agent{agent.Label: agent},
	}, run, nil)
	failPrint = true
	err := controller.Converge(context.Background(), []Agent{agent})
	if err == nil || !strings.Contains(err.Error(), "verify agent") {
		t.Fatalf("Converge() error = %v, want verification failure", err)
	}
}

func TestControllerRetriesWholeLoadSequenceOnLaunchdEIO(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.retry")
	agent.LimitLoadToSessionType = SessionTypeBackground
	tests := []struct {
		name    string
		failure string
	}{
		{name: "bootout", failure: "bootout"},
		{name: "bootstrap", failure: "bootstrap"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls [][]string
			run := launchctlStub(func(args []string) (string, error) {
				calls = append(calls, append([]string(nil), args...))
				switch args[0] {
				case "bootout":
					if test.failure == "bootout" {
						return "in flux", launchctlExit(launchctlInFluxExit)
					}
					return "not loaded", launchctlExit(launchctlNotLoadedExit)
				case "bootstrap":
					return "in flux", launchctlExit(launchctlInFluxExit)
				case "managername":
					return "Aqua\n", nil
				default:
					return "", nil
				}
			})
			controller, _, _, _ := newTestController(t, controllerState{
				Desired: map[string]Agent{}, Applied: map[string]Agent{},
			}, run, nil)
			var delays []time.Duration
			controller.retryWait = func(_ context.Context, delay time.Duration) error {
				delays = append(delays, delay)
				return nil
			}
			err := controller.reload(context.Background(), agent, "/tmp/retry.plist")
			if err == nil || !strings.Contains(err.Error(), "after 6 attempts") ||
				!strings.Contains(err.Error(), `desired session "Background"`) ||
				!strings.Contains(err.Error(), `current manager "Aqua"`) {
				t.Fatalf("reload() error = %v", err)
			}
			wantDelays := []time.Duration{
				200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
				1600 * time.Millisecond, 3200 * time.Millisecond,
			}
			if !reflect.DeepEqual(delays, wantDelays) {
				t.Fatalf("retry delays = %v, want %v", delays, wantDelays)
			}
			var wantCalls [][]string
			for range bootstrapAttempts {
				wantCalls = append(wantCalls, []string{"bootout", serviceTarget(agent.Label)})
				if test.failure == "bootstrap" {
					wantCalls = append(wantCalls,
						[]string{"enable", serviceTarget(agent.Label)},
						[]string{"bootstrap", domainTarget(), "/tmp/retry.plist"},
					)
				}
			}
			wantCalls = append(wantCalls, []string{"managername"})
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf("launchctl calls = %v, want %v", calls, wantCalls)
			}
		})
	}
}

func TestControllerDoesNotRetryNonEIO(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.no-retry")
	var calls int
	run := launchctlStub(func([]string) (string, error) {
		calls++
		return "denied", launchctlExit(77)
	})
	controller, _, _, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)
	controller.retryWait = func(context.Context, time.Duration) error {
		t.Fatal("non-EIO failure waited for retry")
		return nil
	}
	if err := controller.reload(context.Background(), agent, "/tmp/no-retry.plist"); err == nil {
		t.Fatal("reload() succeeded")
	}
	if calls != 1 {
		t.Fatalf("launchctl calls = %d, want 1", calls)
	}
}

func TestControllerCloseCancelsAdmittedOperationAtBoundAndRejectsNewWork(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.close")
	started := make(chan struct{})
	var once sync.Once
	run := func(ctx context.Context, _ worker.CommandRequest) (worker.CommandResult, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return worker.CommandResult{}, ctx.Err()
	}
	controller, runtime, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, run, nil)
	converged := make(chan error, 1)
	go func() { converged <- controller.Converge(context.Background(), []Agent{agent}) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() = %v, want deadline", err)
	}
	if err := <-converged; !errors.Is(err, context.Canceled) {
		t.Fatalf("Converge() = %v, want cancellation", err)
	}
	if err := controller.Converge(context.Background(), nil); !errors.Is(err, ErrControllerClosed) {
		t.Fatalf("post-close Converge() = %v", err)
	}
	if runtime.closeCalls != 1 || store.closeCalls != 1 {
		t.Fatalf("close calls = runtime %d store %d", runtime.closeCalls, store.closeCalls)
	}
}

func TestControllerCloseUsesFreshContextAfterCallerCancellation(t *testing.T) {
	controller, runtime, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{},
	}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() = %v, want caller cancellation", err)
	}
	if runtime.closeCalls != 1 || store.closeCalls != 1 {
		t.Fatalf("fresh close did not settle ownership: runtime %d store %d", runtime.closeCalls, store.closeCalls)
	}
}
