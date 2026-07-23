package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

type controllerRuntimeStub struct {
	mu sync.Mutex

	events      *[]string
	recoverErr  error
	run         func(context.Context, supervise.Task) error
	wait        func(context.Context) error
	closeCalls  int
	cancelCalls int
}

func (r *controllerRuntimeStub) Recover(context.Context) error {
	r.record("recover")
	return r.recoverErr
}

func (r *controllerRuntimeStub) Run(ctx context.Context, task supervise.Task) error {
	if task.RecoveryClass != proc.RecoveryService {
		return fmt.Errorf("task recovery class = %q, want %q", task.RecoveryClass, proc.RecoveryService)
	}
	r.record("run:" + strings.Join(task.Args, " "))
	if r.run != nil {
		return r.run(ctx, task)
	}
	return nil
}

func (r *controllerRuntimeStub) Close() {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()
	r.record("close-runtime")
}

func (r *controllerRuntimeStub) Cancel() {
	r.mu.Lock()
	r.cancelCalls++
	r.mu.Unlock()
	r.record("cancel-runtime")
}

func (r *controllerRuntimeStub) Wait(ctx context.Context) error {
	r.record("wait-runtime")
	if r.wait != nil {
		return r.wait(ctx)
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
	events  *[]string
	calls   int
	classes []proc.RecoveryClass
	err     error
}

func (r *controllerReceiptsStub) RecoverReapReceipts(
	_ context.Context,
	class proc.RecoveryClass,
	_ func(context.Context, proc.ReapReceipt) error,
) (proc.ReapReceiptFloor, error) {
	if class != proc.RecoveryService && class != proc.RecoveryStopControl {
		return proc.ReapReceiptFloor{}, fmt.Errorf("receipt class = %q", class)
	}
	r.calls++
	r.classes = append(r.classes, class)
	if r.events != nil {
		*r.events = append(*r.events, fmt.Sprintf("recover-receipts:%d", class))
	}
	return proc.ReapReceiptFloor{RecoveryClass: class}, r.err
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

func launchctlExit(code int) error { return &supervise.ExitError{Code: code} }

func launchctlStub(fn func([]string) (string, error)) func(context.Context, supervise.Task) error {
	return func(_ context.Context, task supervise.Task) error {
		out, err := fn(task.Args)
		if task.Stdout != nil {
			_, _ = task.Stdout.Write([]byte(out))
		}
		return err
	}
}

func newTestController(
	t *testing.T,
	state controllerState,
	run func(context.Context, supervise.Task) error,
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
		fmt.Sprintf("recover-receipts:%d", proc.RecoveryService),
		fmt.Sprintf("recover-receipts:%d", proc.RecoveryStopControl),
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
	if runtime.closeCalls != 1 || runtime.cancelCalls != 1 || store.closeCalls != 1 {
		t.Fatalf("constructor cleanup = close %d cancel %d store %d", runtime.closeCalls, runtime.cancelCalls, store.closeCalls)
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
		if args[0] != "print" {
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
		"recover", "load", "run:print " + serviceTarget(agent.Label),
		fmt.Sprintf("recover-receipts:%d", proc.RecoveryService),
		fmt.Sprintf("recover-receipts:%d", proc.RecoveryStopControl),
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

func TestControllerRecoveryRejectsUnsafeAppliedProgramBeforeReceiptAck(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(base, "holder")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(base, "holder-link")
	if err := os.Symlink(executable, linked); err != nil {
		t.Fatal(err)
	}
	agent := controllerAgent(t, "com.example.unsafe-recovery")
	agent.Program = linked
	var events []string
	runtime := &controllerRuntimeStub{events: &events, run: launchctlStub(func(args []string) (string, error) {
		return "", fmt.Errorf("unexpected launchctl effect: %v", args)
	})}
	store := &controllerStoreStub{events: &events, state: controllerState{
		Desired: map[string]Agent{agent.Label: agent},
		Applied: map[string]Agent{agent.Label: agent},
	}}
	receipts := &controllerReceiptsStub{events: &events}
	if _, err := newControllerWithRuntime(context.Background(), controllerConfig(t), runtime, receipts, store); err == nil {
		t.Fatal("newControllerWithRuntime() accepted unsafe recovered program")
	}
	if receipts.calls != 0 {
		t.Fatalf("receipt recovery calls = %d, want 0", receipts.calls)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "run:") {
			t.Fatalf("unsafe recovery invoked launchctl: %v", events)
		}
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
	if want := []string{"run:print " + serviceTarget(agent.Label)}; !reflect.DeepEqual(events, want) {
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

func TestControllerRetriesWholeReloadPairOnLaunchdEIO(t *testing.T) {
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
			wantBootout, wantBootstrap := bootstrapAttempts, 0
			if test.failure == "bootstrap" {
				wantBootstrap = bootstrapAttempts
			}
			var bootouts, bootstraps, managers int
			for _, call := range calls {
				switch call[0] {
				case "bootout":
					bootouts++
				case "bootstrap":
					bootstraps++
				case "managername":
					managers++
				}
			}
			if bootouts != wantBootout || bootstraps != wantBootstrap || managers != 1 {
				t.Fatalf("calls = %v; bootout/bootstrap/manager = %d/%d/%d, want %d/%d/1", calls, bootouts, bootstraps, managers, wantBootout, wantBootstrap)
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
	run := func(ctx context.Context, _ supervise.Task) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
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
	if runtime.closeCalls != 1 || runtime.cancelCalls != 1 || store.closeCalls != 1 {
		t.Fatalf("close calls = runtime %d cancel %d store %d", runtime.closeCalls, runtime.cancelCalls, store.closeCalls)
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
