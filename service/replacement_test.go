package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

type replacementLaunchd struct {
	loaded map[string]bool
	events []string
}

func (l *replacementLaunchd) run(_ context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	l.events = append(l.events, strings.Join(task.Args, " "))
	if len(task.Args) == 0 {
		return worker.CommandResult{}, errors.New("empty launchctl arguments")
	}
	var err error
	switch task.Args[0] {
	case "print":
		if l.loaded[task.Args[1]] {
			break
		}
		err = launchctlExit(launchctlNotLoadedExit)
	case "bootout":
		if len(task.Args) > 1 {
			l.loaded[task.Args[1]] = false
		}
	case "bootstrap":
	case "enable":
	case "kickstart":
		if len(task.Args) > 1 {
			l.loaded[task.Args[1]] = true
		}
	}
	return worker.CommandResult{}, err
}

func newReplacementController(
	t *testing.T,
	agent Agent,
) (*Controller, *controllerStoreStub, *replacementLaunchd) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	launchd := &replacementLaunchd{loaded: map[string]bool{serviceTarget(agent.Label): true}}
	controller, _, store, _ := newTestController(t, controllerState{
		Desired: map[string]Agent{agent.Label: agent},
		Applied: map[string]Agent{agent.Label: agent},
	}, launchd.run, nil)
	return controller, store, launchd
}

func openDurableReplacementController(
	t *testing.T,
	config ControllerConfig,
	launchd *replacementLaunchd,
) *Controller {
	t.Helper()
	store, err := openControllerStore(t.Context(), config.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &controllerRuntimeStub{run: launchd.run}
	receipts := &controllerReceiptsStub{}
	controller, err := newControllerWithRuntime(t.Context(), config, runtime, receipts, store)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func closeReplacementController(t *testing.T, controller *Controller) {
	t.Helper()
	if err := controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func replacementPlan(t *testing.T, agents ...Agent) Plan {
	t.Helper()
	plan, err := NewPlan(agents)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func replacementBinding(value string) ReplacementBinding {
	return sha256.Sum256([]byte("daemonkit replacement test: " + value))
}

func proveReplacement(t *testing.T, controller *Controller, receipt QuiesceReceipt) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)
	controller.replacementNow = func() time.Time { return now }
	controller.replacementWait = func(ctx context.Context, duration time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(duration)
		return nil
	}
	controller.replacementProcesses = func(string) ([]proc.Identity, error) { return nil, nil }
	paths, err := receipt.Plan.programPaths()
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.ProveQuiesced(t.Context(), receipt, paths); err != nil {
		t.Fatal(err)
	}
}

func TestPlanIsCanonicalImmutableAndDigestSensitive(t *testing.T) {
	first := controllerAgent(t, "com.example.first")
	second := controllerAgent(t, "com.example.second")
	first.Args = []string{"first"}
	first.Env = map[string]string{"A": "1"}
	plan := replacementPlan(t, second, first)
	reordered := replacementPlan(t, first, second)
	if plan.Digest() != reordered.Digest() {
		t.Fatalf("reordered digest = %s, want %s", reordered.Digest(), plan.Digest())
	}
	agents := plan.Agents()
	if !slices.Equal([]string{agents[0].Label, agents[1].Label}, []string{first.Label, second.Label}) {
		t.Fatalf("agents are not canonical: %#v", agents)
	}
	agents[0].Args[0] = "mutated"
	agents[0].Env["A"] = "mutated"
	if plan.Agents()[0].Args[0] != "first" || plan.Agents()[0].Env["A"] != "1" {
		t.Fatal("Plan.Agents exposed mutable plan state")
	}
	first.Args = []string{"different"}
	if changed := replacementPlan(t, first, second); changed.Digest() == plan.Digest() {
		t.Fatal("plan digest ignored an agent change")
	}
}

func TestPlanRejectsNonExactProgramPaths(t *testing.T) {
	target := filepath.Join(t.TempDir(), "executable")
	if err := os.WriteFile(target, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "executable-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		program string
	}{
		{name: "empty", program: ""},
		{name: "relative", program: "usr/bin/true"},
		{name: "symlink", program: link},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := controllerAgent(t, "com.example.invalid-plan")
			agent.Program = test.program
			if _, err := NewPlan([]Agent{agent}); err == nil {
				t.Fatal("NewPlan accepted unsafe program")
			}
		})
	}
}

func TestReplacementFenceOwnsQuiesceApplyAndCommit(t *testing.T) {
	priorAgent := controllerAgent(t, "com.example.prior")
	controller, store, launchd := newReplacementController(t, priorAgent)
	prior := replacementPlan(t, priorAgent)
	binding := replacementBinding("replace-1")
	receipt, err := controller.Quiesce(t.Context(), "replace-1", binding, prior)
	if err != nil {
		t.Fatal(err)
	}
	if launchd.loaded[serviceTarget(priorAgent.Label)] || len(store.state.Desired) != 0 {
		t.Fatalf("quiesce retained launch ownership: loaded=%v state=%#v", launchd.loaded, store.state)
	}
	if err := controller.Converge(t.Context(), nil); !errors.Is(err, ErrQuiesced) {
		t.Fatalf("Converge during fence = %v, want ErrQuiesced", err)
	}
	proveReplacement(t, controller, receipt)
	nextAgent := controllerAgent(t, "com.example.next")
	next := replacementPlan(t, nextAgent)
	if err := controller.ApplyReplacement(t.Context(), "replace-1", replacementBinding("other-build"), next); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("ApplyReplacement wrong binding = %v", err)
	}
	if err := controller.ApplyReplacement(t.Context(), "replace-1", binding, next); err != nil {
		t.Fatal(err)
	}
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || status.Phase != ReplacementRunningOwned || !plansEqual(status.Current, next) || len(status.Proofs) != 1 {
		t.Fatalf("replacement status = %#v", status)
	}
	if _, err := controller.CommitReplacement(t.Context(), "replace-1", binding, prior); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("CommitReplacement stale plan = %v", err)
	}
	if store.state.Replacement == nil {
		t.Fatal("mismatched commit cleared fence")
	}
	commit, err := controller.CommitReplacement(t.Context(), "replace-1", binding, next)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := controller.ReplacementStatus(t.Context()); err != nil || status != nil {
		t.Fatalf("committed status = %#v, %v", status, err)
	}
	if err := controller.Converge(t.Context(), next.Agents()); !errors.Is(err, ErrReplacementCommitPending) {
		t.Fatalf("Converge before acknowledgement = %v", err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatal(err)
	}
	if err := controller.Converge(t.Context(), next.Agents()); err != nil {
		t.Fatalf("Converge after acknowledgement = %v", err)
	}
}

func TestReplacementRollbackRequiresFreshRequiesceProof(t *testing.T) {
	priorAgent := controllerAgent(t, "com.example.rollback-prior")
	controller, _, _ := newReplacementController(t, priorAgent)
	prior := replacementPlan(t, priorAgent)
	binding := replacementBinding("replace-rollback")
	receipt, err := controller.Quiesce(t.Context(), "replace-rollback", binding, prior)
	if err != nil {
		t.Fatal(err)
	}
	proveReplacement(t, controller, receipt)
	next := replacementPlan(t, controllerAgent(t, "com.example.rollback-next"))
	if err := controller.ApplyReplacement(t.Context(), "replace-rollback", binding, next); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreReplacement(t.Context(), "replace-rollback", binding); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("restore without requiesce = %v", err)
	}
	second, err := controller.Requiesce(t.Context(), "replace-rollback", binding)
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch != receipt.Epoch+1 || !plansEqual(second.Plan, next) {
		t.Fatalf("requiesce receipt = %#v", second)
	}
	if err := controller.ProveQuiesced(t.Context(), receipt, mustProgramPaths(t, receipt.Plan)); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("stale proof receipt = %v", err)
	}
	proveReplacement(t, controller, second)
	if err := controller.RestoreReplacement(t.Context(), "replace-rollback", binding); err != nil {
		t.Fatal(err)
	}
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil || status == nil || status.Phase != ReplacementRunningOwned ||
		!plansEqual(status.Current, prior) || len(status.Proofs) != 2 {
		t.Fatalf("restored status = %#v, %v", status, err)
	}
	if _, err := controller.CommitReplacement(t.Context(), "replace-rollback", binding, prior); err != nil {
		t.Fatal(err)
	}
}

func mustProgramPaths(t *testing.T, plan Plan) []string {
	t.Helper()
	paths, err := plan.programPaths()
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestProveQuiescedRejectsRestartAndResetsQuietWindow(t *testing.T) {
	agent := controllerAgent(t, "com.example.quiet")
	controller, _, launchd := newReplacementController(t, agent)
	plan := replacementPlan(t, agent)
	binding := replacementBinding("replace-quiet")
	receipt, err := controller.Quiesce(t.Context(), "replace-quiet", binding, plan)
	if err != nil {
		t.Fatal(err)
	}
	launchd.loaded[serviceTarget(agent.Label)] = true
	if err := controller.ProveQuiesced(t.Context(), receipt, mustProgramPaths(t, plan)); !errors.Is(err, ErrNotQuiesced) {
		t.Fatalf("proof accepted restarted agent: %v", err)
	}
	launchd.loaded[serviceTarget(agent.Label)] = false
	now := time.Unix(1_700_000_000, 0)
	waits := 0
	controller.replacementNow = func() time.Time { return now }
	controller.replacementWait = func(context.Context, time.Duration) error {
		waits++
		now = now.Add(replacementPoll)
		return nil
	}
	observations := 3
	controller.replacementProcesses = func(string) ([]proc.Identity, error) {
		if observations > 0 {
			observations--
			return []proc.Identity{{PID: 42}}, nil
		}
		return nil, nil
	}
	if err := controller.ProveQuiesced(t.Context(), receipt, mustProgramPaths(t, plan)); err != nil {
		t.Fatal(err)
	}
	if waits < observations+int(replacementQuiet/replacementPoll) {
		t.Fatalf("quiet proof waits = %d, did not restart after live identity", waits)
	}
}

func TestQuiesceMismatchAndDriftLeavePriorFenceIntact(t *testing.T) {
	agent := controllerAgent(t, "com.example.mismatch")
	controller, store, _ := newReplacementController(t, agent)
	prior := replacementPlan(t, agent)
	wrong := replacementPlan(t, controllerAgent(t, "com.example.wrong"))
	binding := replacementBinding("replace-mismatch")
	if _, err := controller.Quiesce(t.Context(), "replace-mismatch", binding, wrong); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("Quiesce wrong plan = %v", err)
	}
	if store.state.Replacement != nil {
		t.Fatal("wrong prior created a fence")
	}
	receipt, err := controller.Quiesce(t.Context(), "replace-mismatch", binding, prior)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Quiesce(t.Context(), "other-operation", binding, prior); !errors.Is(err, ErrQuiesced) {
		t.Fatalf("second operation = %v", err)
	}
	if store.state.Replacement == nil || store.state.Replacement.OperationID != receipt.OperationID {
		t.Fatal("mismatch damaged original fence")
	}
}

func TestQuiesceRequiresExactLoadedPriorPlan(t *testing.T) {
	agent := controllerAgent(t, "com.example.prior-drift")
	controller, store, launchd := newReplacementController(t, agent)
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	launchd.loaded[serviceTarget(agent.Label)] = false
	if _, err := controller.Quiesce(t.Context(), "replace-prior-drift", replacementBinding("prior-drift"), replacementPlan(t, agent)); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("Quiesce drift = %v, want ErrReplacementMismatch", err)
	}
	if store.state.Replacement != nil || !reflect.DeepEqual(store.state.Desired, store.state.Applied) {
		t.Fatalf("failed precondition changed state: %#v", store.state)
	}
}

func TestReplacementFenceSurvivesStoreReopen(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "services.db")
	store, err := openControllerStore(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	agent := controllerAgent(t, "com.example.reopen")
	plan := replacementPlan(t, agent)
	replacement := &replacementState{
		OperationID: "replace-reopen", Phase: ReplacementUnloaded, Epoch: 1,
		Binding: replacementBinding("replace-reopen"), Prior: plan, Current: plan,
	}
	if _, err := store.SetReplacement(t.Context(), map[string]Agent{}, replacement, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openControllerStore(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.Replacement == nil || state.Replacement.OperationID != replacement.OperationID ||
		state.Replacement.Phase != ReplacementUnloaded || !plansEqual(state.Replacement.Prior, plan) {
		t.Fatalf("reopened fence = %#v", state.Replacement)
	}
}

func TestProveQuiescedIsExactAndIdempotentAfterControllerReopen(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	directory := t.TempDir()
	config := ControllerConfig{
		StatePath:   filepath.Join(directory, "services.db"),
		ProcessPath: filepath.Join(directory, "processes.db"), WorkerLimit: 1,
	}
	launchd := &replacementLaunchd{loaded: make(map[string]bool)}
	agent := controllerAgent(t, "com.example.proof-reopen")
	plan := replacementPlan(t, agent)
	binding := replacementBinding("proof-reopen")
	controller := openDurableReplacementController(t, config, launchd)
	if err := controller.Converge(t.Context(), plan.Agents()); err != nil {
		t.Fatal(err)
	}
	receipt, err := controller.Quiesce(t.Context(), "proof-reopen", binding, plan)
	if err != nil {
		t.Fatal(err)
	}
	proveReplacement(t, controller, receipt)
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	replayed, err := controller.Quiesce(t.Context(), "proof-reopen", binding, plan)
	if err != nil {
		t.Fatal(err)
	}
	observations := 0
	now := time.Unix(1_700_000_100, 0)
	controller.replacementNow = func() time.Time { return now }
	controller.replacementWait = func(ctx context.Context, duration time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(duration)
		return nil
	}
	controller.replacementProcesses = func(string) ([]proc.Identity, error) {
		observations++
		return nil, nil
	}
	if err := controller.ProveQuiesced(t.Context(), replayed, mustProgramPaths(t, replayed.Plan)); err != nil {
		t.Fatal(err)
	}
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if observations < 2 || status == nil || status.Phase != ReplacementQuiesced || len(status.Proofs) != 1 ||
		status.Proofs[0].Epoch != replayed.Epoch || !slices.Equal(status.Proofs[0].ProgramPaths, mustProgramPaths(t, plan)) {
		t.Fatalf("replayed proof observations/status = %d %#v", observations, status)
	}
	closeReplacementController(t, controller)
}

func TestReplacementCommitSurvivesReopenAndBlocksUntilExactAcknowledgement(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	directory := t.TempDir()
	config := ControllerConfig{
		StatePath:   filepath.Join(directory, "services.db"),
		ProcessPath: filepath.Join(directory, "processes.db"), WorkerLimit: 1,
	}
	launchd := &replacementLaunchd{loaded: make(map[string]bool)}
	prior := replacementPlan(t, controllerAgent(t, "com.example.commit-prior"))
	next := replacementPlan(t, controllerAgent(t, "com.example.commit-next"))
	binding := replacementBinding("commit-reopen")
	controller := openDurableReplacementController(t, config, launchd)
	if err := controller.Converge(t.Context(), prior.Agents()); err != nil {
		t.Fatal(err)
	}
	receipt, err := controller.Quiesce(t.Context(), "commit-reopen", binding, prior)
	if err != nil {
		t.Fatal(err)
	}
	proveReplacement(t, controller, receipt)
	if err := controller.ApplyReplacement(t.Context(), "commit-reopen", binding, next); err != nil {
		t.Fatal(err)
	}
	commit, err := controller.CommitReplacement(t.Context(), "commit-reopen", binding, next)
	if err != nil {
		t.Fatal(err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	completion, err := controller.ReplacementCompletion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if completion == nil || completion.OperationID() != commit.OperationID() || completion.Binding() != binding ||
		!plansEqual(completion.Prior(), prior) || !plansEqual(completion.Next(), next) {
		t.Fatalf("reopened completion = %#v", completion)
	}
	if err := controller.Converge(t.Context(), next.Agents()); !errors.Is(err, ErrReplacementCommitPending) {
		t.Fatalf("Converge with pending commit = %v", err)
	}
	if _, err := controller.Quiesce(t.Context(), "other", binding, next); !errors.Is(err, ErrReplacementCommitPending) {
		t.Fatalf("Quiesce with pending commit = %v", err)
	}
	if _, err := controller.CommitReplacement(t.Context(), "commit-reopen", binding, next); err != nil {
		t.Fatalf("repeat exact commit = %v", err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), next, next,
	); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("mismatched acknowledgement = %v", err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatal(err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	completion, err = controller.ReplacementCompletion(t.Context())
	if err != nil || completion != nil {
		t.Fatalf("completion after acknowledgement = %#v, %v", completion, err)
	}
	acknowledgement, err := controller.ReplacementAcknowledgement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if acknowledgement == nil || acknowledgement.OperationID() != commit.OperationID() ||
		acknowledgement.Binding() != commit.Binding() || !plansEqual(acknowledgement.Prior(), commit.Prior()) ||
		!plansEqual(acknowledgement.Next(), commit.Next()) {
		t.Fatalf("reopened acknowledgement = %#v", acknowledgement)
	}
	mutatedPrior := acknowledgement.Prior()
	delete(mutatedPrior.agents, prior.Agents()[0].Label)
	unchanged, err := controller.ReplacementAcknowledgement(t.Context())
	if err != nil || unchanged == nil || !plansEqual(unchanged.Prior(), prior) {
		t.Fatalf("acknowledgement exposed mutable state: %#v, %v", unchanged, err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatalf("replayed exact acknowledgement = %v", err)
	}
	ackMismatches := []struct {
		name      string
		operation string
		binding   ReplacementBinding
		prior     Plan
		next      Plan
	}{
		{name: "operation", operation: "commit-never-happened", binding: commit.Binding(), prior: commit.Prior(), next: commit.Next()},
		{name: "binding", operation: commit.OperationID(), binding: replacementBinding("wrong-ack"), prior: commit.Prior(), next: commit.Next()},
		{name: "prior", operation: commit.OperationID(), binding: commit.Binding(), prior: commit.Next(), next: commit.Next()},
		{name: "next", operation: commit.OperationID(), binding: commit.Binding(), prior: commit.Prior(), next: commit.Prior()},
	}
	for _, test := range ackMismatches {
		t.Run("ack mismatch "+test.name, func(t *testing.T) {
			if err := controller.AcknowledgeReplacementCommit(
				t.Context(), test.operation, test.binding, test.prior, test.next,
			); !errors.Is(err, ErrReplacementMismatch) {
				t.Fatalf("AcknowledgeReplacementCommit = %v, want ErrReplacementMismatch", err)
			}
		})
	}
	later := replacementPlan(t, controllerAgent(t, "com.example.commit-later"))
	if err := controller.Converge(t.Context(), later.Agents()); err != nil {
		t.Fatalf("Converge after acknowledgement = %v", err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatalf("replayed acknowledgement after Converge = %v", err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	defer closeReplacementController(t, controller)
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatalf("replayed acknowledgement after Converge and reopen = %v", err)
	}
	acknowledgement, err = controller.ReplacementAcknowledgement(t.Context())
	if err != nil || acknowledgement == nil || acknowledgement.OperationID() != commit.OperationID() {
		t.Fatalf("acknowledgement after Converge and reopen = %#v, %v", acknowledgement, err)
	}
	if _, err := controller.Quiesce(
		t.Context(), commit.OperationID(), replacementBinding("reused-operation"), later,
	); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("Quiesce with acknowledged operation ID = %v, want ErrReplacementMismatch", err)
	}

	laterBinding := replacementBinding("later-operation")
	laterReceipt, err := controller.Quiesce(t.Context(), "later-operation", laterBinding, later)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatalf("replayed acknowledgement during later fence = %v", err)
	}
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil || status == nil || status.OperationID != laterReceipt.OperationID {
		t.Fatalf("later fence after old acknowledgement replay = %#v, %v", status, err)
	}
	proveReplacement(t, controller, laterReceipt)
	final := replacementPlan(t, controllerAgent(t, "com.example.commit-final"))
	if err := controller.ApplyReplacement(t.Context(), laterReceipt.OperationID, laterBinding, final); err != nil {
		t.Fatal(err)
	}
	laterCommit, err := controller.CommitReplacement(t.Context(), laterReceipt.OperationID, laterBinding, final)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatalf("replayed acknowledgement during later pending commit = %v", err)
	}
	completion, err = controller.ReplacementCompletion(t.Context())
	if err != nil || completion == nil || completion.OperationID() != laterCommit.OperationID() {
		t.Fatalf("later completion after old acknowledgement replay = %#v, %v", completion, err)
	}
}

func TestReplacementHistorySurvivesRemovedPriorProgram(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	directory := t.TempDir()
	realDirectory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	priorProgram := filepath.Join(realDirectory, "prior")
	nextProgram := filepath.Join(realDirectory, "next")
	for _, path := range []string{priorProgram, nextProgram} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	config := ControllerConfig{
		StatePath:   filepath.Join(realDirectory, "services.db"),
		ProcessPath: filepath.Join(realDirectory, "processes.db"), WorkerLimit: 1,
	}
	launchd := &replacementLaunchd{loaded: make(map[string]bool)}
	priorAgent := controllerAgent(t, "com.example.removed-prior")
	priorAgent.Program = priorProgram
	nextAgent := priorAgent
	nextAgent.Program = nextProgram
	prior := replacementPlan(t, priorAgent)
	next := replacementPlan(t, nextAgent)
	binding := replacementBinding("removed-prior")

	controller := openDurableReplacementController(t, config, launchd)
	if err := controller.Converge(t.Context(), prior.Agents()); err != nil {
		t.Fatal(err)
	}
	receipt, err := controller.Quiesce(t.Context(), "removed-prior", binding, prior)
	if err != nil {
		t.Fatal(err)
	}
	proveReplacement(t, controller, receipt)
	if err := os.Remove(priorProgram); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(priorProgram); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prior program stat = %v, want not exist", err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || status.Phase != ReplacementQuiesced ||
		!plansEqual(status.Prior, prior) || !plansEqual(status.Current, prior) {
		t.Fatalf("reopened quiesced status = %#v", status)
	}
	if err := controller.ApplyReplacement(t.Context(), "removed-prior", binding, next); err != nil {
		t.Fatalf("ApplyReplacement after prior program removal: %v", err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	status, err = controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || status.Phase != ReplacementRunningOwned ||
		!plansEqual(status.Prior, prior) || !plansEqual(status.Current, next) {
		t.Fatalf("reopened status = %#v", status)
	}
	commit, err := controller.CommitReplacement(t.Context(), "removed-prior", binding, next)
	if err != nil {
		t.Fatal(err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	completion, err := controller.ReplacementCompletion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if completion == nil || !plansEqual(completion.Prior(), prior) || !plansEqual(completion.Next(), next) {
		t.Fatalf("reopened completion = %#v", completion)
	}
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), commit.OperationID(), commit.Binding(), commit.Prior(), commit.Next(),
	); err != nil {
		t.Fatal(err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	acknowledgement, err := controller.ReplacementAcknowledgement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if acknowledgement == nil || !plansEqual(acknowledgement.Prior(), prior) ||
		!plansEqual(acknowledgement.Next(), next) {
		t.Fatalf("reopened acknowledgement = %#v", acknowledgement)
	}
	closeReplacementController(t, controller)
}

func TestReplacementAcknowledgementRejectsNeverCommittedOperation(t *testing.T) {
	agent := controllerAgent(t, "com.example.never-committed")
	controller, _, _ := newReplacementController(t, agent)
	plan := replacementPlan(t, agent)
	if err := controller.AcknowledgeReplacementCommit(
		t.Context(), "never-committed", replacementBinding("never-committed"), plan, plan,
	); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("AcknowledgeReplacementCommit = %v, want ErrReplacementMismatch", err)
	}
	acknowledgement, err := controller.ReplacementAcknowledgement(t.Context())
	if err != nil || acknowledgement != nil {
		t.Fatalf("acknowledgement = %#v, %v", acknowledgement, err)
	}
}

func TestSharedExecutablePlanQuiescesAndReopensWithOneProofPath(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	directory := t.TempDir()
	config := ControllerConfig{
		StatePath:   filepath.Join(directory, "services.db"),
		ProcessPath: filepath.Join(directory, "processes.db"), WorkerLimit: 1,
	}
	launchd := &replacementLaunchd{loaded: make(map[string]bool)}
	first := controllerAgent(t, "com.example.shared-executable-first")
	second := controllerAgent(t, "com.example.shared-executable-second")
	if first.Program != second.Program {
		t.Fatalf("programs differ: %q != %q", first.Program, second.Program)
	}
	plan := replacementPlan(t, first, second)
	controller := openDurableReplacementController(t, config, launchd)
	if err := controller.Converge(t.Context(), plan.Agents()); err != nil {
		t.Fatal(err)
	}
	receipt, err := controller.Quiesce(
		t.Context(), "shared-executable", replacementBinding("shared-executable"), plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	paths := mustProgramPaths(t, plan)
	if !slices.Equal(paths, []string{first.Program}) {
		t.Fatalf("program paths = %q, want one %q", paths, first.Program)
	}
	proveReplacement(t, controller, receipt)
	if err := controller.ProveQuiesced(t.Context(), receipt, []string{first.Program, second.Program}); err != nil {
		t.Fatalf("replayed proof with duplicate role paths = %v", err)
	}
	closeReplacementController(t, controller)

	controller = openDurableReplacementController(t, config, launchd)
	defer closeReplacementController(t, controller)
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || status.Phase != ReplacementQuiesced || len(status.Proofs) != 1 ||
		!slices.Equal(status.Proofs[0].ProgramPaths, paths) {
		t.Fatalf("reopened shared-executable status = %#v", status)
	}
}

func TestReplacementRecoverySuppressesRestartAlwaysBeforeReceipts(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	agent := controllerAgent(t, "com.example.crash-recovery")
	plan := replacementPlan(t, agent)
	replacement := &replacementState{
		OperationID: "replace-crash", Phase: ReplacementUnloaded, Epoch: 1,
		Binding: replacementBinding("replace-crash"), Prior: plan, Current: plan,
	}
	events := []string{}
	launchd := &replacementLaunchd{loaded: map[string]bool{serviceTarget(agent.Label): true}}
	runtime := &controllerRuntimeStub{events: &events, run: launchd.run}
	store := &controllerStoreStub{events: &events, state: controllerState{
		Desired: map[string]Agent{}, Applied: map[string]Agent{agent.Label: agent}, Replacement: replacement,
	}}
	receipts := &controllerReceiptsStub{events: &events}
	controller, err := newControllerWithRuntime(t.Context(), controllerConfig(t), runtime, receipts, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controller.Close(context.Background()) })
	bootout := slices.IndexFunc(events, func(event string) bool { return strings.Contains(event, "run:bootout ") })
	ack := slices.IndexFunc(events, func(event string) bool { return strings.HasPrefix(event, "recover-receipts:") })
	if bootout < 0 || ack < 0 || bootout >= ack || store.state.Replacement == nil || launchd.loaded[serviceTarget(agent.Label)] {
		t.Fatalf("recovery events/state = %v %#v loaded=%v", events, store.state, launchd.loaded)
	}
}

func TestCommitReplacementRejectsDriftBeforeClearingFence(t *testing.T) {
	agent := controllerAgent(t, "com.example.commit-drift")
	controller, store, launchd := newReplacementController(t, agent)
	plan := replacementPlan(t, agent)
	binding := replacementBinding("replace-drift")
	receipt, err := controller.Quiesce(t.Context(), "replace-drift", binding, plan)
	if err != nil {
		t.Fatal(err)
	}
	proveReplacement(t, controller, receipt)
	if err := controller.ApplyReplacement(t.Context(), "replace-drift", binding, plan); err != nil {
		t.Fatal(err)
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	launchd.loaded[serviceTarget(agent.Label)] = false
	if _, err := controller.CommitReplacement(t.Context(), "replace-drift", binding, plan); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("commit drift = %v, want ErrReplacementMismatch", err)
	}
	if store.state.Replacement == nil {
		t.Fatal("drift rejection cleared replacement fence")
	}
	if err := controller.install(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.CommitReplacement(t.Context(), "replace-drift", binding, plan); err != nil {
		t.Fatal(err)
	}
	if store.state.Replacement != nil || !launchd.loaded[serviceTarget(agent.Label)] {
		t.Fatalf("exact retry did not clear fence: %#v loaded=%v", store.state, launchd.loaded)
	}
}

func TestReplacementStatusAndReceiptAreDefensiveCopies(t *testing.T) {
	agent := controllerAgent(t, "com.example.copy")
	controller, _, _ := newReplacementController(t, agent)
	plan := replacementPlan(t, agent)
	receipt, err := controller.Quiesce(t.Context(), "replace-copy", replacementBinding("replace-copy"), plan)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Plan.agents[agent.Label] = Agent{}
	status, err := controller.ReplacementStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || reflect.DeepEqual(status.Current.agents[agent.Label], Agent{}) {
		t.Fatal("receipt mutation changed controller state")
	}
}
