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
	"github.com/yasyf/daemonkit/supervise"
)

type replacementLaunchd struct {
	loaded map[string]bool
	events []string
}

func (l *replacementLaunchd) run(_ context.Context, task supervise.Task) error {
	l.events = append(l.events, strings.Join(task.Args, " "))
	if len(task.Args) == 0 {
		return errors.New("empty launchctl arguments")
	}
	switch task.Args[0] {
	case "print":
		if l.loaded[task.Args[1]] {
			return nil
		}
		return launchctlExit(launchctlNotLoadedExit)
	case "bootout":
		if len(task.Args) > 1 {
			l.loaded[task.Args[1]] = false
		}
		return nil
	case "bootstrap":
		return nil
	case "enable":
		return nil
	case "kickstart":
		if len(task.Args) > 1 {
			l.loaded[task.Args[1]] = true
		}
		return nil
	default:
		return nil
	}
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
	if err := controller.CommitReplacement(t.Context(), "replace-1", binding, prior); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("CommitReplacement stale plan = %v", err)
	}
	if store.state.Replacement == nil {
		t.Fatal("mismatched commit cleared fence")
	}
	if err := controller.CommitReplacement(t.Context(), "replace-1", binding, next); err != nil {
		t.Fatal(err)
	}
	if status, err := controller.ReplacementStatus(t.Context()); err != nil || status != nil {
		t.Fatalf("committed status = %#v, %v", status, err)
	}
	if err := controller.Converge(t.Context(), next.Agents()); err != nil {
		t.Fatalf("Converge after commit = %v", err)
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
	if err := controller.CommitReplacement(t.Context(), "replace-rollback", binding, prior); err != nil {
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
	if _, err := store.SetReplacement(t.Context(), map[string]Agent{}, replacement); err != nil {
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
	if err := controller.CommitReplacement(t.Context(), "replace-drift", binding, plan); !errors.Is(err, ErrReplacementMismatch) {
		t.Fatalf("commit drift = %v, want ErrReplacementMismatch", err)
	}
	if store.state.Replacement == nil {
		t.Fatal("drift rejection cleared replacement fence")
	}
	if err := controller.install(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if err := controller.CommitReplacement(t.Context(), "replace-drift", binding, plan); err != nil {
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
