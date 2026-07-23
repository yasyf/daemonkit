package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	replacementQuiet = 250 * time.Millisecond
	replacementPoll  = 25 * time.Millisecond
)

var (
	// ErrQuiesced means an ordinary convergence request raced a deployment-owned fence.
	ErrQuiesced = errors.New("service: replacement fence is active")
	// ErrReplacementMismatch means a replacement operation, plan, or receipt is stale.
	ErrReplacementMismatch = errors.New("service: replacement fence mismatch")
	// ErrNotQuiesced means launchd or an exact executable remains live.
	ErrNotQuiesced = errors.New("service: replacement is not quiesced")
)

// PlanDigest is the immutable digest of one canonical service plan.
type PlanDigest [sha256.Size]byte

// String returns the lowercase hexadecimal digest.
func (d PlanDigest) String() string { return hex.EncodeToString(d[:]) }

// ReplacementBinding binds one fence to an exact consumer build and policy.
type ReplacementBinding [sha256.Size]byte

// String returns the lowercase hexadecimal binding.
func (b ReplacementBinding) String() string { return hex.EncodeToString(b[:]) }

func (b ReplacementBinding) validate() error {
	if b == (ReplacementBinding{}) {
		return errors.New("service: replacement binding is required")
	}
	return nil
}

// Plan is an immutable canonical set of LaunchAgents.
type Plan struct {
	agents map[string]Agent
	digest PlanDigest
	valid  bool
}

// NewPlan validates and canonicalizes one complete LaunchAgent set.
func NewPlan(agents []Agent) (Plan, error) {
	canonical, err := desiredAgents(agents)
	if err != nil {
		return Plan{}, err
	}
	return planFromAgents(canonical)
}

func planFromAgents(agents map[string]Agent) (Plan, error) {
	type encodedAgent struct {
		Label string          `json:"label"`
		Agent json.RawMessage `json:"agent"`
	}
	wire := struct {
		Identity string         `json:"identity"`
		Schema   int            `json:"schema"`
		Agents   []encodedAgent `json:"agents"`
	}{Identity: "daemonkit.service.plan.v1", Schema: 1}
	for _, label := range slices.Sorted(maps.Keys(agents)) {
		agent := agents[label]
		if label != agent.Label {
			return Plan{}, fmt.Errorf("service: plan key %q does not match agent label %q", label, agent.Label)
		}
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return Plan{}, err
		}
		wire.Agents = append(wire.Agents, encodedAgent{Label: label, Agent: payload})
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return Plan{}, fmt.Errorf("service: encode plan: %w", err)
	}
	return Plan{agents: copyAgents(agents), digest: sha256.Sum256(payload), valid: true}, nil
}

// Agents returns a defensive copy in canonical label order.
func (p Plan) Agents() []Agent {
	agents := make([]Agent, 0, len(p.agents))
	for _, label := range slices.Sorted(maps.Keys(p.agents)) {
		agent := copyAgents(map[string]Agent{label: p.agents[label]})[label]
		agents = append(agents, agent)
	}
	return agents
}

// Digest returns the canonical plan digest.
func (p Plan) Digest() PlanDigest { return p.digest }

func (p Plan) validate() error {
	if !p.valid {
		return errors.New("service: plan was not created by NewPlan or Snapshot")
	}
	canonical, err := planFromAgents(p.agents)
	if err != nil {
		return err
	}
	if canonical.digest != p.digest {
		return errors.New("service: plan digest does not match its agents")
	}
	return nil
}

func plansEqual(left, right Plan) bool {
	return left.valid && right.valid && left.digest == right.digest && reflect.DeepEqual(left.agents, right.agents)
}

// ReplacementPhase is the durable ownership phase of a service replacement.
type ReplacementPhase string

const (
	// ReplacementUnloaded means launchd ownership is durably suppressed pending exact-stop proof.
	ReplacementUnloaded ReplacementPhase = "unloaded"
	// ReplacementQuiesced means the current plan has a durable exact-stop proof.
	ReplacementQuiesced ReplacementPhase = "quiesced"
	// ReplacementRunningOwned means the fenced plan is live but deployment still owns it.
	ReplacementRunningOwned ReplacementPhase = "running-owned"
)

// QuiescenceProof records one exact executable-absence proof.
type QuiescenceProof struct {
	Epoch        uint64
	PlanDigest   PlanDigest
	ProgramPaths []string
	ProvedAt     time.Time
}

type replacementProof struct {
	Epoch        uint64
	PlanDigest   PlanDigest
	ProgramPaths []string
	ProvedAt     time.Time
}

type replacementState struct {
	OperationID string
	Binding     ReplacementBinding
	Phase       ReplacementPhase
	Epoch       uint64
	Prior       Plan
	Current     Plan
	Proofs      []replacementProof
}

func copyReplacement(replacement *replacementState) *replacementState {
	if replacement == nil {
		return nil
	}
	copied := *replacement
	copied.Prior = Plan{agents: copyAgents(replacement.Prior.agents), digest: replacement.Prior.digest, valid: replacement.Prior.valid}
	copied.Current = Plan{agents: copyAgents(replacement.Current.agents), digest: replacement.Current.digest, valid: replacement.Current.valid}
	copied.Proofs = make([]replacementProof, len(replacement.Proofs))
	for index, proof := range replacement.Proofs {
		copied.Proofs[index] = proof
		copied.Proofs[index].ProgramPaths = append([]string(nil), proof.ProgramPaths...)
	}
	return &copied
}

// ReplacementStatus is a defensive snapshot of the durable replacement fence.
type ReplacementStatus struct {
	OperationID string
	Binding     ReplacementBinding
	Phase       ReplacementPhase
	Epoch       uint64
	Prior       Plan
	Current     Plan
	Proofs      []QuiescenceProof
}

func statusFromReplacement(replacement *replacementState) *ReplacementStatus {
	if replacement == nil {
		return nil
	}
	status := &ReplacementStatus{
		OperationID: replacement.OperationID,
		Binding:     replacement.Binding,
		Phase:       replacement.Phase,
		Epoch:       replacement.Epoch,
		Prior:       copyReplacement(replacement).Prior,
		Current:     copyReplacement(replacement).Current,
	}
	for _, proof := range replacement.Proofs {
		status.Proofs = append(status.Proofs, QuiescenceProof{
			Epoch: proof.Epoch, PlanDigest: proof.PlanDigest,
			ProgramPaths: append([]string(nil), proof.ProgramPaths...), ProvedAt: proof.ProvedAt,
		})
	}
	return status
}

// QuiesceReceipt binds a product-owned exact stop to one fence epoch and plan.
type QuiesceReceipt struct {
	OperationID string
	Binding     ReplacementBinding
	Epoch       uint64
	Plan        Plan
}

func receiptFromReplacement(replacement *replacementState) QuiesceReceipt {
	return QuiesceReceipt{
		OperationID: replacement.OperationID,
		Binding:     replacement.Binding,
		Epoch:       replacement.Epoch,
		Plan:        copyReplacement(replacement).Current,
	}
}

// Snapshot returns the exact currently desired service plan.
func (c *Controller) Snapshot(ctx context.Context) (Plan, error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer finish()
	if err := opCtx.Err(); err != nil {
		return Plan{}, err
	}
	return planFromAgents(c.state.Desired)
}

// ReplacementStatus returns the active durable fence, if any.
func (c *Controller) ReplacementStatus(ctx context.Context) (*ReplacementStatus, error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if err := opCtx.Err(); err != nil {
		return nil, err
	}
	return statusFromReplacement(c.state.Replacement), nil
}

// Quiesce durably fences the expected plan, suppresses RestartAlways, and unloads it.
func (c *Controller) Quiesce(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
	expected Plan,
) (QuiesceReceipt, error) {
	if err := validateReplacementOperationID(operationID); err != nil {
		return QuiesceReceipt{}, err
	}
	if err := binding.validate(); err != nil {
		return QuiesceReceipt{}, err
	}
	if err := expected.validate(); err != nil {
		return QuiesceReceipt{}, err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return QuiesceReceipt{}, err
	}
	defer finish()
	if existing := c.state.Replacement; existing != nil {
		if existing.OperationID != operationID || existing.Binding != binding || !plansEqual(existing.Prior, expected) ||
			(existing.Phase != ReplacementUnloaded && existing.Phase != ReplacementQuiesced) {
			return QuiesceReceipt{}, ErrQuiesced
		}
		if err := c.reconcile(opCtx, copyAgents(c.state.Applied), c.state.Desired); err != nil {
			return QuiesceReceipt{}, err
		}
		return receiptFromReplacement(existing), nil
	}
	current, err := planFromAgents(c.state.Desired)
	if err != nil {
		return QuiesceReceipt{}, err
	}
	if !plansEqual(current, expected) || !reflect.DeepEqual(c.state.Applied, c.state.Desired) {
		return QuiesceReceipt{}, fmt.Errorf("%w: expected prior plan %s, current desired %s", ErrReplacementMismatch, expected.digest, current.digest)
	}
	if err := c.requireExactPlanLoaded(opCtx, expected); err != nil {
		return QuiesceReceipt{}, fmt.Errorf("%w: prior plan is not exact and loaded: %w", ErrReplacementMismatch, err)
	}
	replacement := &replacementState{
		OperationID: operationID, Phase: ReplacementUnloaded, Epoch: 1,
		Binding: binding, Prior: expected, Current: expected,
	}
	if err := c.transitionReplacement(opCtx, map[string]Agent{}, replacement); err != nil {
		return QuiesceReceipt{}, err
	}
	if err := c.reconcile(opCtx, copyAgents(c.state.Applied), c.state.Desired); err != nil {
		return QuiesceReceipt{}, err
	}
	return receiptFromReplacement(c.state.Replacement), nil
}

// ProveQuiesced records continuous launchd and executable absence for one receipt.
func (c *Controller) ProveQuiesced(ctx context.Context, receipt QuiesceReceipt, programPaths []string) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return err
	}
	defer finish()
	replacement, err := c.matchReceipt(receipt, ReplacementUnloaded)
	if err != nil {
		return err
	}
	expectedPaths, err := replacement.Current.programPaths()
	if err != nil {
		return err
	}
	paths, err := exactProgramPaths(programPaths)
	if err != nil {
		return err
	}
	if !slices.Equal(paths, expectedPaths) {
		return fmt.Errorf("%w: program paths %q differ from fenced plan %q", ErrReplacementMismatch, paths, expectedPaths)
	}
	quietSince := time.Time{}
	for {
		if err := c.requireReplacementUnloaded(opCtx, replacement.Current); err != nil {
			return err
		}
		empty := true
		for _, path := range paths {
			identities, err := c.replacementProcesses(path)
			if err != nil {
				return fmt.Errorf("service: inspect replacement executable %q: %w", path, err)
			}
			if len(identities) != 0 {
				empty = false
				break
			}
		}
		now := c.replacementNow()
		if empty {
			if quietSince.IsZero() {
				quietSince = now
			}
			if now.Sub(quietSince) >= replacementQuiet {
				break
			}
		} else {
			quietSince = time.Time{}
		}
		if err := c.replacementWait(opCtx, replacementPoll); err != nil {
			return err
		}
	}
	updated := copyReplacement(replacement)
	updated.Phase = ReplacementQuiesced
	updated.Proofs = append(updated.Proofs, replacementProof{
		Epoch: updated.Epoch, PlanDigest: updated.Current.digest,
		ProgramPaths: paths, ProvedAt: c.replacementNow().UTC(),
	})
	return c.transitionReplacement(opCtx, map[string]Agent{}, updated)
}

// ApplyReplacement starts next while retaining deployment ownership of the fence.
func (c *Controller) ApplyReplacement(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
	next Plan,
) error {
	return c.resumeReplacement(ctx, operationID, binding, next)
}

// RestoreReplacement restarts the exact prior plan while retaining the fence.
func (c *Controller) RestoreReplacement(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return err
	}
	defer finish()
	replacement := c.state.Replacement
	if replacement == nil || replacement.OperationID != operationID || replacement.Binding != binding {
		return ErrReplacementMismatch
	}
	return c.resumeReplacementLocked(opCtx, replacement, replacement.Prior)
}

func (c *Controller) resumeReplacement(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
	next Plan,
) error {
	if err := validateReplacementOperationID(operationID); err != nil {
		return err
	}
	if err := next.validate(); err != nil {
		return err
	}
	if err := binding.validate(); err != nil {
		return err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return err
	}
	defer finish()
	replacement := c.state.Replacement
	if replacement == nil || replacement.OperationID != operationID || replacement.Binding != binding {
		return ErrReplacementMismatch
	}
	return c.resumeReplacementLocked(opCtx, replacement, next)
}

func (c *Controller) resumeReplacementLocked(ctx context.Context, replacement *replacementState, next Plan) error {
	if replacement.Phase == ReplacementRunningOwned && plansEqual(replacement.Current, next) {
		return c.reconcile(ctx, copyAgents(c.state.Applied), c.state.Desired)
	}
	if replacement.Phase != ReplacementQuiesced {
		return fmt.Errorf("%w: cannot resume phase %q", ErrReplacementMismatch, replacement.Phase)
	}
	updated := copyReplacement(replacement)
	updated.Current = next
	updated.Phase = ReplacementRunningOwned
	if err := c.transitionReplacement(ctx, next.agents, updated); err != nil {
		return err
	}
	return c.reconcile(ctx, copyAgents(c.state.Applied), c.state.Desired)
}

// Requiesce suppresses the currently deployment-owned plan before another exact stop.
func (c *Controller) Requiesce(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
) (QuiesceReceipt, error) {
	if err := validateReplacementOperationID(operationID); err != nil {
		return QuiesceReceipt{}, err
	}
	if err := binding.validate(); err != nil {
		return QuiesceReceipt{}, err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return QuiesceReceipt{}, err
	}
	defer finish()
	replacement := c.state.Replacement
	if replacement == nil || replacement.OperationID != operationID || replacement.Binding != binding {
		return QuiesceReceipt{}, ErrReplacementMismatch
	}
	if replacement.Phase == ReplacementRunningOwned {
		updated := copyReplacement(replacement)
		updated.Phase = ReplacementUnloaded
		updated.Epoch++
		if err := c.transitionReplacement(opCtx, map[string]Agent{}, updated); err != nil {
			return QuiesceReceipt{}, err
		}
	} else if replacement.Phase != ReplacementUnloaded {
		return QuiesceReceipt{}, fmt.Errorf("%w: cannot requiesce phase %q", ErrReplacementMismatch, replacement.Phase)
	}
	if err := c.reconcile(opCtx, copyAgents(c.state.Applied), c.state.Desired); err != nil {
		return QuiesceReceipt{}, err
	}
	return receiptFromReplacement(c.state.Replacement), nil
}

// CommitReplacement clears the fence only for the exact durable deployment plan.
func (c *Controller) CommitReplacement(
	ctx context.Context,
	operationID string,
	binding ReplacementBinding,
	expected Plan,
) error {
	if err := validateReplacementOperationID(operationID); err != nil {
		return err
	}
	if err := expected.validate(); err != nil {
		return err
	}
	if err := binding.validate(); err != nil {
		return err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	opCtx, finish, err := c.admit(ctx)
	if err != nil {
		return err
	}
	defer finish()
	replacement := c.state.Replacement
	if replacement == nil || replacement.OperationID != operationID || replacement.Binding != binding ||
		replacement.Phase != ReplacementRunningOwned || !plansEqual(replacement.Current, expected) {
		return ErrReplacementMismatch
	}
	if !reflect.DeepEqual(c.state.Desired, expected.agents) || !reflect.DeepEqual(c.state.Applied, expected.agents) {
		return fmt.Errorf("%w: committed service state differs from deployment receipt", ErrReplacementMismatch)
	}
	if err := c.requireExactPlanLoaded(opCtx, expected); err != nil {
		return fmt.Errorf("%w: committed plan is not exact and loaded: %w", ErrReplacementMismatch, err)
	}
	return c.transitionReplacement(opCtx, expected.agents, nil)
}

func (c *Controller) transitionReplacement(ctx context.Context, desired map[string]Agent, replacement *replacementState) error {
	state, err := c.store.SetReplacement(ctx, desired, replacement)
	if err != nil {
		return err
	}
	c.state = state
	return nil
}

func (c *Controller) matchReceipt(receipt QuiesceReceipt, phase ReplacementPhase) (*replacementState, error) {
	replacement := c.state.Replacement
	if replacement == nil || replacement.OperationID != receipt.OperationID ||
		replacement.Binding != receipt.Binding || replacement.Epoch != receipt.Epoch || replacement.Phase != phase ||
		!plansEqual(replacement.Current, receipt.Plan) {
		return nil, ErrReplacementMismatch
	}
	return replacement, nil
}

func (c *Controller) requireReplacementUnloaded(ctx context.Context, plan Plan) error {
	for _, agent := range plan.Agents() {
		_, err := c.launchctl(ctx, "print", serviceTarget(agent.Label))
		if err == nil {
			return fmt.Errorf("%w: agent %q remains loaded", ErrNotQuiesced, agent.Label)
		}
		if launchctlExitCode(err) != launchctlNotLoadedExit {
			return fmt.Errorf("service: inspect quiesced agent %q: %w", agent.Label, err)
		}
	}
	return nil
}

func (c *Controller) requireExactPlanLoaded(ctx context.Context, plan Plan) error {
	for _, agent := range plan.Agents() {
		exact, err := c.verify(ctx, agent)
		if err != nil {
			return fmt.Errorf("verify agent %q: %w", agent.Label, err)
		}
		if !exact {
			return fmt.Errorf("agent %q is not exact and loaded", agent.Label)
		}
	}
	return nil
}

func (p Plan) programPaths() ([]string, error) {
	paths := make([]string, 0, len(p.agents))
	for _, agent := range p.Agents() {
		path, err := agent.programPath()
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return exactProgramPaths(paths)
}

func exactProgramPaths(paths []string) ([]string, error) {
	canonical := append([]string(nil), paths...)
	slices.Sort(canonical)
	canonical = slices.Compact(canonical)
	if len(canonical) != len(paths) {
		return nil, errors.New("service: replacement program paths contain duplicates")
	}
	for _, path := range canonical {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("service: replacement program path %q is not exact and absolute", path)
		}
	}
	return canonical, nil
}

func validateReplacementOperationID(operationID string) error {
	if operationID == "" || strings.TrimSpace(operationID) != operationID || len(operationID) > 256 {
		return errors.New("service: replacement operation ID must be exact and non-empty")
	}
	return nil
}

func defaultReplacementWait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
