package deployment

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/service"
)

const (
	deploymentSchema      = 1
	deploymentIdentity    = "daemonkit.deployment.transaction.v1"
	deploymentFingerprint = "8d3c5de15ddabe1c9ab19093239d7f3f222c62dbef813dd028d46c2a4434e127"
	receiptIdentity       = "daemonkit.deployment.receipt.v1"
	receiptFingerprint    = "87f517a7e2d67df3c3aa715aa8adc1918d393e2755cc61a47243b7a672729311"
	serviceWorkerLimit    = 4
)

// ErrRecoveryRequired means the deployment remains durably fenced and needs
// an operator to resolve state that cannot be classified without guessing.
var ErrRecoveryRequired = errors.New("deployment: deployment recovery required")

// ErrAmbiguousState means exact transaction, namespace, receipt, and service
// observations do not identify one safe recovery transition.
var ErrAmbiguousState = errors.New("deployment: ambiguous deployment state")

// RuntimeStopper is the narrow controller authority available to the product's
// exact-runtime quiesce hook.
type RuntimeStopper interface {
	StopRuntime(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error)
}

// RuntimeProof records product evidence for one exact runtime settlement.
// Absent and ProcessGeneration are mutually exclusive.
type RuntimeProof struct {
	Role              ProofRole
	Absent            bool
	ProcessGeneration string
	Digest            SHA256
}

// Proof records deterministic product evidence for a generation-bound check.
type Proof struct {
	Role       ProofRole
	PlanDigest SHA256
	Digest     SHA256
}

// ProofRole names the exact semantic domain of one callback proof.
type ProofRole string

// Proof roles bind evidence to one exact callback boundary.
const (
	ProofPostInstall     ProofRole = "post_install"
	ProofCandidateReady  ProofRole = "candidate_ready"
	ProofPriorRestore    ProofRole = "prior_restore"
	ProofPriorReady      ProofRole = "prior_ready"
	ProofPriorRuntime    ProofRole = "prior_runtime"
	ProofRollbackRuntime ProofRole = "rollback_runtime"
)

// CanonicalGeneration identifies exact signed bytes at the fixed app path.
// Device and Inode are opaque filesystem-generation components, not runtime
// process generations.
type CanonicalGeneration struct {
	Path                  string
	Release               Release
	DesignatedRequirement string
	CDHash                string
	BundleDigest          SHA256
	Device                string
	Inode                 string
}

// Operation binds a product hook to one deployment and bundle generation.
type Operation struct {
	ID         string
	Generation CanonicalGeneration
	Role       ProofRole
	PlanDigest SHA256
}

// RuntimeQuiesceOperation binds a runtime stop callback to one exact deployment generation.
type RuntimeQuiesceOperation struct {
	ID         string
	Generation CanonicalGeneration
	Role       ProofRole
}

// Config is the complete product input to one fixed signed-app
// deployment. Every callback must be idempotent and must not rename, replace,
// or remove the canonical app.
type Config struct {
	Dir           string
	AppName       string
	Release       Release
	Identity      codeidentity.CodeIdentity
	ConsumerBuild string
	PolicyDigest  SHA256

	RuntimeQuiesce       func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error)
	PostInstallProof     func(context.Context, Operation) (Proof, error)
	PriorAppRestoreProof func(context.Context, Operation) (Proof, error)
	BuildPlan            func(context.Context, Operation) (service.Plan, error)
	Readiness            func(context.Context, Operation, service.Plan) (Proof, error)
}

// DeactivateConfig identifies one receipted installed app and the current
// product callback implementation used to retire its exact runtime.
type DeactivateConfig struct {
	Dir           string
	AppName       string
	Identity      codeidentity.CodeIdentity
	ConsumerBuild string
	PolicyDigest  SHA256

	RuntimeQuiesce func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error)
	Readiness      func(context.Context, Operation, service.Plan) (Proof, error)
}

// DeploymentState is the externally inspectable durable outcome.
type DeploymentState string //nolint:revive // the qualifier distinguishes it from service state

// Deployment states are the only durable receipt outcomes.
const (
	DeploymentActive           DeploymentState = "active"
	DeploymentInactive         DeploymentState = "inactive"
	DeploymentRecoveryRequired DeploymentState = "recovery_required"
)

// DeploymentReceipt is the exact durable outcome of Deploy, Deactivate, or Recover.
type DeploymentReceipt struct { //nolint:revive // explicit at consumer call sites
	operationID    string
	state          DeploymentState
	current        *CanonicalGeneration
	plan           service.Plan
	activationPlan service.Plan
	failure        string
}

// OperationID returns the exact operation that produced the receipt.
func (r DeploymentReceipt) OperationID() string { return r.operationID }

// State returns the receipt's finite durable state.
func (r DeploymentReceipt) State() DeploymentState { return r.state }

// Current returns the sealed installed generation when one exists.
func (r DeploymentReceipt) Current() (CanonicalGeneration, bool) {
	if r.current == nil {
		return CanonicalGeneration{}, false
	}
	return *r.current, true
}

// Plan returns the exact currently active service plan.
func (r DeploymentReceipt) Plan() service.Plan { return r.plan }

// ActivationPlan returns the exact plan retained for reactivation.
func (r DeploymentReceipt) ActivationPlan() service.Plan { return r.activationPlan }

// Failure returns the bounded failure attached to a recovery-required receipt.
func (r DeploymentReceipt) Failure() string { return r.failure }

// DeactivationState is the finite outcome of Deactivate.
type DeactivationState string

// Deactivation states distinguish managed absence from retained inactivity.
const (
	DeactivationAbsent   DeactivationState = "absent"
	DeactivationInactive DeactivationState = "inactive"
)

// DeactivationResult distinguishes a never-installed app from an exact
// retained inactive deployment receipt.
type DeactivationResult struct {
	state   DeactivationState
	receipt *DeploymentReceipt
}

// State returns the finite deactivation outcome.
func (r DeactivationResult) State() DeactivationState { return r.state }

// Receipt returns the exact retained inactive receipt when one exists.
func (r DeactivationResult) Receipt() (DeploymentReceipt, bool) {
	if r.receipt == nil {
		return DeploymentReceipt{}, false
	}
	return *r.receipt, true
}

// RecoveryState is the finite durable observation returned by Recover.
type RecoveryState string

// Recovery states classify the complete post-recovery durable state.
const (
	RecoveryAbsent   RecoveryState = "absent"
	RecoveryActive   RecoveryState = "active"
	RecoveryInactive RecoveryState = "inactive"
	RecoveryRequired RecoveryState = "recovery_required"
)

// RecoveryResult carries either an exact completed receipt or typed managed
// absence after recovery.
type RecoveryResult struct {
	state   RecoveryState
	receipt *DeploymentReceipt
}

// State returns the finite recovery outcome.
func (r RecoveryResult) State() RecoveryState { return r.state }

// Receipt returns the exact completed or recovery-required receipt when one exists.
func (r RecoveryResult) Receipt() (DeploymentReceipt, bool) {
	if r.receipt == nil {
		return DeploymentReceipt{}, false
	}
	return *r.receipt, true
}

// DeploymentStatus is a read-only observation. Status never repairs state.
type DeploymentStatus struct { //nolint:revive // explicit at consumer call sites
	receipt            *DeploymentReceipt
	operationID        string
	consumerBuild      string
	policyDigest       string
	replacementBinding string
	phase              Phase
	direction          Direction
	canonical          *CanonicalGeneration
	staged             *CanonicalGeneration
	failure            string
	configMatches      bool
	configMismatch     string
	recoveryRequired   bool
}

// Receipt returns the exact completed receipt observed by Status when one exists.
func (s DeploymentStatus) Receipt() (DeploymentReceipt, bool) {
	if s.receipt == nil {
		return DeploymentReceipt{}, false
	}
	return *s.receipt, true
}

// OperationID returns the active transaction operation, if any.
func (s DeploymentStatus) OperationID() string { return s.operationID }

// ConsumerBuild returns the callback implementation bound to the observation.
func (s DeploymentStatus) ConsumerBuild() string { return s.consumerBuild }

// PolicyDigest returns the policy digest bound to the observation.
func (s DeploymentStatus) PolicyDigest() string { return s.policyDigest }

// ReplacementBinding returns the exact service replacement binding.
func (s DeploymentStatus) ReplacementBinding() string { return s.replacementBinding }

// Phase returns the active durable transaction phase, if any.
func (s DeploymentStatus) Phase() Phase { return s.phase }

// Direction returns the active transaction direction, if any.
func (s DeploymentStatus) Direction() Direction { return s.direction }

// Canonical returns the sealed canonical generation observed by Status.
func (s DeploymentStatus) Canonical() (CanonicalGeneration, bool) {
	if s.canonical == nil {
		return CanonicalGeneration{}, false
	}
	return *s.canonical, true
}

// Staged returns the sealed staged generation observed by Status.
func (s DeploymentStatus) Staged() (CanonicalGeneration, bool) {
	if s.staged == nil {
		return CanonicalGeneration{}, false
	}
	return *s.staged, true
}

// Failure returns the bounded active-transaction failure.
func (s DeploymentStatus) Failure() string { return s.failure }

// ConfigMatches reports whether the queried config exactly matches the observation.
func (s DeploymentStatus) ConfigMatches() bool { return s.configMatches }

// ConfigMismatch returns the exact mismatch classification.
func (s DeploymentStatus) ConfigMismatch() string { return s.configMismatch }

// RecoveryRequired reports whether Status observed a state requiring explicit recovery.
func (s DeploymentStatus) RecoveryRequired() bool { return s.recoveryRequired }

func completedDeploymentReceipt(
	operationID string,
	state DeploymentState,
	current CanonicalGeneration,
	plan service.Plan,
	activationPlan service.Plan,
) DeploymentReceipt {
	return DeploymentReceipt{
		operationID:    operationID,
		state:          state,
		current:        &current,
		plan:           plan,
		activationPlan: activationPlan,
	}
}

func recoveryRequiredDeploymentReceipt(operationID, failure string) DeploymentReceipt {
	return DeploymentReceipt{operationID: operationID, state: DeploymentRecoveryRequired, failure: failure}
}

func absentDeactivationResult() DeactivationResult {
	return DeactivationResult{state: DeactivationAbsent}
}

func inactiveDeactivationResult(receipt DeploymentReceipt) DeactivationResult {
	return DeactivationResult{state: DeactivationInactive, receipt: &receipt}
}

func absentRecoveryResult() RecoveryResult { return RecoveryResult{state: RecoveryAbsent} }

func completedRecoveryResult(receipt DeploymentReceipt) RecoveryResult {
	state := RecoveryActive
	if receipt.state == DeploymentInactive {
		state = RecoveryInactive
	}
	return RecoveryResult{state: state, receipt: &receipt}
}

func requiredRecoveryResult(receipt DeploymentReceipt) RecoveryResult {
	return RecoveryResult{state: RecoveryRequired, receipt: &receipt}
}

func absentDeploymentStatus() DeploymentStatus {
	return DeploymentStatus{configMatches: true}
}

func newDeploymentStatus() DeploymentStatus { return DeploymentStatus{} }

type deploymentController interface {
	RuntimeStopper
	Snapshot(context.Context) (service.Plan, error)
	ReplacementStatus(context.Context) (*service.ReplacementStatus, error)
	DeploymentCompletion(context.Context) (*replacementCompletion, error)
	DeploymentAcknowledgement(context.Context) (*replacementCompletion, error)
	Quiesce(context.Context, string, service.ReplacementBinding, service.Plan) (service.QuiesceReceipt, error)
	ProveQuiesced(context.Context, service.QuiesceReceipt, []string) error
	ApplyReplacement(context.Context, string, service.ReplacementBinding, service.Plan) error
	Requiesce(context.Context, string, service.ReplacementBinding) (service.QuiesceReceipt, error)
	RestoreReplacement(context.Context, string, service.ReplacementBinding) error
	CommitDeploymentReplacement(context.Context, string, service.ReplacementBinding, service.Plan) (replacementCompletion, error)
	AcknowledgeDeploymentReplacement(context.Context, replacementCompletion) error
	Close(context.Context) error
}

type replacementCompletion struct {
	OperationID string
	Binding     service.ReplacementBinding
	Prior       service.Plan
	Next        service.Plan
}

type serviceControllerAdapter struct{ *service.Controller }

func (a serviceControllerAdapter) DeploymentCompletion(ctx context.Context) (*replacementCompletion, error) {
	commit, err := a.ReplacementCompletion(ctx)
	if err != nil || commit == nil {
		return nil, err
	}
	return &replacementCompletion{
		OperationID: commit.OperationID(), Binding: commit.Binding(),
		Prior: commit.Prior(), Next: commit.Next(),
	}, nil
}

func (a serviceControllerAdapter) DeploymentAcknowledgement(ctx context.Context) (*replacementCompletion, error) {
	commit, err := a.ReplacementAcknowledgement(ctx)
	if err != nil || commit == nil {
		return nil, err
	}
	return &replacementCompletion{
		OperationID: commit.OperationID(), Binding: commit.Binding(),
		Prior: commit.Prior(), Next: commit.Next(),
	}, nil
}

func (a serviceControllerAdapter) CommitDeploymentReplacement(
	ctx context.Context,
	operationID string,
	binding service.ReplacementBinding,
	next service.Plan,
) (replacementCompletion, error) {
	commit, err := a.CommitReplacement(ctx, operationID, binding, next)
	if err != nil {
		return replacementCompletion{}, err
	}
	return replacementCompletion{
		OperationID: commit.OperationID(), Binding: commit.Binding(),
		Prior: commit.Prior(), Next: commit.Next(),
	}, nil
}

func (a serviceControllerAdapter) AcknowledgeDeploymentReplacement(ctx context.Context, commit replacementCompletion) error {
	return a.AcknowledgeReplacementCommit(ctx, commit.OperationID, commit.Binding, commit.Prior, commit.Next)
}

type controllerFactory func(context.Context, service.ControllerConfig) (deploymentController, error)

// Controller owns the only public signed-app publication workflow.
type Controller struct {
	client         *http.Client
	verifier       Verifier
	openController controllerFactory
	operationID    func() (string, error)
	exchange       namespaceOperation
	publishFirst   namespaceOperation
	failpoint      func(string) error
}

// New returns a Controller using the platform codesign verifier and the durable
// service controller rooted beside the app's deployment journal.
func New() *Controller {
	return &Controller{
		client: http.DefaultClient, verifier: newVerifier(),
		openController: func(ctx context.Context, cfg service.ControllerConfig) (deploymentController, error) {
			controller, err := service.NewController(ctx, cfg)
			if err != nil {
				return nil, err
			}
			return serviceControllerAdapter{Controller: controller}, nil
		},
		operationID:  newOperationID,
		exchange:     exchangePaths,
		publishFirst: publishExclusive,
	}
}

// Phase is one finite durable deployment checkpoint.
type Phase string

// Deployment phases are the complete v1 transaction checkpoint set.
const (
	PhasePrepared             Phase = "prepared"
	PhasePriorQuiesced        Phase = "prior_quiesced"
	PhaseNamespaceCandidate   Phase = "namespace_candidate"
	PhaseCandidateProved      Phase = "candidate_proved"
	PhaseTargetPlanned        Phase = "target_planned"
	PhaseCandidateActivated   Phase = "candidate_activated"
	PhaseCandidateReady       Phase = "candidate_ready"
	PhaseReceiptCommitted     Phase = "receipt_committed"
	PhaseServiceCommitPending Phase = "service_commit_pending"
	PhaseCleanupComplete      Phase = "cleanup_complete"
	PhaseRollbackQuiesced     Phase = "rollback_quiesced"
	PhasePriorRestored        Phase = "prior_restored"
	PhasePriorProved          Phase = "prior_proved"
	PhasePriorActivated       Phase = "prior_activated"
	PhasePriorReady           Phase = "prior_ready"
	PhaseRecoveryRequired     Phase = "recovery_required"
)

// Direction fixes the only recovery direction allowed for a transaction.
type Direction string

// Directions are the only legal durable transaction flows.
const (
	DirectionForward  Direction = "forward"
	DirectionRollback Direction = "rollback"
)

type operationMode string

const (
	modeReplace     operationMode = "replace"
	modeReconfigure operationMode = "reconfigure"
	modeDeactivate  operationMode = "deactivate"
)

type storedGeneration struct {
	Path                  string `json:"path"`
	Version               string `json:"version"`
	URL                   string `json:"url"`
	SHA256                string `json:"sha256"`
	DesignatedRequirement string `json:"designated_requirement"`
	CDHash                string `json:"cdhash"`
	BundleDigest          string `json:"bundle_digest"`
	FileID                fileID `json:"file_id"`
}

type storedPlan struct {
	Agents []service.Agent `json:"agents"`
	Digest string          `json:"digest"`
}

type storedRuntimeProof struct {
	OperationID       string    `json:"operation_id"`
	Role              ProofRole `json:"role"`
	BundleDevice      string    `json:"bundle_device"`
	BundleInode       string    `json:"bundle_inode"`
	BundleCDHash      string    `json:"bundle_cdhash"`
	BundleDigest      string    `json:"bundle_digest"`
	Absent            bool      `json:"absent"`
	ProcessGeneration string    `json:"process_generation"`
	Digest            string    `json:"digest"`
}

type storedActivation struct {
	OperationID string `json:"operation_id"`
	Binding     string `json:"binding"`
	Epoch       uint64 `json:"epoch"`
	PlanDigest  string `json:"plan_digest"`
}

type storedProof struct {
	OperationID  string    `json:"operation_id"`
	Role         ProofRole `json:"role"`
	BundleDevice string    `json:"bundle_device"`
	BundleInode  string    `json:"bundle_inode"`
	BundleCDHash string    `json:"bundle_cdhash"`
	BundleDigest string    `json:"bundle_digest"`
	PlanDigest   string    `json:"plan_digest"`
	Digest       string    `json:"digest"`
}

type deploymentTransaction struct {
	Identity                string              `json:"identity"`
	Schema                  int                 `json:"schema"`
	Fingerprint             string              `json:"fingerprint"`
	OperationID             string              `json:"operation_id"`
	ArtifactFingerprint     string              `json:"artifact_fingerprint"`
	ConsumerBuild           string              `json:"consumer_build"`
	PolicyDigest            string              `json:"policy_digest"`
	ReplacementBinding      string              `json:"replacement_binding"`
	Direction               Direction           `json:"direction"`
	Mode                    operationMode       `json:"mode"`
	Phase                   Phase               `json:"phase"`
	RollbackFrom            Phase               `json:"rollback_from"`
	Stage                   string              `json:"stage"`
	PriorReceipt            *storedReceipt      `json:"prior_receipt"`
	Candidate               storedGeneration    `json:"candidate"`
	PriorPlan               storedPlan          `json:"prior_plan"`
	NextPlan                *storedPlan         `json:"next_plan"`
	Activation              *storedActivation   `json:"activation"`
	PriorRuntimeProof       *storedRuntimeProof `json:"prior_runtime_proof"`
	RollbackRuntimeProof    *storedRuntimeProof `json:"rollback_runtime_proof"`
	PostProof               *storedProof        `json:"post_proof"`
	RestoreProof            *storedProof        `json:"restore_proof"`
	CandidateReadinessProof *storedProof        `json:"candidate_readiness_proof"`
	PriorReadinessProof     *storedProof        `json:"prior_readiness_proof"`
	Failure                 string              `json:"failure"`
}

type storedOperation struct {
	OperationID        string `json:"operation_id"`
	ConsumerBuild      string `json:"consumer_build"`
	PolicyDigest       string `json:"policy_digest"`
	ReplacementBinding string `json:"replacement_binding"`
}

type storedReceipt struct {
	Identity            string            `json:"identity"`
	Schema              int               `json:"schema"`
	Fingerprint         string            `json:"fingerprint"`
	ArtifactFingerprint string            `json:"artifact_fingerprint"`
	LastOperation       storedOperation   `json:"last_operation"`
	State               DeploymentState   `json:"state"`
	Current             *storedGeneration `json:"current"`
	PriorPlan           storedPlan        `json:"prior_plan"`
	Plan                storedPlan        `json:"plan"`
	ActivationPlan      storedPlan        `json:"activation_plan"`
	ActivationOperation storedOperation   `json:"activation_operation"`
	Failure             string            `json:"failure"`
}

type deploymentPaths struct {
	canonical      string
	metadataDir    string
	lock           string
	receipt        string
	transaction    string
	serviceState   string
	serviceProcess string
}

func deploymentPathsFor(cfg Config) deploymentPaths {
	return deploymentPathsForLocation(stateLocation{Dir: cfg.Dir, AppName: cfg.AppName})
}

func deploymentPathsForLocation(location stateLocation) deploymentPaths {
	metadata := filepath.Join(location.Dir, ".daemonkit-deployment", location.AppName)
	return deploymentPaths{
		canonical: bundle.AppPath(location.Dir, location.AppName), metadataDir: metadata,
		lock:    filepath.Join(metadata, "deployment.lock"),
		receipt: filepath.Join(metadata, "receipt.json"), transaction: filepath.Join(metadata, "transaction.json"),
		serviceState:   filepath.Join(metadata, "services.db"),
		serviceProcess: filepath.Join(metadata, "service-workers.db"),
	}
}

func newOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("deployment: generate deployment operation id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func validateConfigPublic(cfg Config) (string, error) {
	dr, err := validateArtifactConfig(artifactConfig{
		Release: cfg.Release, Dir: cfg.Dir, AppName: cfg.AppName, Identity: cfg.Identity,
	})
	if err != nil {
		return "", err
	}
	if cfg.RuntimeQuiesce == nil || cfg.PostInstallProof == nil || cfg.PriorAppRestoreProof == nil ||
		cfg.BuildPlan == nil || cfg.Readiness == nil {
		return "", fmt.Errorf("%w: every deployment callback is required", ErrInvalidConfig)
	}
	if cfg.ConsumerBuild == "" || strings.TrimSpace(cfg.ConsumerBuild) != cfg.ConsumerBuild || cfg.PolicyDigest == (SHA256{}) {
		return "", fmt.Errorf("%w: consumer build and policy digest are required", ErrInvalidConfig)
	}
	return dr, nil
}

func validateDeactivateConfig(cfg DeactivateConfig) (string, error) {
	if err := (stateLocation{Dir: cfg.Dir, AppName: cfg.AppName}).validateAllowMissing(); err != nil {
		return "", err
	}
	requirement, err := cfg.Identity.DRString()
	if err != nil {
		return "", fmt.Errorf("%w: designated requirement: %w", ErrInvalidConfig, err)
	}
	if cfg.RuntimeQuiesce == nil || cfg.Readiness == nil || cfg.ConsumerBuild == "" ||
		strings.TrimSpace(cfg.ConsumerBuild) != cfg.ConsumerBuild || cfg.PolicyDigest == (SHA256{}) {
		return "", fmt.Errorf("%w: deactivate callbacks, consumer build, and policy digest are required", ErrInvalidConfig)
	}
	return requirement, nil
}

func artifactFingerprint(cfg Config, dr string) (string, error) {
	payload, err := json.Marshal(struct {
		Identity    string `json:"identity"`
		Schema      int    `json:"schema"`
		Version     string `json:"version"`
		URL         string `json:"url"`
		SHA256      string `json:"sha256"`
		Dir         string `json:"dir"`
		AppName     string `json:"app_name"`
		Requirement string `json:"requirement"`
	}{
		"daemonkit.deployment.artifact.v1", 1, cfg.Release.Version, cfg.Release.URL,
		cfg.Release.SHA256.String(), cfg.Dir, cfg.AppName, dr,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func replacementBinding(cfg Config) service.ReplacementBinding {
	return callbackBinding(cfg.ConsumerBuild, cfg.PolicyDigest)
}

func callbackBinding(consumerBuild string, policyDigest SHA256) service.ReplacementBinding {
	payload := []byte("daemonkit.deployment.replacement-binding.v1\x00" + consumerBuild + "\x00" + policyDigest.String())
	sum := sha256.Sum256(payload)
	var binding service.ReplacementBinding
	copy(binding[:], sum[:])
	return binding
}

func canonicalGeneration(stored storedGeneration) CanonicalGeneration {
	digest, _ := ParseSHA256(stored.SHA256)
	bundleDigest, _ := ParseSHA256(stored.BundleDigest)
	return CanonicalGeneration{
		Path:                  stored.Path,
		Release:               Release{Version: stored.Version, URL: stored.URL, SHA256: digest},
		DesignatedRequirement: stored.DesignatedRequirement,
		CDHash:                stored.CDHash, BundleDigest: bundleDigest,
		Device: stored.FileID.Device, Inode: stored.FileID.Inode,
	}
}

func storePlan(plan service.Plan) storedPlan {
	return storedPlan{Agents: plan.Agents(), Digest: plan.Digest().String()}
}

func restorePlan(stored storedPlan) (service.Plan, error) {
	var digest service.PlanDigest
	decoded, err := hex.DecodeString(stored.Digest)
	if err != nil || len(decoded) != len(digest) {
		return service.Plan{}, fmt.Errorf("%w: service plan digest encoding", ErrInstallState)
	}
	copy(digest[:], decoded)
	plan, err := service.RestorePlan(stored.Agents, digest)
	if err != nil {
		return service.Plan{}, err
	}
	return plan, nil
}

func validateProof(proof Proof, role ProofRole, planDigest SHA256) error {
	if proof.Digest == (SHA256{}) || proof.Role != role || proof.PlanDigest != planDigest {
		return errors.New("deployment: proof role, plan digest, and evidence digest must match the operation")
	}
	return nil
}

func validateRuntimeProof(proof RuntimeProof, role ProofRole) error {
	if proof.Absent == (proof.ProcessGeneration != "") {
		return errors.New("deployment: runtime proof must identify exactly one of absence or a process generation")
	}
	return validateProof(Proof{Role: proof.Role, Digest: proof.Digest}, role, SHA256{})
}

func bindProof(id string, role ProofRole, generation storedGeneration, planDigest string, proof Proof) *storedProof {
	return &storedProof{
		OperationID: id, Role: role, BundleDevice: generation.FileID.Device,
		BundleInode: generation.FileID.Inode, BundleCDHash: generation.CDHash, BundleDigest: generation.BundleDigest,
		PlanDigest: planDigest, Digest: proof.Digest.String(),
	}
}

func bindRuntimeProof(id string, role ProofRole, generation storedGeneration, proof RuntimeProof) *storedRuntimeProof {
	return &storedRuntimeProof{
		OperationID: id, Role: role, BundleDevice: generation.FileID.Device,
		BundleInode: generation.FileID.Inode, BundleCDHash: generation.CDHash, BundleDigest: generation.BundleDigest,
		Absent:            proof.Absent,
		ProcessGeneration: proof.ProcessGeneration, Digest: proof.Digest.String(),
	}
}

func (r storedReceipt) public() (DeploymentReceipt, error) {
	plan, err := restorePlan(r.Plan)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	activationPlan, err := restorePlan(r.ActivationPlan)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	if r.Current == nil {
		return DeploymentReceipt{}, fmt.Errorf("%w: completed receipt has no current generation", ErrInstallState)
	}
	return completedDeploymentReceipt(
		r.LastOperation.OperationID,
		r.State,
		canonicalGeneration(*r.Current),
		plan,
		activationPlan,
	), nil
}

func samePlan(left, right service.Plan) bool {
	return left.Digest() == right.Digest() && reflect.DeepEqual(left.Agents(), right.Agents())
}

func syncDeploymentState(path string, value any) error {
	if err := writeJSONDurable(path, value); err != nil {
		return err
	}
	return daemon.SyncDir(filepath.Dir(path))
}
