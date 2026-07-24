package deployment

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

const (
	activationLockDeadline = 30 * time.Second
	serviceWorkerLimit     = 4
)

// ActivateInstalledConfig seals one already-installed signed app activation.
type ActivateInstalledConfig struct {
	OperationID        string
	AppPath            string
	Version            string
	Identity           codeidentity.CodeIdentity
	BundleDigest       SHA256
	EntitlementsDigest SHA256
	ConsumerBuild      string
	PolicyDigest       SHA256
	Plan               service.Plan
	Readiness          func(context.Context, InstalledOperation) (ReadinessProof, error)
}

// DeactivateInstalledConfig seals one activation removal operation.
type DeactivateInstalledConfig struct {
	OperationID    string
	AppPath        string
	Identity       codeidentity.CodeIdentity
	ConsumerBuild  string
	PolicyDigest   SHA256
	RuntimeQuiesce func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error)
}

// InstalledGeneration is one exact attested app generation.
type InstalledGeneration struct{ stored storedGeneration }

// Path returns the canonical full app path.
func (g InstalledGeneration) Path() string { return g.stored.Path }

// Version returns the exact bundle marketing version.
func (g InstalledGeneration) Version() string { return g.stored.Version }

// TeamID returns the exact signing team.
func (g InstalledGeneration) TeamID() string { return g.stored.TeamID }

// SigningIdentifier returns the exact signing identifier.
func (g InstalledGeneration) SigningIdentifier() string { return g.stored.SigningIdentifier }

// DesignatedRequirement returns the exact codesign requirement.
func (g InstalledGeneration) DesignatedRequirement() string { return g.stored.DesignatedRequirement }

// CDHash returns the exact code-directory hash.
func (g InstalledGeneration) CDHash() string { return g.stored.CDHash }

// BundleDigest returns the exact bundle-tree digest.
func (g InstalledGeneration) BundleDigest() SHA256 { return mustParseDigest(g.stored.BundleDigest) }

// EntitlementsDigest returns the full normalized entitlement dictionary digest.
func (g InstalledGeneration) EntitlementsDigest() SHA256 {
	return mustParseDigest(g.stored.EntitlementsDigest)
}

// InstalledOperation is the exact callback scope for readiness proof.
type InstalledOperation struct {
	operationID string
	generation  InstalledGeneration
	plan        service.Plan
}

// OperationID returns the caller-supplied stable operation ID.
func (o InstalledOperation) OperationID() string { return o.operationID }

// Generation returns the exact attested app generation.
func (o InstalledOperation) Generation() InstalledGeneration { return o.generation }

// Plan returns the immutable exact service plan.
func (o InstalledOperation) Plan() service.Plan { return o.plan }

// ReadinessProof binds product health evidence to an exact runtime generation.
type ReadinessProof struct {
	runtimeBuild      string
	processGeneration proc.OwnerGeneration
	resourceDigest    SHA256
}

// NewReadinessProof constructs exact product readiness evidence.
func NewReadinessProof(runtimeBuild string, generation proc.OwnerGeneration, resourceDigest SHA256) (ReadinessProof, error) {
	if strings.TrimSpace(runtimeBuild) == "" || runtimeBuild != strings.TrimSpace(runtimeBuild) ||
		generation == (proc.OwnerGeneration{}) || resourceDigest == (SHA256{}) {
		return ReadinessProof{}, fmt.Errorf("%w: readiness proof is incomplete", ErrInvalidConfig)
	}
	return ReadinessProof{runtimeBuild: runtimeBuild, processGeneration: generation, resourceDigest: resourceDigest}, nil
}

// RuntimeBuild returns the exact runtime build proved ready.
func (p ReadinessProof) RuntimeBuild() string { return p.runtimeBuild }

// ProcessGeneration returns the exact runtime process generation proved ready.
func (p ReadinessProof) ProcessGeneration() proc.OwnerGeneration { return p.processGeneration }

// ResourceDigest returns the product-defined digest of exact readiness evidence.
func (p ReadinessProof) ResourceDigest() SHA256 { return p.resourceDigest }

// RuntimeProof proves the prior runtime is absent before service removal.
type RuntimeProof struct {
	absent            bool
	processGeneration proc.OwnerGeneration
	digest            SHA256
}

// NewRuntimeProof constructs exact quiescence evidence.
func NewRuntimeProof(absent bool, generation proc.OwnerGeneration, digest SHA256) (RuntimeProof, error) {
	if !absent || digest == (SHA256{}) {
		return RuntimeProof{}, fmt.Errorf("%w: runtime proof must establish absence", ErrInvalidConfig)
	}
	return RuntimeProof{absent: true, processGeneration: generation, digest: digest}, nil
}

// Absent reports whether the exact prior runtime was proved gone.
func (p RuntimeProof) Absent() bool { return p.absent }

// ProcessGeneration returns the stopped generation, or zero when already absent.
func (p RuntimeProof) ProcessGeneration() proc.OwnerGeneration { return p.processGeneration }

// Digest returns the product-defined exact quiescence evidence digest.
func (p RuntimeProof) Digest() SHA256 { return p.digest }

// RuntimeStopper is a request-scoped capability for exact runtime shutdown.
type RuntimeStopper interface {
	StopRuntime(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error)
}

// DeactivateInstalledOperation binds quiescence to an exact activation and removal operation.
type DeactivateInstalledOperation struct {
	operationID string
	activation  ActivationReceipt
}

// OperationID returns the caller-supplied stable deactivation operation ID.
func (o DeactivateInstalledOperation) OperationID() string { return o.operationID }

// Activation returns the exact activation being removed.
func (o DeactivateInstalledOperation) Activation() ActivationReceipt { return o.activation }

// ActivationReceipt is one immutable schema-v1 activation receipt.
type ActivationReceipt struct {
	operationID string
	active      bool
	generation  InstalledGeneration
	plan        service.Plan
	readiness   ReadinessProof
}

// OperationID returns the stable activation operation ID.
func (r ActivationReceipt) OperationID() string { return r.operationID }

// Active reports whether readiness was durably proven.
func (r ActivationReceipt) Active() bool { return r.active }

// Generation returns the exact app generation.
func (r ActivationReceipt) Generation() InstalledGeneration { return r.generation }

// Plan returns the immutable exact service plan.
func (r ActivationReceipt) Plan() service.Plan { return r.plan }

// Readiness returns the exact active readiness evidence.
func (r ActivationReceipt) Readiness() (ReadinessProof, bool) { return r.readiness, r.active }

// DeactivationReceipt is one immutable exact removal result.
type DeactivationReceipt struct {
	operationID string
	runtime     RuntimeProof
}

// OperationID returns the stable removal operation ID.
func (r DeactivationReceipt) OperationID() string { return r.operationID }

// RuntimeProof returns the exact quiescence evidence.
func (r DeactivationReceipt) RuntimeProof() RuntimeProof { return r.runtime }

// InstalledState is the exact observed activation phase.
type InstalledState string

const (
	// InstalledVerifiedUnactivated means exact app bytes exist without activation ownership.
	InstalledVerifiedUnactivated InstalledState = "verified_unactivated"
	// InstalledPrepared means the exact durable receipt precedes completed readiness.
	InstalledPrepared InstalledState = "prepared"
	// InstalledActive means receipt, app, services, and readiness are exact.
	InstalledActive InstalledState = "active"
)

// InstalledStatus is a read-only exact installed-app observation.
type InstalledStatus struct {
	state   InstalledState
	receipt *ActivationReceipt
}

// State returns the exact observed state.
func (s InstalledStatus) State() InstalledState { return s.state }

// Receipt returns the activation receipt when one exists.
func (s InstalledStatus) Receipt() (ActivationReceipt, bool) {
	if s.receipt == nil {
		return ActivationReceipt{}, false
	}
	return *s.receipt, true
}

// NewOperationID returns a stable random operation ID for callers to persist before invocation.
func NewOperationID() (string, error) { return newOperationID() }

// ActivateInstalled activates only the exact already-installed app in config.
func (c *Controller) ActivateInstalled(ctx context.Context, config ActivateInstalledConfig) (ActivationReceipt, error) {
	validated, err := validateActivateConfig(config)
	if err != nil {
		return ActivationReceipt{}, err
	}
	if c == nil || c.verifier == nil || c.openService == nil {
		return ActivationReceipt{}, fmt.Errorf("%w: controller dependencies are required", ErrInvalidConfig)
	}
	paths := deploymentPathsForApp(config.AppPath)
	if err := ensureMetadataDir(paths); err != nil {
		return ActivationReceipt{}, err
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: activationLockDeadline}).Acquire(ctx)
	if err != nil {
		return ActivationReceipt{}, fmt.Errorf("deployment: acquire activation lock: %w", err)
	}
	defer lock.Close()
	return c.activateLocked(ctx, config, validated, paths)
}

type validatedActivation struct {
	requirement string
	fingerprint string
}

func validateActivateConfig(config ActivateInstalledConfig) (validatedActivation, error) {
	if !validOperationID(config.OperationID) {
		return validatedActivation{}, fmt.Errorf("%w: operation ID must be 32 hexadecimal characters", ErrInvalidConfig)
	}
	if err := validateCanonicalAppPath(config.AppPath); err != nil {
		return validatedActivation{}, err
	}
	if strings.TrimSpace(config.Version) == "" || config.Version != strings.TrimSpace(config.Version) ||
		strings.TrimSpace(config.ConsumerBuild) == "" || config.ConsumerBuild != strings.TrimSpace(config.ConsumerBuild) {
		return validatedActivation{}, fmt.Errorf("%w: version and consumer build are required exact strings", ErrInvalidConfig)
	}
	if err := config.BundleDigest.validate("bundle digest"); err != nil {
		return validatedActivation{}, err
	}
	if err := config.EntitlementsDigest.validate("entitlements digest"); err != nil {
		return validatedActivation{}, err
	}
	if err := config.PolicyDigest.validate("policy digest"); err != nil {
		return validatedActivation{}, err
	}
	if config.Plan.Digest() == (service.PlanDigest{}) || config.Readiness == nil {
		return validatedActivation{}, fmt.Errorf("%w: exact plan and readiness callback are required", ErrInvalidConfig)
	}
	if err := validatePlanPrograms(config.AppPath, config.Plan); err != nil {
		return validatedActivation{}, err
	}
	requirement, err := config.Identity.DRString()
	if err != nil {
		return validatedActivation{}, fmt.Errorf("%w: designated requirement: %w", ErrInvalidConfig, err)
	}
	fingerprint, err := activationConfigFingerprint(config, requirement)
	if err != nil {
		return validatedActivation{}, err
	}
	return validatedActivation{requirement: requirement, fingerprint: fingerprint}, nil
}

func validatePlanPrograms(appPath string, plan service.Plan) error {
	for _, agent := range plan.Agents() {
		relative, err := filepath.Rel(appPath, agent.Program)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%w: service program %q is outside canonical app", ErrInvalidConfig, agent.Program)
		}
		resolved, err := filepath.EvalSymlinks(agent.Program)
		if err != nil || resolved != agent.Program {
			return fmt.Errorf("%w: service program %q is not a canonical real path", ErrInstallConflict, agent.Program)
		}
	}
	return nil
}

func activationConfigFingerprint(config ActivateInstalledConfig, requirement string) (string, error) {
	wire := struct {
		AppPath, Version, TeamID, SigningIdentifier, Requirement                  string
		BundleDigest, EntitlementsDigest, ConsumerBuild, PolicyDigest, PlanDigest string
	}{
		AppPath: config.AppPath, Version: config.Version, TeamID: config.Identity.TeamID,
		SigningIdentifier: config.Identity.SigningIdentifier, Requirement: requirement,
		BundleDigest: config.BundleDigest.String(), EntitlementsDigest: config.EntitlementsDigest.String(),
		ConsumerBuild: config.ConsumerBuild, PolicyDigest: config.PolicyDigest.String(),
		PlanDigest: config.Plan.Digest().String(),
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

func (c *Controller) activateLocked(
	ctx context.Context,
	config ActivateInstalledConfig,
	validated validatedActivation,
	paths deploymentPaths,
) (result ActivationReceipt, returnErr error) {
	generation, err := attestInstalled(ctx, c.verifier, config.AppPath, config.Version, config.Identity, config.BundleDigest, config.EntitlementsDigest)
	if err != nil {
		return ActivationReceipt{}, err
	}
	receipt, err := readActivation(paths.activation)
	createdReceipt := false
	if errors.Is(err, os.ErrNotExist) {
		if fileExists(paths.serviceState) || fileExists(paths.serviceProcess) {
			return ActivationReceipt{}, fmt.Errorf("%w: service state exists without activation receipt", ErrInstallConflict)
		}
		receipt = &activationReceiptWire{
			Identity: activationIdentity, Schema: activationSchema, OperationID: config.OperationID,
			ConfigFingerprint: validated.fingerprint, ConsumerBuild: config.ConsumerBuild,
			PolicyDigest: config.PolicyDigest.String(), Phase: activationPrepared,
			Generation: generation, Plan: storePlan(config.Plan),
		}
		if err := writeJSONDurable(paths.activation, receipt); err != nil {
			return ActivationReceipt{}, err
		}
		createdReceipt = true
		if err := c.inject("activate:prepared"); err != nil {
			return ActivationReceipt{}, err
		}
	} else if err != nil {
		return ActivationReceipt{}, err
	}
	if receipt.OperationID != config.OperationID || receipt.ConfigFingerprint != validated.fingerprint ||
		receipt.ConsumerBuild != config.ConsumerBuild || receipt.PolicyDigest != config.PolicyDigest.String() ||
		!reflect.DeepEqual(receipt.Generation, generation) || receipt.Plan.Digest != config.Plan.Digest().String() {
		return ActivationReceipt{}, fmt.Errorf("%w: activation receipt differs from request", ErrInstallConflict)
	}
	plan, err := restorePlan(receipt.Plan)
	if err != nil {
		return ActivationReceipt{}, err
	}
	servicesExisted := fileExists(paths.serviceState) || fileExists(paths.serviceProcess)
	controller, err := c.openService(ctx, service.ControllerConfig{
		StatePath: paths.serviceState, ProcessPath: paths.serviceProcess, WorkerLimit: serviceWorkerLimit,
	})
	if err != nil {
		return ActivationReceipt{}, c.rollbackActivation(ctx, paths, nil, createdReceipt, servicesExisted, err)
	}
	closed := false
	defer func() {
		if !closed {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			returnErr = errors.Join(returnErr, controller.Close(closeCtx))
		}
	}()
	if err := controller.Converge(ctx, plan.Agents()); err != nil {
		return ActivationReceipt{}, c.rollbackActivation(ctx, paths, controller, createdReceipt, servicesExisted, err)
	}
	if err := c.inject("activate:converged"); err != nil {
		return ActivationReceipt{}, err
	}
	operation := InstalledOperation{operationID: receipt.OperationID, generation: InstalledGeneration{stored: generation}, plan: plan}
	proof, err := config.Readiness(ctx, operation)
	if err != nil {
		return ActivationReceipt{}, c.rollbackActivation(ctx, paths, controller, createdReceipt, servicesExisted, err)
	}
	if proof.runtimeBuild == "" || proof.processGeneration == (proc.OwnerGeneration{}) || proof.resourceDigest == (SHA256{}) {
		return ActivationReceipt{}, c.rollbackActivation(ctx, paths, controller, createdReceipt, servicesExisted,
			fmt.Errorf("%w: readiness callback returned incomplete proof", ErrInvalidConfig))
	}
	storedProof := &storedReadinessProof{
		RuntimeBuild: proof.runtimeBuild, ProcessGeneration: proof.processGeneration,
		ResourceDigest: proof.resourceDigest.String(),
	}
	if receipt.Phase == activationActive && !reflect.DeepEqual(receipt.Readiness, storedProof) {
		return ActivationReceipt{}, fmt.Errorf("%w: readiness proof differs from active receipt", ErrInstallConflict)
	}
	if err := c.inject("activate:healthy"); err != nil {
		return ActivationReceipt{}, err
	}
	verified, err := attestInstalled(ctx, c.verifier, config.AppPath, config.Version, config.Identity, config.BundleDigest, config.EntitlementsDigest)
	if err != nil || !reflect.DeepEqual(verified, generation) {
		if err == nil {
			err = fmt.Errorf("%w: app changed during activation", ErrInstallConflict)
		}
		return ActivationReceipt{}, c.rollbackActivation(ctx, paths, controller, createdReceipt, servicesExisted, err)
	}
	receipt.Phase = activationActive
	receipt.Readiness = storedProof
	if err := writeJSONDurable(paths.activation, receipt); err != nil {
		return ActivationReceipt{}, err
	}
	if err := c.inject("activate:active"); err != nil {
		return ActivationReceipt{}, err
	}
	if err := controller.Close(ctx); err != nil {
		return ActivationReceipt{}, err
	}
	closed = true
	return receipt.public()
}

func (c *Controller) rollbackActivation(
	ctx context.Context,
	paths deploymentPaths,
	controller serviceController,
	createdReceipt, servicesExisted bool,
	cause error,
) error {
	if !createdReceipt {
		return cause
	}
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var rollbackErr error
	if controller != nil {
		rollbackErr = errors.Join(rollbackErr, controller.Converge(rollbackCtx, nil), controller.Close(rollbackCtx))
	}
	if !servicesExisted {
		rollbackErr = errors.Join(rollbackErr, removeIfExistsDurable(paths.serviceState), removeIfExistsDurable(paths.serviceProcess))
	}
	rollbackErr = errors.Join(rollbackErr, removeIfExistsDurable(paths.activation))
	return errors.Join(cause, rollbackErr)
}

// StatusInstalled verifies the exact app and observes its durable activation.
func (c *Controller) StatusInstalled(ctx context.Context, config ActivateInstalledConfig) (InstalledStatus, error) {
	validated, err := validateActivateConfig(config)
	if err != nil {
		return InstalledStatus{}, err
	}
	if c == nil || c.verifier == nil || c.openService == nil {
		return InstalledStatus{}, ErrInvalidConfig
	}
	paths := deploymentPathsForApp(config.AppPath)
	generation, err := attestInstalled(ctx, c.verifier, config.AppPath, config.Version, config.Identity, config.BundleDigest, config.EntitlementsDigest)
	if err != nil {
		return InstalledStatus{}, err
	}
	receipt, err := readActivation(paths.activation)
	if errors.Is(err, os.ErrNotExist) {
		if fileExists(paths.serviceState) || fileExists(paths.serviceProcess) {
			return InstalledStatus{}, fmt.Errorf("%w: service state exists without activation receipt", ErrInstallConflict)
		}
		return InstalledStatus{state: InstalledVerifiedUnactivated}, nil
	}
	if err != nil {
		return InstalledStatus{}, err
	}
	if receipt.OperationID != config.OperationID || receipt.ConfigFingerprint != validated.fingerprint ||
		!reflect.DeepEqual(receipt.Generation, generation) {
		return InstalledStatus{}, fmt.Errorf("%w: activation receipt differs from request", ErrInstallConflict)
	}
	public, err := receipt.public()
	if err != nil {
		return InstalledStatus{}, err
	}
	state := InstalledPrepared
	if receipt.Phase == activationActive {
		state = InstalledActive
	}
	return InstalledStatus{state: state, receipt: &public}, nil
}

// DeactivateInstalled removes exact service ownership while retaining the packaged app.
func (c *Controller) DeactivateInstalled(ctx context.Context, config DeactivateInstalledConfig) (DeactivationReceipt, error) {
	if err := validateDeactivateConfig(config); err != nil {
		return DeactivationReceipt{}, err
	}
	if c == nil || c.openService == nil {
		return DeactivationReceipt{}, ErrInvalidConfig
	}
	paths := deploymentPathsForApp(config.AppPath)
	if err := requireRealDirectory(paths.metadataDir); err != nil {
		return DeactivationReceipt{}, fmt.Errorf("%w: activation receipt is required", ErrInstallConflict)
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: activationLockDeadline}).Acquire(ctx)
	if err != nil {
		return DeactivationReceipt{}, err
	}
	defer lock.Close()
	return c.deactivateLocked(ctx, config, paths)
}

func validateDeactivateConfig(config DeactivateInstalledConfig) error {
	if !validOperationID(config.OperationID) || strings.TrimSpace(config.ConsumerBuild) == "" ||
		config.ConsumerBuild != strings.TrimSpace(config.ConsumerBuild) || config.RuntimeQuiesce == nil {
		return fmt.Errorf("%w: operation, consumer build, and runtime quiesce are required", ErrInvalidConfig)
	}
	if err := validateCanonicalAppPath(config.AppPath); err != nil {
		return err
	}
	if err := config.Identity.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return config.PolicyDigest.validate("policy digest")
}

func (c *Controller) deactivateLocked(
	ctx context.Context,
	config DeactivateInstalledConfig,
	paths deploymentPaths,
) (DeactivationReceipt, error) {
	activation, err := readActivation(paths.activation)
	if errors.Is(err, os.ErrNotExist) {
		tombstone, tombstoneErr := readDeactivation(paths.deactivation)
		if tombstoneErr != nil {
			return DeactivationReceipt{}, fmt.Errorf("%w: activation receipt is required", ErrInstallConflict)
		}
		if tombstone.OperationID != config.OperationID || tombstone.ConsumerBuild != config.ConsumerBuild ||
			tombstone.PolicyDigest != config.PolicyDigest.String() || tombstone.Phase != deactivationInactive {
			return DeactivationReceipt{}, fmt.Errorf("%w: deactivation replay differs from receipt", ErrInstallConflict)
		}
		return tombstone.public()
	}
	if err != nil {
		return DeactivationReceipt{}, err
	}
	if activation.ConsumerBuild != config.ConsumerBuild || activation.PolicyDigest != config.PolicyDigest.String() ||
		activation.Generation.TeamID != config.Identity.TeamID ||
		activation.Generation.SigningIdentifier != config.Identity.SigningIdentifier ||
		activation.Generation.Path != config.AppPath {
		return DeactivationReceipt{}, fmt.Errorf("%w: deactivation config differs from activation", ErrInstallConflict)
	}
	if err := verifyGenerationIdentity(config.AppPath, activation.Generation.FileID); err != nil {
		return DeactivationReceipt{}, fmt.Errorf("%w: canonical app generation changed: %v", ErrInstallConflict, err)
	}
	tombstone, err := readDeactivation(paths.deactivation)
	if errors.Is(err, os.ErrNotExist) {
		tombstone = nil
	} else if err != nil {
		return DeactivationReceipt{}, err
	} else if tombstone.Phase == deactivationInactive {
		if tombstone.OperationID == config.OperationID && tombstone.ConsumerBuild == config.ConsumerBuild &&
			tombstone.PolicyDigest == config.PolicyDigest.String() &&
			tombstone.ActivationFingerprint == activation.ConfigFingerprint {
			if err := removeIfExistsDurable(paths.activation); err != nil {
				return DeactivationReceipt{}, err
			}
			return tombstone.public()
		}
		tombstone = nil
	}
	if tombstone == nil {
		tombstone = &deactivationReceiptWire{
			Identity: deactivationIdentity, Schema: activationSchema, OperationID: config.OperationID,
			ConsumerBuild: config.ConsumerBuild, PolicyDigest: config.PolicyDigest.String(),
			ActivationFingerprint: activation.ConfigFingerprint, Phase: deactivationPrepared,
		}
		if err := writeJSONDurable(paths.deactivation, tombstone); err != nil {
			return DeactivationReceipt{}, err
		}
		if err := c.inject("deactivate:prepared"); err != nil {
			return DeactivationReceipt{}, err
		}
	}
	if tombstone.OperationID != config.OperationID || tombstone.ConsumerBuild != config.ConsumerBuild ||
		tombstone.PolicyDigest != config.PolicyDigest.String() ||
		tombstone.ActivationFingerprint != activation.ConfigFingerprint {
		return DeactivationReceipt{}, fmt.Errorf("%w: deactivation receipt differs from request", ErrInstallConflict)
	}
	plan, err := restorePlan(activation.Plan)
	if err != nil {
		return DeactivationReceipt{}, err
	}
	controller, err := c.openService(ctx, service.ControllerConfig{
		StatePath: paths.serviceState, ProcessPath: paths.serviceProcess, WorkerLimit: serviceWorkerLimit,
	})
	if err != nil {
		return DeactivationReceipt{}, err
	}
	closed := false
	defer func() {
		if !closed {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			_ = controller.Close(closeCtx)
		}
	}()
	runtimeBuild := ""
	if activation.Readiness != nil {
		runtimeBuild = activation.Readiness.RuntimeBuild
	}
	stopper := &runtimeStopAccess{
		controller: controller, active: true, operationID: config.OperationID, runtimeBuild: runtimeBuild,
	}
	proof, err := config.RuntimeQuiesce(ctx, stopper, DeactivateInstalledOperation{
		operationID: config.OperationID, activation: mustPublicActivation(*activation),
	})
	stopper.revoke()
	if err != nil {
		return DeactivationReceipt{}, err
	}
	if !proof.absent || proof.digest == (SHA256{}) {
		return DeactivationReceipt{}, fmt.Errorf("%w: runtime quiesce did not prove absence", ErrInvalidConfig)
	}
	if stopper.stopped != (proof.processGeneration != (proc.OwnerGeneration{})) ||
		(stopper.stopped && stopper.target != proof.processGeneration) {
		return DeactivationReceipt{}, fmt.Errorf("%w: runtime proof differs from stop receipt", ErrInstallConflict)
	}
	if err := c.inject("deactivate:quiesced"); err != nil {
		return DeactivationReceipt{}, err
	}
	if err := controller.Converge(ctx, nil); err != nil {
		return DeactivationReceipt{}, err
	}
	for _, agent := range plan.Agents() {
		status, err := controller.Status(ctx, agent.Label)
		if err != nil {
			return DeactivationReceipt{}, err
		}
		if status.Desired || status.Applied || status.Loaded || status.Exact {
			return DeactivationReceipt{}, fmt.Errorf("%w: service %q remains active", ErrInstallState, agent.Label)
		}
	}
	if err := c.inject("deactivate:services_absent"); err != nil {
		return DeactivationReceipt{}, err
	}
	if err := controller.Close(ctx); err != nil {
		return DeactivationReceipt{}, err
	}
	closed = true
	if err := errors.Join(removeIfExistsDurable(paths.serviceState), removeIfExistsDurable(paths.serviceProcess)); err != nil {
		return DeactivationReceipt{}, err
	}
	tombstone.Phase = deactivationInactive
	var stopped *proc.OwnerGeneration
	if proof.processGeneration != (proc.OwnerGeneration{}) {
		generation := proof.processGeneration
		stopped = &generation
	}
	tombstone.RuntimeProof = &storedRuntimeProof{
		Absent: true, ProcessGeneration: stopped, Digest: proof.digest.String(),
	}
	if err := writeJSONDurable(paths.deactivation, tombstone); err != nil {
		return DeactivationReceipt{}, err
	}
	if err := c.inject("deactivate:inactive"); err != nil {
		return DeactivationReceipt{}, err
	}
	if err := removeIfExistsDurable(paths.activation); err != nil {
		return DeactivationReceipt{}, err
	}
	if err := c.inject("deactivate:receipt_removed"); err != nil {
		return DeactivationReceipt{}, err
	}
	return tombstone.public()
}

type runtimeStopAccess struct {
	mu           sync.Mutex
	controller   serviceController
	active       bool
	operationID  string
	runtimeBuild string
	stopped      bool
	target       proc.OwnerGeneration
}

func (s *runtimeStopAccess) StopRuntime(ctx context.Context, request service.StopRuntimeRequest) (service.StopReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return service.StopReceipt{}, errors.New("deployment: runtime stopper capability expired")
	}
	if s.runtimeBuild == "" || request.OperationID != s.operationID || request.ExpectedRuntimeBuild != s.runtimeBuild {
		return service.StopReceipt{}, errors.New("deployment: runtime stop request differs from deactivation scope")
	}
	receipt, err := s.controller.StopRuntime(ctx, request)
	if err != nil {
		return service.StopReceipt{}, err
	}
	if s.stopped && s.target != receipt.Target().ProcessGeneration {
		return service.StopReceipt{}, fmt.Errorf("%w: runtime stop receipt changed", ErrInstallConflict)
	}
	s.stopped = true
	s.target = receipt.Target().ProcessGeneration
	return receipt, nil
}

func (s *runtimeStopAccess) revoke() {
	s.mu.Lock()
	s.active = false
	s.mu.Unlock()
}

func (c *Controller) inject(point string) error {
	if c.failpoint == nil {
		return nil
	}
	return c.failpoint(point)
}

func (receipt activationReceiptWire) public() (ActivationReceipt, error) {
	plan, err := restorePlan(receipt.Plan)
	if err != nil {
		return ActivationReceipt{}, err
	}
	result := ActivationReceipt{
		operationID: receipt.OperationID, active: receipt.Phase == activationActive,
		generation: InstalledGeneration{stored: receipt.Generation}, plan: plan,
	}
	if receipt.Readiness != nil {
		result.readiness = ReadinessProof{
			runtimeBuild:      receipt.Readiness.RuntimeBuild,
			processGeneration: receipt.Readiness.ProcessGeneration,
			resourceDigest:    mustParseDigest(receipt.Readiness.ResourceDigest),
		}
	}
	return result, nil
}

func mustPublicActivation(receipt activationReceiptWire) ActivationReceipt {
	public, err := receipt.public()
	if err != nil {
		panic(err)
	}
	return public
}

func (receipt deactivationReceiptWire) public() (DeactivationReceipt, error) {
	if err := receipt.validate(); err != nil {
		return DeactivationReceipt{}, err
	}
	var generation proc.OwnerGeneration
	if receipt.RuntimeProof.ProcessGeneration != nil {
		generation = *receipt.RuntimeProof.ProcessGeneration
	}
	return DeactivationReceipt{
		operationID: receipt.OperationID,
		runtime: RuntimeProof{
			absent:            receipt.RuntimeProof.Absent,
			processGeneration: generation,
			digest:            mustParseDigest(receipt.RuntimeProof.Digest),
		},
	}, nil
}

func mustParseDigest(value string) SHA256 {
	digest, err := ParseSHA256(value)
	if err != nil {
		panic(err)
	}
	return digest
}

func fileExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func removeIfExistsDurable(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return daemon.SyncDir(filepath.Dir(path))
}
