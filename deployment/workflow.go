package deployment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

const (
	recoveryBound = 2 * time.Minute
	lockDeadline  = 30 * time.Second
)

// Deploy first recovers any exact prior transaction under the per-app lock,
// then publishes cfg.Release through the fenced deployment state machine.
func (c *Controller) Deploy(ctx context.Context, cfg Config) (DeploymentReceipt, error) {
	requirement, err := validateConfigPublic(cfg)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	return c.withDeploymentLock(ctx, cfg, func(
		opCtx context.Context,
		paths deploymentPaths,
		services serviceAccess,
	) (DeploymentReceipt, error) {
		recovered, active, err := c.recoverLocked(opCtx, cfg, requirement, paths, services)
		if err != nil {
			return recovered, err
		}
		if active {
			return recovered, nil
		}
		fingerprint, err := artifactFingerprint(cfg, requirement)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		current, err := readDeploymentReceipt(paths.receipt)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return DeploymentReceipt{}, err
		}
		if current != nil && current.ArtifactFingerprint == fingerprint && current.State == DeploymentActive &&
			current.LastOperation.ConsumerBuild == cfg.ConsumerBuild &&
			current.LastOperation.PolicyDigest == cfg.PolicyDigest.String() {
			if err := c.verifyComplete(opCtx, cfg, paths, services, *current); err != nil {
				return DeploymentReceipt{}, err
			}
			if _, err := c.generationProof(
				opCtx, cfg.PostInstallProof, current.LastOperation.OperationID, *current.Current, ProofPostInstall,
			); err != nil {
				return DeploymentReceipt{}, err
			}
			plan, err := restorePlan(current.Plan)
			if err != nil {
				return DeploymentReceipt{}, err
			}
			if _, err := c.readinessProof(
				opCtx, cfg, current.LastOperation.OperationID, *current.Current, plan, ProofCandidateReady,
			); err != nil {
				return DeploymentReceipt{}, err
			}
			return current.public()
		}
		if current != nil && current.ArtifactFingerprint == fingerprint && current.Current != nil {
			return c.beginReconfigure(opCtx, cfg, fingerprint, paths, services, *current)
		}
		return c.beginLocked(opCtx, cfg, requirement, fingerprint, paths, services, current)
	})
}

func (c *Controller) beginReconfigure(
	ctx context.Context,
	cfg Config,
	fingerprint string,
	paths deploymentPaths,
	services serviceAccess,
	prior storedReceipt,
) (DeploymentReceipt, error) {
	if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	if err := c.verifyStoredReceipt(ctx, paths, prior); err != nil {
		return DeploymentReceipt{}, err
	}
	currentPlan, err := services.snapshot(ctx)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	receiptPlan, err := restorePlan(prior.Plan)
	if err != nil || !samePlan(currentPlan, receiptPlan) {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired,
			errors.New("deployment: service plan differs from completed receipt"), err)
	}
	opID, err := c.operationID()
	if err != nil {
		return DeploymentReceipt{}, err
	}
	binding := replacementBinding(cfg)
	tx := &deploymentTransaction{
		Identity: deploymentIdentity, Schema: deploymentSchema, Fingerprint: deploymentFingerprint,
		OperationID: opID, ArtifactFingerprint: fingerprint, ConsumerBuild: cfg.ConsumerBuild,
		PolicyDigest: cfg.PolicyDigest.String(), ReplacementBinding: binding.String(),
		Direction: DirectionForward, Mode: modeReconfigure, Phase: PhasePrepared,
		PriorReceipt: &prior, Candidate: *prior.Current, PriorPlan: storePlan(currentPlan),
	}
	if prior.State == DeploymentInactive && prior.ActivationOperation.ConsumerBuild == cfg.ConsumerBuild &&
		prior.ActivationOperation.PolicyDigest == cfg.PolicyDigest.String() {
		replay := prior.ActivationPlan
		tx.NextPlan = &replay
	}
	if err := c.checkpoint(paths, tx); err != nil {
		return DeploymentReceipt{}, err
	}
	return c.forwardOrRollback(ctx, cfg, paths, services, tx)
}

// Recover is the sole repair driver. It never starts a new deployment.
func (c *Controller) Recover(ctx context.Context, cfg Config) (RecoveryResult, error) {
	requirement, err := validateConfigPublic(cfg)
	if err != nil {
		return RecoveryResult{}, err
	}
	receipt, err := c.withDeploymentLock(ctx, cfg, func(
		opCtx context.Context,
		paths deploymentPaths,
		services serviceAccess,
	) (DeploymentReceipt, error) {
		receipt, _, err := c.recoverLocked(opCtx, cfg, requirement, paths, services)
		return receipt, err
	})
	if receipt.state == "" && receipt.current == nil {
		return absentRecoveryResult(), err
	}
	if receipt.state == DeploymentRecoveryRequired {
		return requiredRecoveryResult(receipt), err
	}
	return completedRecoveryResult(receipt), err
}

// Deactivate durably retires the exact receipted service plan and runtime while
// retaining the verified signed app for a later Deploy reactivation. A
// never-installed app returns DeactivationAbsent without creating state.
func (c *Controller) Deactivate(ctx context.Context, cfg DeactivateConfig) (DeactivationResult, error) {
	requirement, err := validateDeactivateConfig(cfg)
	if err != nil {
		return DeactivationResult{}, err
	}
	if c == nil || c.client == nil || c.verifier == nil || c.openController == nil || c.operationID == nil {
		return DeactivationResult{}, fmt.Errorf("%w: controller dependencies are required", ErrInvalidConfig)
	}
	paths := deploymentPathsForLocation(stateLocation{Dir: cfg.Dir, AppName: cfg.AppName})
	metadata, metadataErr := os.Lstat(paths.metadataDir)
	if errors.Is(metadataErr, os.ErrNotExist) {
		if _, canonicalErr := os.Lstat(paths.canonical); errors.Is(canonicalErr, os.ErrNotExist) {
			return absentDeactivationResult(), nil
		} else if canonicalErr != nil {
			return DeactivationResult{}, canonicalErr
		}
		return DeactivationResult{}, fmt.Errorf("%w: canonical app has no deployment state", ErrInstallConflict)
	}
	if metadataErr != nil {
		return DeactivationResult{}, metadataErr
	}
	if !metadata.IsDir() || metadata.Mode()&os.ModeSymlink != 0 {
		return DeactivationResult{}, fmt.Errorf("%w: deployment state root is not a real directory", ErrInstallConflict)
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: lockDeadline}).AcquireExisting(ctx)
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("%w: existing deployment state has no usable lock: %w", ErrInstallState, err)
	}
	defer lock.Close()
	services := serviceAccess{open: c.openController, config: service.ControllerConfig{
		StatePath: paths.serviceState, ProcessPath: paths.serviceProcess, WorkerLimit: serviceWorkerLimit,
	}}
	result, err := func(opCtx context.Context) (DeploymentReceipt, error) {
		tx, txErr := readDeploymentTransaction(paths.transaction)
		if txErr == nil {
			if tx.Mode != modeDeactivate || tx.ConsumerBuild != cfg.ConsumerBuild ||
				tx.PolicyDigest != cfg.PolicyDigest.String() ||
				tx.ReplacementBinding != callbackBinding(cfg.ConsumerBuild, cfg.PolicyDigest).String() {
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired,
					errors.New("deployment: deactivate config does not match active transaction"))
			}
			internal := configForDeactivate(cfg, tx.Candidate)
			receipt, _, err := c.recoverLocked(opCtx, internal, requirement, paths, services)
			return receipt, err
		}
		if !errors.Is(txErr, os.ErrNotExist) {
			return DeploymentReceipt{}, txErr
		}
		receipt, err := readDeploymentReceipt(paths.receipt)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if err := c.verifyStoredReceipt(opCtx, paths, *receipt); err != nil {
			return DeploymentReceipt{}, err
		}
		if receipt.Current == nil || receipt.Current.DesignatedRequirement != requirement {
			return DeploymentReceipt{}, fmt.Errorf("%w: deactivate identity differs from installed receipt", ErrInvalidConfig)
		}
		if receipt.LastOperation.PolicyDigest != cfg.PolicyDigest.String() {
			return DeploymentReceipt{}, fmt.Errorf("%w: deactivate policy differs from completed receipt", ErrInvalidConfig)
		}
		if err := acknowledgeReceiptCompletion(opCtx, services, *receipt); err != nil {
			return DeploymentReceipt{}, err
		}
		if receipt.State == DeploymentInactive {
			if err := verifyInactiveServices(opCtx, services, *receipt); err != nil {
				return DeploymentReceipt{}, err
			}
			return receipt.public()
		}
		if receipt.State != DeploymentActive {
			return DeploymentReceipt{}, fmt.Errorf("%w: deployment is not active", ErrInstallState)
		}
		return c.beginDeactivate(opCtx, cfg, requirement, paths, services, *receipt)
	}(ctx)
	if err != nil {
		return DeactivationResult{}, err
	}
	if result.state != DeploymentInactive {
		return DeactivationResult{}, fmt.Errorf("%w: deactivate did not reach inactive state", ErrRecoveryRequired)
	}
	return inactiveDeactivationResult(result), nil
}

func verifyInactiveServices(ctx context.Context, services serviceAccess, receipt storedReceipt) error {
	plan, err := restorePlan(receipt.Plan)
	if err != nil {
		return err
	}
	empty, _ := service.NewPlan(nil)
	if !samePlan(plan, empty) {
		return fmt.Errorf("%w: inactive receipt plan is not empty", ErrInstallState)
	}
	completion, err := services.completion(ctx)
	if err != nil {
		return err
	}
	if completion != nil {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: inactive receipt retains a service completion"))
	}
	status, err := services.replacementStatus(ctx)
	if err != nil {
		return err
	}
	if status != nil {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: inactive receipt retains a service fence"))
	}
	current, err := services.snapshot(ctx)
	if err != nil {
		return err
	}
	if !samePlan(current, empty) {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: inactive services are not empty"))
	}
	return nil
}

func configForDeactivate(cfg DeactivateConfig, generation storedGeneration) Config {
	digest, _ := ParseSHA256(generation.SHA256)
	return Config{
		Dir: cfg.Dir, AppName: cfg.AppName,
		Release:  Release{Version: generation.Version, URL: generation.URL, SHA256: digest},
		Identity: cfg.Identity, ConsumerBuild: cfg.ConsumerBuild, PolicyDigest: cfg.PolicyDigest,
		RuntimeQuiesce: cfg.RuntimeQuiesce, Readiness: cfg.Readiness,
	}
}

func (c *Controller) beginDeactivate(
	ctx context.Context,
	cfg DeactivateConfig,
	requirement string,
	paths deploymentPaths,
	services serviceAccess,
	prior storedReceipt,
) (DeploymentReceipt, error) {
	currentPlan, err := services.snapshot(ctx)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	receiptPlan, err := restorePlan(prior.Plan)
	if err != nil || !samePlan(currentPlan, receiptPlan) {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired,
			errors.New("deployment: active service plan differs from receipt"), err)
	}
	opID, err := c.operationID()
	if err != nil {
		return DeploymentReceipt{}, err
	}
	binding := callbackBinding(cfg.ConsumerBuild, cfg.PolicyDigest)
	tx := &deploymentTransaction{
		Identity: deploymentIdentity, Schema: deploymentSchema, Fingerprint: deploymentFingerprint,
		OperationID: opID, ArtifactFingerprint: prior.ArtifactFingerprint, ConsumerBuild: cfg.ConsumerBuild,
		PolicyDigest: cfg.PolicyDigest.String(), ReplacementBinding: binding.String(),
		Direction: DirectionForward, Mode: modeDeactivate, Phase: PhasePrepared,
		PriorReceipt: &prior, Candidate: *prior.Current, PriorPlan: storePlan(currentPlan),
	}
	if err := c.checkpoint(paths, tx); err != nil {
		return DeploymentReceipt{}, err
	}
	internal := configForDeactivate(cfg, tx.Candidate)
	if tx.Candidate.DesignatedRequirement != requirement {
		return c.markRecoveryRequired(paths, tx, errors.New("deployment: deactivate designated requirement changed"))
	}
	return c.forwardOrRollback(ctx, internal, paths, services, tx)
}

// Status observes durable and namespace state without opening a service
// controller or advancing recovery.
func (c *Controller) Status(ctx context.Context, cfg Config) (DeploymentStatus, error) {
	if err := ctx.Err(); err != nil {
		return DeploymentStatus{}, err
	}
	if c == nil || c.client == nil || c.verifier == nil {
		return DeploymentStatus{}, fmt.Errorf("%w: controller dependencies are required", ErrInvalidConfig)
	}
	requirement, err := validateArtifactConfig(artifactConfig{
		Release: cfg.Release, Dir: cfg.Dir, AppName: cfg.AppName, Identity: cfg.Identity,
	})
	if err != nil {
		return DeploymentStatus{}, err
	}
	paths := deploymentPathsFor(cfg)
	info, err := os.Lstat(paths.metadataDir)
	if errors.Is(err, os.ErrNotExist) {
		if _, canonicalErr := os.Lstat(paths.canonical); !errors.Is(canonicalErr, os.ErrNotExist) {
			if canonicalErr == nil {
				return DeploymentStatus{}, fmt.Errorf("%w: canonical app has no deployment state", ErrInstallConflict)
			}
			return DeploymentStatus{}, canonicalErr
		}
		return absentDeploymentStatus(), nil
	}
	if err != nil {
		return DeploymentStatus{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return DeploymentStatus{}, fmt.Errorf("%w: deployment state root is not a real directory", ErrInstallConflict)
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockShared, Deadline: lockDeadline}).AcquireExisting(ctx)
	if errors.Is(err, os.ErrNotExist) {
		if _, receiptErr := os.Lstat(paths.receipt); !errors.Is(receiptErr, os.ErrNotExist) {
			return DeploymentStatus{}, fmt.Errorf("%w: deployment receipt exists without its lock", ErrInstallState)
		}
		if _, txErr := os.Lstat(paths.transaction); !errors.Is(txErr, os.ErrNotExist) {
			return DeploymentStatus{}, fmt.Errorf("%w: deployment transaction exists without its lock", ErrInstallState)
		}
		return DeploymentStatus{}, fmt.Errorf("%w: partial deployment metadata exists without its lock", ErrInstallState)
	}
	if err != nil {
		return DeploymentStatus{}, err
	}
	defer lock.Close()
	fingerprint, err := artifactFingerprint(cfg, requirement)
	if err != nil {
		return DeploymentStatus{}, err
	}
	binding := replacementBinding(cfg).String()
	status := newDeploymentStatus()
	receipt, receiptErr := readDeploymentReceipt(paths.receipt)
	if receiptErr == nil {
		value, err := receipt.public()
		if err != nil {
			return DeploymentStatus{}, err
		}
		status.receipt = &value
		status.consumerBuild = receipt.LastOperation.ConsumerBuild
		status.policyDigest = receipt.LastOperation.PolicyDigest
		status.replacementBinding = receipt.LastOperation.ReplacementBinding
		status.configMatches = receipt.ArtifactFingerprint == fingerprint &&
			receipt.LastOperation.ConsumerBuild == cfg.ConsumerBuild &&
			receipt.LastOperation.PolicyDigest == cfg.PolicyDigest.String() &&
			receipt.LastOperation.ReplacementBinding == binding
		if !status.configMatches {
			status.configMismatch = "current config differs from completed receipt"
		}
	} else if !errors.Is(receiptErr, os.ErrNotExist) {
		return DeploymentStatus{}, receiptErr
	}
	tx, txErr := readDeploymentTransaction(paths.transaction)
	if errors.Is(txErr, os.ErrNotExist) {
		if receipt != nil && receipt.Current != nil {
			if err := c.verifyStoredReceipt(ctx, paths, *receipt); err != nil {
				return DeploymentStatus{}, err
			}
			generation := canonicalGeneration(*receipt.Current)
			status.canonical = &generation
		}
		return status, nil
	}
	if txErr != nil {
		return DeploymentStatus{}, txErr
	}
	if receipt != nil && receipt.LastOperation.OperationID == tx.OperationID {
		expected, expectedErr := tx.forwardReceipt()
		if expectedErr != nil || !reflect.DeepEqual(*receipt, expected) {
			return DeploymentStatus{}, errors.Join(ErrRecoveryRequired, ErrAmbiguousState, expectedErr,
				errors.New("deployment: status receipt does not match active transaction outcome"))
		}
	} else if err := verifyPriorReceiptExact(tx, receipt); err != nil {
		return DeploymentStatus{}, errors.Join(ErrRecoveryRequired, ErrAmbiguousState, err)
	}
	status.operationID = tx.OperationID
	status.consumerBuild = tx.ConsumerBuild
	status.policyDigest = tx.PolicyDigest
	status.replacementBinding = tx.ReplacementBinding
	status.phase = tx.Phase
	status.direction = tx.Direction
	status.failure = tx.Failure
	status.configMatches = tx.ArtifactFingerprint == fingerprint && tx.ConsumerBuild == cfg.ConsumerBuild &&
		tx.PolicyDigest == cfg.PolicyDigest.String() && tx.ReplacementBinding == binding
	if !status.configMatches {
		status.configMismatch = "current config differs from active transaction"
		status.recoveryRequired = true
	}
	if tx.Phase == PhaseRecoveryRequired {
		status.recoveryRequired = true
	}
	canonical, staged, err := exactNamespaceState(paths, tx)
	if err != nil {
		return DeploymentStatus{}, err
	}
	if canonical != nil {
		var record storedGeneration
		switch {
		case *canonical == tx.Candidate.FileID:
			record = tx.Candidate
		case tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil && *canonical == tx.PriorReceipt.Current.FileID:
			record = *tx.PriorReceipt.Current
		default:
			return status, errors.Join(ErrRecoveryRequired, ErrAmbiguousState)
		}
		if err := c.verifyGeneration(ctx, record); err != nil {
			return status, errors.Join(ErrRecoveryRequired, err)
		}
		value := canonicalGeneration(record)
		status.canonical = &value
	}
	if staged != nil {
		var record storedGeneration
		switch {
		case *staged == tx.Candidate.FileID:
			record = tx.Candidate
		case tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil && *staged == tx.PriorReceipt.Current.FileID:
			record = *tx.PriorReceipt.Current
		default:
			return status, errors.Join(ErrRecoveryRequired, ErrAmbiguousState)
		}
		stagedRecord := record
		stagedRecord.Path = stageApp(paths, tx)
		if err := c.verifyGeneration(ctx, stagedRecord); err != nil {
			return status, errors.Join(ErrRecoveryRequired, err)
		}
		value := canonicalGeneration(record)
		status.staged = &value
	}
	return status, nil
}

type lockedOperation func(context.Context, deploymentPaths, serviceAccess) (DeploymentReceipt, error)

type serviceAccess struct {
	open   controllerFactory
	config service.ControllerConfig
}

func (a serviceAccess) use(ctx context.Context, operation func(deploymentController) error) (returnErr error) {
	controller, err := a.open(ctx, a.config)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryBound)
		defer cancel()
		returnErr = errors.Join(returnErr, controller.Close(closeCtx))
	}()
	return operation(controller)
}

func serviceValue[T any](ctx context.Context, access serviceAccess, operation func(deploymentController) (T, error)) (value T, returnErr error) {
	returnErr = access.use(ctx, func(controller deploymentController) error {
		var err error
		value, err = operation(controller)
		return err
	})
	return value, returnErr
}

var errRuntimeStopperExpired = errors.New("deployment: runtime stopper capability expired")

type runtimeStopAccess struct {
	services     serviceAccess
	scope        context.Context
	cancel       context.CancelFunc
	operationID  string
	runtimeBuild string
	serial       chan struct{}

	mu       sync.Mutex
	closed   bool
	inFlight int
	settled  *sync.Cond
}

func newRuntimeStopAccess(
	ctx context.Context,
	services serviceAccess,
	operationID, runtimeBuild string,
) *runtimeStopAccess {
	scope, cancel := context.WithCancel(ctx)
	access := &runtimeStopAccess{
		services: services, scope: scope, cancel: cancel,
		operationID: operationID, runtimeBuild: runtimeBuild, serial: make(chan struct{}, 1),
	}
	access.serial <- struct{}{}
	access.settled = sync.NewCond(&access.mu)
	return access
}

func (s *runtimeStopAccess) StopRuntime(
	ctx context.Context,
	request service.StopRuntimeRequest,
) (service.StopReceipt, error) {
	if err := ctx.Err(); err != nil {
		return service.StopReceipt{}, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return service.StopReceipt{}, errRuntimeStopperExpired
	}
	if request.OperationID != s.operationID || request.ExpectedRuntimeBuild != s.runtimeBuild {
		s.mu.Unlock()
		return service.StopReceipt{}, errors.New("deployment: runtime stop request differs from operation scope")
	}
	s.inFlight++
	s.mu.Unlock()
	opCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.scope, cancel) //nolint:contextcheck // merge callback and operation lifetimes
	defer func() {
		stop()
		cancel()
		s.mu.Lock()
		s.inFlight--
		if s.inFlight == 0 {
			s.settled.Broadcast()
		}
		s.mu.Unlock()
	}()
	select {
	case <-opCtx.Done():
		return service.StopReceipt{}, opCtx.Err()
	case <-s.serial:
	}
	defer func() { s.serial <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return service.StopReceipt{}, err
	}
	if err := opCtx.Err(); err != nil {
		return service.StopReceipt{}, err
	}
	return serviceValue(opCtx, s.services, func(controller deploymentController) (service.StopReceipt, error) {
		return controller.StopRuntime(opCtx, request)
	})
}

func (s *runtimeStopAccess) revoke() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	for s.inFlight != 0 {
		s.settled.Wait()
	}
	s.mu.Unlock()
}

func (a serviceAccess) snapshot(ctx context.Context) (service.Plan, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (service.Plan, error) {
		return controller.Snapshot(ctx)
	})
}

func (a serviceAccess) replacementStatus(ctx context.Context) (*service.ReplacementStatus, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (*service.ReplacementStatus, error) {
		return controller.ReplacementStatus(ctx)
	})
}

func (a serviceAccess) quiesce(ctx context.Context, operationID string, binding service.ReplacementBinding, plan service.Plan) (service.QuiesceReceipt, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (service.QuiesceReceipt, error) {
		return controller.Quiesce(ctx, operationID, binding, plan)
	})
}

func (a serviceAccess) proveQuiesced(ctx context.Context, receipt service.QuiesceReceipt, paths []string) error {
	return a.use(ctx, func(controller deploymentController) error {
		return controller.ProveQuiesced(ctx, receipt, paths)
	})
}

func (a serviceAccess) apply(ctx context.Context, operationID string, binding service.ReplacementBinding, plan service.Plan) error {
	return a.use(ctx, func(controller deploymentController) error {
		return controller.ApplyReplacement(ctx, operationID, binding, plan)
	})
}

func (a serviceAccess) requiesce(ctx context.Context, operationID string, binding service.ReplacementBinding) (service.QuiesceReceipt, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (service.QuiesceReceipt, error) {
		return controller.Requiesce(ctx, operationID, binding)
	})
}

func (a serviceAccess) restore(ctx context.Context, operationID string, binding service.ReplacementBinding) error {
	return a.use(ctx, func(controller deploymentController) error {
		return controller.RestoreReplacement(ctx, operationID, binding)
	})
}

func (a serviceAccess) completion(ctx context.Context) (*replacementCompletion, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (*replacementCompletion, error) {
		return controller.DeploymentCompletion(ctx)
	})
}

func (a serviceAccess) acknowledgement(ctx context.Context) (*replacementCompletion, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (*replacementCompletion, error) {
		return controller.DeploymentAcknowledgement(ctx)
	})
}

func (a serviceAccess) commit(ctx context.Context, operationID string, binding service.ReplacementBinding, plan service.Plan) (replacementCompletion, error) {
	return serviceValue(ctx, a, func(controller deploymentController) (replacementCompletion, error) {
		return controller.CommitDeploymentReplacement(ctx, operationID, binding, plan)
	})
}

func (a serviceAccess) acknowledge(ctx context.Context, commit replacementCompletion) error {
	return a.use(ctx, func(controller deploymentController) error {
		return controller.AcknowledgeDeploymentReplacement(ctx, commit)
	})
}

func (c *Controller) withDeploymentLock(ctx context.Context, cfg Config, operation lockedOperation) (receipt DeploymentReceipt, returnErr error) {
	if c == nil || c.client == nil || c.verifier == nil || c.openController == nil || c.operationID == nil {
		return DeploymentReceipt{}, fmt.Errorf("%w: controller dependencies are required", ErrInvalidConfig)
	}
	paths := deploymentPathsFor(cfg)
	if err := ensureMetadataDir(cfg.Dir, paths.metadataDir); err != nil {
		return DeploymentReceipt{}, err
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: lockDeadline}).Acquire(ctx)
	if err != nil {
		return DeploymentReceipt{}, fmt.Errorf("deployment: acquire deployment lock: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Close()) }()
	services := serviceAccess{open: c.openController, config: service.ControllerConfig{
		StatePath: paths.serviceState, ProcessPath: paths.serviceProcess, WorkerLimit: serviceWorkerLimit,
	}}
	return operation(ctx, paths, services)
}

func (c *Controller) beginLocked(
	ctx context.Context,
	cfg Config,
	requirement string,
	fingerprint string,
	paths deploymentPaths,
	services serviceAccess,
	prior *storedReceipt,
) (DeploymentReceipt, error) {
	if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: transaction appeared before begin"))
		}
		return DeploymentReceipt{}, err
	}
	if err := cleanupPreparationResidue(paths); err != nil {
		return DeploymentReceipt{}, err
	}
	if prior == nil {
		if _, err := os.Lstat(paths.canonical); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return DeploymentReceipt{}, fmt.Errorf("%w: canonical app has no schema-v1 deployment receipt", ErrInstallConflict)
			}
			return DeploymentReceipt{}, err
		}
	} else if err := c.verifyStoredReceipt(ctx, paths, *prior); err != nil {
		return DeploymentReceipt{}, err
	}
	priorPlan, err := services.snapshot(ctx)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	if prior == nil && len(priorPlan.Agents()) != 0 {
		return DeploymentReceipt{}, fmt.Errorf("%w: service plan exists without a deployment receipt", ErrInstallConflict)
	}
	if prior != nil {
		receiptPlan, err := restorePlan(prior.Plan)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if !samePlan(priorPlan, receiptPlan) {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: service plan differs from prior receipt"))
		}
	}
	stage, candidate, err := c.prepareCandidate(ctx, cfg, paths, requirement)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	opID, err := c.operationID()
	if err != nil {
		_ = os.RemoveAll(filepath.Join(paths.metadataDir, stage))
		return DeploymentReceipt{}, err
	}
	binding := replacementBinding(cfg)
	tx := &deploymentTransaction{
		Identity: deploymentIdentity, Schema: deploymentSchema, Fingerprint: deploymentFingerprint,
		OperationID: opID, ArtifactFingerprint: fingerprint, ConsumerBuild: cfg.ConsumerBuild,
		PolicyDigest: cfg.PolicyDigest.String(), ReplacementBinding: binding.String(),
		Direction: DirectionForward, Mode: modeReplace, Phase: PhasePrepared, Stage: stage,
		PriorReceipt: prior, Candidate: candidate, PriorPlan: storePlan(priorPlan),
	}
	if err := c.checkpoint(paths, tx); err != nil {
		if !isCheckpointFailure(err) {
			exact, readErr := readDeploymentTransaction(paths.transaction)
			switch {
			case readErr == nil && reflect.DeepEqual(exact, tx):
			case errors.Is(readErr, os.ErrNotExist):
				_ = os.RemoveAll(filepath.Join(paths.metadataDir, stage))
			default:
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err, readErr)
			}
		}
		return DeploymentReceipt{}, err
	}
	return c.forwardOrRollback(ctx, cfg, paths, services, tx)
}

func (c *Controller) recoverLocked(
	ctx context.Context,
	cfg Config,
	requirement string,
	paths deploymentPaths,
	services serviceAccess,
) (DeploymentReceipt, bool, error) {
	tx, err := readDeploymentTransaction(paths.transaction)
	if errors.Is(err, os.ErrNotExist) {
		receipt, receiptErr := readDeploymentReceipt(paths.receipt)
		if errors.Is(receiptErr, os.ErrNotExist) {
			completion, completionErr := services.completion(ctx)
			if completionErr != nil {
				return DeploymentReceipt{}, false, completionErr
			}
			if completion != nil {
				return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired,
					errors.New("deployment: service completion exists without a deployment receipt"))
			}
			if _, canonicalErr := os.Lstat(paths.canonical); !errors.Is(canonicalErr, os.ErrNotExist) {
				if canonicalErr != nil {
					return DeploymentReceipt{}, false, canonicalErr
				}
				return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired,
					ErrInstallConflict, errors.New("deployment: canonical app exists without a deployment receipt"))
			}
			if err := cleanupPreparationResidue(paths); err != nil {
				return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, err)
			}
			status, statusErr := services.replacementStatus(ctx)
			current, snapshotErr := services.snapshot(ctx)
			empty, planErr := service.NewPlan(nil)
			if statusErr != nil || snapshotErr != nil || planErr != nil || status != nil || !samePlan(current, empty) {
				return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, statusErr, snapshotErr, planErr,
					errors.New("deployment: managed absence service state is not empty"))
			}
			return DeploymentReceipt{}, false, nil
		}
		if receiptErr != nil {
			return DeploymentReceipt{}, false, receiptErr
		}
		if err := c.verifyStoredReceipt(ctx, paths, *receipt); err != nil {
			return DeploymentReceipt{}, false, err
		}
		if err := acknowledgeReceiptCompletion(ctx, services, *receipt); err != nil {
			return DeploymentReceipt{}, false, err
		}
		if err := verifyCompletedReceiptServices(ctx, services, *receipt); err != nil {
			return DeploymentReceipt{}, false, err
		}
		if err := cleanupPreparationResidue(paths); err != nil {
			return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, err)
		}
		public, err := receipt.public()
		return public, false, err
	}
	if err != nil {
		return DeploymentReceipt{}, false, err
	}
	if err := candidateMatchesConfig(*tx, cfg, requirement, paths); err != nil {
		return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, err)
	}
	fingerprint, err := artifactFingerprint(cfg, requirement)
	if err != nil {
		return DeploymentReceipt{}, false, err
	}
	binding := replacementBinding(cfg)
	if tx.Mode != modeDeactivate && tx.ArtifactFingerprint != fingerprint || tx.ConsumerBuild != cfg.ConsumerBuild ||
		tx.PolicyDigest != cfg.PolicyDigest.String() || tx.ReplacementBinding != binding.String() {
		return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, errors.New("deployment: recovery config does not match transaction"))
	}
	if tx.Phase == PhaseRecoveryRequired {
		return recoveryRequiredDeploymentReceipt(tx.OperationID, tx.Failure),
			false, errors.Join(ErrRecoveryRequired, ErrAmbiguousState)
	}
	current, receiptErr := readDeploymentReceipt(paths.receipt)
	if receiptErr != nil && !errors.Is(receiptErr, os.ErrNotExist) {
		receipt, markErr := c.markRecoveryRequired(paths, tx, receiptErr)
		return receipt, false, markErr
	}
	if current != nil && current.LastOperation.OperationID == tx.OperationID {
		expected, expectedErr := tx.forwardReceipt()
		if expectedErr != nil || !reflect.DeepEqual(*current, expected) {
			receipt, markErr := c.markRecoveryRequired(paths, tx, errors.Join(
				errors.New("deployment: durable receipt does not exactly match its transaction"), expectedErr))
			return receipt, false, markErr
		}
		if !phaseAtLeast(tx.Phase, PhaseReceiptCommitted) {
			tx.Phase = PhaseReceiptCommitted
		}
		tx.Direction = DirectionForward
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, false, errors.Join(ErrRecoveryRequired, err)
		}
		receipt, err := c.finishCommitted(ctx, cfg, paths, services, tx)
		return receipt, true, err
	}
	if err := verifyPriorReceiptExact(tx, current); err != nil {
		receipt, markErr := c.markRecoveryRequired(paths, tx, err)
		return receipt, false, markErr
	}
	if tx.Direction == DirectionRollback {
		receipt, err := c.rollback(ctx, cfg, paths, services, tx, errors.New(tx.Failure))
		return receipt, false, err
	}
	if tx.Phase == PhaseCandidateReady {
		receipt, err := c.forwardOrRollback(ctx, cfg, paths, services, tx)
		return receipt, true, err
	}
	receipt, err := c.rollback(ctx, cfg, paths, services, tx, errors.New("deployment: recovered incomplete forward transaction"))
	return receipt, false, err
}

func acknowledgeReceiptCompletion(ctx context.Context, services serviceAccess, receipt storedReceipt) error {
	completion, err := services.completion(ctx)
	if err != nil || completion == nil {
		return err
	}
	prior, priorErr := restorePlan(receipt.PriorPlan)
	next, nextErr := restorePlan(receipt.Plan)
	if priorErr != nil || nextErr != nil || completion.OperationID != receipt.LastOperation.OperationID ||
		completion.Binding.String() != receipt.LastOperation.ReplacementBinding ||
		!samePlan(completion.Prior, prior) || !samePlan(completion.Next, next) {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: pending service completion differs from receipt"),
			priorErr, nextErr)
	}
	return services.acknowledge(ctx, *completion)
}

func verifyCompletedReceiptServices(ctx context.Context, services serviceAccess, receipt storedReceipt) error {
	status, statusErr := services.replacementStatus(ctx)
	current, snapshotErr := services.snapshot(ctx)
	expected, planErr := restorePlan(receipt.Plan)
	if statusErr != nil || snapshotErr != nil || planErr != nil || status != nil || !samePlan(current, expected) {
		return errors.Join(ErrRecoveryRequired, statusErr, snapshotErr, planErr,
			errors.New("deployment: completed receipt service state is not exact"))
	}
	return nil
}

func (c *Controller) forwardOrRollback(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	services serviceAccess,
	tx *deploymentTransaction,
) (DeploymentReceipt, error) {
	receipt, err := c.forward(ctx, cfg, paths, services, tx)
	if err == nil {
		return receipt, nil
	}
	if isCheckpointFailure(err) {
		return recoveryRequiredDeploymentReceipt(tx.OperationID, boundedFailure(err)), err
	}
	current, readErr := readDeploymentReceipt(paths.receipt)
	if readErr == nil && current.LastOperation.OperationID == tx.OperationID && current.Current != nil &&
		current.Current.FileID == tx.Candidate.FileID {
		return recoveryRequiredDeploymentReceipt(tx.OperationID, err.Error()),
			errors.Join(ErrRecoveryRequired, err)
	}
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return c.markRecoveryRequired(paths, tx, errors.Join(err, readErr))
	}
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryBound)
	defer cancel()
	return c.rollback(recoveryCtx, cfg, paths, services, tx, err)
}

func (c *Controller) forward(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	services serviceAccess,
	tx *deploymentTransaction,
) (DeploymentReceipt, error) {
	binding := replacementBinding(cfg)
	priorPlan, err := restorePlan(tx.PriorPlan)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	if tx.Phase == PhasePrepared {
		quiescence, err := services.quiesce(ctx, tx.OperationID, binding, priorPlan)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if tx.PriorReceipt != nil && tx.PriorReceipt.State == DeploymentActive && tx.PriorReceipt.Current != nil {
			proof, err := c.runtimeQuiesce(
				ctx, cfg, services, tx.OperationID, *tx.PriorReceipt.Current,
				ProofPriorRuntime,
			)
			if err != nil {
				return DeploymentReceipt{}, err
			}
			tx.PriorRuntimeProof = bindRuntimeProof(tx.OperationID, ProofPriorRuntime, *tx.PriorReceipt.Current, proof)
		}
		programs, err := exactProgramPaths(priorPlan)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if err := services.proveQuiesced(ctx, quiescence, programs); err != nil {
			return DeploymentReceipt{}, err
		}
		tx.Phase = PhasePriorQuiesced
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhasePriorQuiesced {
		switch tx.Mode {
		case modeReconfigure:
			if err := c.verifyGeneration(ctx, tx.Candidate); err != nil {
				return DeploymentReceipt{}, err
			}
			tx.Phase = PhaseNamespaceCandidate
			if err := c.checkpoint(paths, tx); err != nil {
				return DeploymentReceipt{}, err
			}
		case modeDeactivate:
			empty, err := service.NewPlan(nil)
			if err != nil {
				return DeploymentReceipt{}, err
			}
			record := storePlan(empty)
			tx.NextPlan = &record
			tx.Phase = PhaseTargetPlanned
			if err := c.checkpoint(paths, tx); err != nil {
				return DeploymentReceipt{}, err
			}
		}
	}
	if tx.Phase == PhasePriorQuiesced {
		if layout, err := classifyNamespace(paths, tx); err != nil || layout != "pre_swap" {
			return c.markRecoveryRequired(paths, tx, errors.Join(err, errors.New("deployment: namespace is not pre-swap")))
		}
		if err := c.publishCandidate(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
		tx.Phase = PhaseNamespaceCandidate
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhaseNamespaceCandidate {
		proof, err := c.generationProof(ctx, cfg.PostInstallProof, tx.OperationID, tx.Candidate, ProofPostInstall)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		tx.PostProof = bindProof(tx.OperationID, ProofPostInstall, tx.Candidate, "", proof)
		tx.Phase = PhaseCandidateProved
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhaseCandidateProved {
		if tx.NextPlan != nil {
			tx.Phase = PhaseTargetPlanned
			if err := c.checkpoint(paths, tx); err != nil {
				return DeploymentReceipt{}, err
			}
		}
	}
	if tx.Phase == PhaseCandidateProved {
		if err := c.verifyGeneration(ctx, tx.Candidate); err != nil {
			return DeploymentReceipt{}, err
		}
		plan, err := cfg.BuildPlan(ctx, Operation{ID: tx.OperationID, Generation: canonicalGeneration(tx.Candidate)})
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if err := validateDeploymentPlanPrograms(plan, tx.Candidate); err != nil {
			return DeploymentReceipt{}, err
		}
		if err := c.verifyGeneration(ctx, tx.Candidate); err != nil {
			return DeploymentReceipt{}, err
		}
		validated, err := service.NewPlan(plan.Agents())
		if err != nil || !samePlan(plan, validated) {
			return DeploymentReceipt{}, errors.Join(errors.New("deployment: BuildPlan returned a non-canonical plan"), err)
		}
		record := storePlan(validated)
		tx.NextPlan = &record
		tx.Phase = PhaseTargetPlanned
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhaseTargetPlanned {
		next, err := restorePlan(*tx.NextPlan)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if err := services.apply(ctx, tx.OperationID, binding, next); err != nil {
			return DeploymentReceipt{}, err
		}
		status, err := services.replacementStatus(ctx)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if status == nil || status.OperationID != tx.OperationID || status.Binding != binding ||
			status.Phase != service.ReplacementRunningOwned || !samePlan(status.Current, next) {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: service activation receipt mismatch"))
		}
		tx.Activation = &storedActivation{
			OperationID: tx.OperationID, Binding: binding.String(),
			Epoch: status.Epoch, PlanDigest: next.Digest().String(),
		}
		tx.Phase = PhaseCandidateActivated
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhaseCandidateActivated {
		next, err := restorePlan(*tx.NextPlan)
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if tx.Mode != modeDeactivate {
			proof, err := c.readinessProof(ctx, cfg, tx.OperationID, tx.Candidate, next, ProofCandidateReady)
			if err != nil {
				return DeploymentReceipt{}, err
			}
			tx.CandidateReadinessProof = bindProof(
				tx.OperationID, ProofCandidateReady, tx.Candidate, next.Digest().String(), proof,
			)
		}
		tx.Phase = PhaseCandidateReady
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, err
		}
	}
	if tx.Phase == PhaseCandidateReady {
		if err := c.verifyGeneration(ctx, tx.Candidate); err != nil {
			return DeploymentReceipt{}, err
		}
		receipt, err := tx.forwardReceipt()
		if err != nil {
			return DeploymentReceipt{}, err
		}
		if err := syncDeploymentState(paths.receipt, receipt); err != nil {
			return DeploymentReceipt{}, err
		}
		exact, err := readDeploymentReceipt(paths.receipt)
		if err != nil || !reflect.DeepEqual(*exact, receipt) {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: committed receipt did not re-read exact"), err)
		}
		tx.Phase = PhaseReceiptCommitted
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
		}
		return c.finishCommitted(ctx, cfg, paths, services, tx)
	}
	return DeploymentReceipt{}, fmt.Errorf("%w: cannot advance forward phase %q", ErrInstallState, tx.Phase)
}

func (c *Controller) finishCommitted(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	services serviceAccess,
	tx *deploymentTransaction,
) (DeploymentReceipt, error) {
	if tx.NextPlan == nil {
		return c.markRecoveryRequired(paths, tx, errors.New("deployment: committed transaction has no next plan"))
	}
	receipt, err := tx.forwardReceipt()
	if err != nil {
		return c.markRecoveryRequired(paths, tx, err)
	}
	exact, err := readDeploymentReceipt(paths.receipt)
	if err != nil || !reflect.DeepEqual(*exact, receipt) {
		return c.markRecoveryRequired(paths, tx, errors.Join(errors.New("deployment: committed receipt changed"), err))
	}
	if err := c.verifyGeneration(ctx, tx.Candidate); err != nil {
		return c.markRecoveryRequired(paths, tx, err)
	}
	priorPlan, err := restorePlan(tx.PriorPlan)
	if err != nil {
		return c.markRecoveryRequired(paths, tx, err)
	}
	next, err := restorePlan(*tx.NextPlan)
	if err != nil {
		return c.markRecoveryRequired(paths, tx, err)
	}
	binding := replacementBinding(cfg)
	if tx.Phase == PhaseReceiptCommitted {
		completion, err := services.completion(ctx)
		if err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
		}
		if completion == nil {
			status, statusErr := services.replacementStatus(ctx)
			if statusErr != nil {
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, statusErr)
			}
			if status == nil || status.OperationID != tx.OperationID || status.Binding != binding ||
				status.Phase != service.ReplacementRunningOwned || !samePlan(status.Current, next) ||
				tx.Activation == nil || status.Epoch != tx.Activation.Epoch {
				return c.markRecoveryRequired(paths, tx, errors.New("deployment: committed service fence mismatch"))
			}
			committed, commitErr := services.commit(ctx, tx.OperationID, binding, next)
			if commitErr != nil {
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, commitErr)
			}
			completion = &committed
		}
		if err := validateCompletion(completion, tx, priorPlan, next); err != nil {
			return c.markRecoveryRequired(paths, tx, err)
		}
		tx.Phase = PhaseServiceCommitPending
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
		}
	}
	if tx.Phase == PhaseServiceCommitPending {
		completion, err := services.completion(ctx)
		if err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
		}
		if err := validateCompletion(completion, tx, priorPlan, next); err != nil {
			return c.markRecoveryRequired(paths, tx, err)
		}
		var prior *fileID
		if tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil {
			value := tx.PriorReceipt.Current.FileID
			prior = &value
		}
		if tx.Mode == modeReplace {
			if err := cleanupJournaledStage(paths, tx, prior, true); err != nil {
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
			}
		}
		tx.Phase = PhaseCleanupComplete
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
		}
	}
	if tx.Phase != PhaseCleanupComplete {
		return c.markRecoveryRequired(paths, tx, fmt.Errorf("deployment: invalid committed cleanup phase %q", tx.Phase))
	}
	if err := removeDurable(paths.transaction); err != nil {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	if err := c.inject("transaction_removed"); err != nil {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	completion, err := services.completion(ctx)
	if err != nil {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	if err := validateCompletion(completion, tx, priorPlan, next); err != nil {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	if err := services.acknowledge(ctx, *completion); err != nil {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, err)
	}
	return receipt.public()
}

func (c *Controller) rollback(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	services serviceAccess,
	tx *deploymentTransaction,
	cause error,
) (DeploymentReceipt, error) {
	if tx.Direction == DirectionForward && phaseAtLeast(tx.Phase, PhaseReceiptCommitted) {
		return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause)
	}
	if tx.Direction != DirectionRollback {
		tx.Direction = DirectionRollback
		tx.RollbackFrom = tx.Phase
		tx.Failure = boundedFailure(cause)
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	binding := replacementBinding(cfg)
	priorPlan, err := restorePlan(tx.PriorPlan)
	if err != nil {
		return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
	}
	if tx.Phase == tx.RollbackFrom {
		status, err := services.replacementStatus(ctx)
		if err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		var quiescence service.QuiesceReceipt
		if status != nil && (status.OperationID != tx.OperationID || status.Binding != binding) {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, service.ErrReplacementMismatch))
		}
		if tx.Activation == nil && status != nil && status.Phase == service.ReplacementRunningOwned && tx.NextPlan != nil {
			next, planErr := restorePlan(*tx.NextPlan)
			if planErr != nil || !samePlan(status.Current, next) {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, planErr, service.ErrReplacementMismatch))
			}
			tx.Activation = &storedActivation{
				OperationID: tx.OperationID, Binding: binding.String(),
				Epoch: status.Epoch, PlanDigest: next.Digest().String(),
			}
			if err := c.checkpoint(paths, tx); err != nil {
				return DeploymentReceipt{}, errors.Join(cause, err)
			}
		}
		if tx.Activation != nil {
			if status == nil || status.Epoch != tx.Activation.Epoch || tx.NextPlan == nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, service.ErrReplacementMismatch))
			}
			next, planErr := restorePlan(*tx.NextPlan)
			if planErr != nil || !samePlan(status.Current, next) {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, planErr, service.ErrReplacementMismatch))
			}
			switch status.Phase {
			case service.ReplacementRunningOwned, service.ReplacementUnloaded:
				quiescence, err = services.requiesce(ctx, tx.OperationID, binding)
			case service.ReplacementQuiesced:
				quiescence = service.QuiesceReceipt{
					OperationID: tx.OperationID, Binding: binding,
					Epoch: status.Epoch, Plan: status.Current,
				}
			default:
				err = service.ErrReplacementMismatch
			}
		} else {
			quiescence, err = services.quiesce(ctx, tx.OperationID, binding, priorPlan)
		}
		if err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		if tx.Activation != nil && tx.Mode != modeDeactivate {
			proof, err := c.runtimeQuiesce(
				ctx, cfg, services, tx.OperationID, tx.Candidate, ProofRollbackRuntime,
			)
			if err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			tx.RollbackRuntimeProof = bindRuntimeProof(tx.OperationID, ProofRollbackRuntime, tx.Candidate, proof)
		} else if tx.PriorReceipt != nil && tx.PriorReceipt.State == DeploymentActive &&
			tx.PriorReceipt.Current != nil && tx.PriorRuntimeProof == nil {
			proof, err := c.runtimeQuiesce(
				ctx, cfg, services, tx.OperationID, *tx.PriorReceipt.Current,
				ProofPriorRuntime,
			)
			if err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			tx.PriorRuntimeProof = bindRuntimeProof(tx.OperationID, ProofPriorRuntime, *tx.PriorReceipt.Current, proof)
		}
		programs, err := exactProgramPaths(quiescence.Plan)
		if err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		if err := services.proveQuiesced(ctx, quiescence, programs); err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		tx.Phase = PhaseRollbackQuiesced
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	if tx.Phase == PhaseRollbackQuiesced {
		if tx.Mode == modeReplace {
			layout, err := classifyNamespace(paths, tx)
			if err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			if layout == "post_swap" {
				if err := c.restorePrior(paths, tx); err != nil {
					return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
				}
			}
		}
		tx.Phase = PhasePriorRestored
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	if tx.Phase == PhasePriorRestored {
		if tx.Mode == modeReplace && tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil {
			proof, err := c.generationProof(
				ctx, cfg.PriorAppRestoreProof, tx.OperationID, *tx.PriorReceipt.Current, ProofPriorRestore,
			)
			if err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			tx.RestoreProof = bindProof(tx.OperationID, ProofPriorRestore, *tx.PriorReceipt.Current, "", proof)
		}
		tx.Phase = PhasePriorProved
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	if tx.Phase == PhasePriorProved {
		if err := services.restore(ctx, tx.OperationID, binding); err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		tx.Phase = PhasePriorActivated
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	if tx.Phase == PhasePriorActivated {
		if tx.PriorReceipt != nil && tx.PriorReceipt.State == DeploymentActive &&
			tx.PriorReceipt.Current != nil {
			proof, err := c.readinessProof(
				ctx, cfg, tx.OperationID, *tx.PriorReceipt.Current, priorPlan, ProofPriorReady,
			)
			if err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			tx.PriorReadinessProof = bindProof(
				tx.OperationID, ProofPriorReady, *tx.PriorReceipt.Current, priorPlan.Digest().String(), proof,
			)
		}
		tx.Phase = PhasePriorReady
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(cause, err)
		}
	}
	if tx.Phase == PhasePriorReady {
		current, err := readDeploymentReceipt(paths.receipt)
		if errors.Is(err, os.ErrNotExist) {
			current = nil
		} else if err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		if err := verifyPriorReceiptExact(tx, current); err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		tx.Phase = PhaseReceiptCommitted
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
		}
	}
	if tx.Phase == PhaseReceiptCommitted {
		completion, err := services.completion(ctx)
		if err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		if completion == nil {
			status, statusErr := services.replacementStatus(ctx)
			if statusErr != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, statusErr))
			}
			if status == nil || status.OperationID != tx.OperationID || status.Binding != binding ||
				status.Phase != service.ReplacementRunningOwned || !samePlan(status.Current, priorPlan) {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause,
					errors.New("deployment: prior fence mismatch before rollback commit")))
			}
			committed, commitErr := services.commit(ctx, tx.OperationID, binding, priorPlan)
			if commitErr != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, commitErr))
			}
			completion = &committed
		}
		if err := validateCompletion(completion, tx, priorPlan, priorPlan); err != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
		}
		tx.Phase = PhaseServiceCommitPending
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
		}
	}
	if tx.Phase == PhaseServiceCommitPending {
		completion, err := services.completion(ctx)
		if err != nil || validateCompletion(completion, tx, priorPlan, priorPlan) != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, err,
				errors.New("deployment: rollback service completion is not exact")))
		}
		if tx.Mode == modeReplace {
			candidate := tx.Candidate.FileID
			if err := cleanupJournaledStage(paths, tx, &candidate, true); err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
		}
		tx.Phase = PhaseCleanupComplete
		if err := c.checkpoint(paths, tx); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
		}
	}
	if tx.Phase == PhaseCleanupComplete {
		completion, completionErr := services.completion(ctx)
		if completionErr != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, completionErr))
		}
		if completion != nil {
			if err := validateCompletion(completion, tx, priorPlan, priorPlan); err != nil {
				return c.markRecoveryRequired(paths, tx, errors.Join(cause, err))
			}
			if err := services.acknowledge(ctx, *completion); err != nil {
				return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
			}
		}
		acknowledged, ackErr := services.acknowledgement(ctx)
		if ackErr != nil || validateCompletion(acknowledged, tx, priorPlan, priorPlan) != nil {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, ackErr,
				errors.New("deployment: rollback acknowledgement is not exact")))
		}
		status, statusErr := services.replacementStatus(ctx)
		current, snapshotErr := services.snapshot(ctx)
		if statusErr != nil || snapshotErr != nil || status != nil || !samePlan(current, priorPlan) {
			return c.markRecoveryRequired(paths, tx, errors.Join(cause, statusErr, snapshotErr,
				errors.New("deployment: rollback acknowledged service state is not exact")))
		}
		if err := c.inject("service_acknowledged"); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
		}
		if err := removeDurable(paths.transaction); err != nil {
			return DeploymentReceipt{}, errors.Join(ErrRecoveryRequired, cause, err)
		}
		if tx.PriorReceipt == nil {
			return DeploymentReceipt{}, cause
		}
		result, err := tx.PriorReceipt.public()
		return result, errors.Join(cause, err)
	}
	return c.markRecoveryRequired(paths, tx, errors.Join(cause, fmt.Errorf("deployment: cannot advance rollback phase %q", tx.Phase)))
}

func (c *Controller) runtimeQuiesce(
	ctx context.Context,
	cfg Config,
	services serviceAccess,
	operationID string,
	generation storedGeneration,
	role ProofRole,
) (RuntimeProof, error) {
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return RuntimeProof{}, err
	}
	stopper := newRuntimeStopAccess(ctx, services, operationID, generation.Version)
	defer stopper.revoke()
	proof, err := cfg.RuntimeQuiesce(ctx, stopper, RuntimeQuiesceOperation{
		ID: operationID, Generation: canonicalGeneration(generation), Role: role,
	})
	if err != nil {
		return RuntimeProof{}, err
	}
	if err := validateRuntimeProof(proof, role); err != nil {
		return RuntimeProof{}, err
	}
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return RuntimeProof{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: RuntimeQuiesce mutated canonical generation"), err)
	}
	return proof, nil
}

func (c *Controller) generationProof(
	ctx context.Context,
	hook func(context.Context, Operation) (Proof, error),
	operationID string,
	generation storedGeneration,
	role ProofRole,
) (Proof, error) {
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return Proof{}, err
	}
	proof, err := hook(ctx, Operation{ID: operationID, Generation: canonicalGeneration(generation), Role: role})
	if err != nil {
		return Proof{}, err
	}
	if err := validateProof(proof, role, SHA256{}); err != nil {
		return Proof{}, err
	}
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return Proof{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: proof hook mutated canonical generation"), err)
	}
	return proof, nil
}

func (c *Controller) readinessProof(
	ctx context.Context,
	cfg Config,
	operationID string,
	generation storedGeneration,
	plan service.Plan,
	role ProofRole,
) (Proof, error) {
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return Proof{}, err
	}
	planDigest := SHA256(plan.Digest())
	proof, err := cfg.Readiness(ctx, Operation{
		ID: operationID, Generation: canonicalGeneration(generation), Role: role, PlanDigest: planDigest,
	}, plan)
	if err != nil {
		return Proof{}, err
	}
	if err := validateProof(proof, role, planDigest); err != nil {
		return Proof{}, err
	}
	if err := c.verifyGeneration(ctx, generation); err != nil {
		return Proof{}, errors.Join(ErrRecoveryRequired, errors.New("deployment: readiness mutated canonical generation"), err)
	}
	return proof, nil
}

func (c *Controller) verifyComplete(
	ctx context.Context,
	cfg Config,
	paths deploymentPaths,
	services serviceAccess,
	receipt storedReceipt,
) error {
	if receipt.Current == nil {
		return fmt.Errorf("%w: complete receipt has no current generation", ErrInstallState)
	}
	if err := c.verifyStoredReceipt(ctx, paths, receipt); err != nil {
		return err
	}
	plan, err := restorePlan(receipt.Plan)
	if err != nil {
		return err
	}
	status, err := services.replacementStatus(ctx)
	if err != nil {
		return err
	}
	if status != nil {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: complete receipt retains a service fence"))
	}
	current, err := services.snapshot(ctx)
	if err != nil {
		return err
	}
	if receipt.State != DeploymentActive || !samePlan(current, plan) ||
		receipt.LastOperation.PolicyDigest != cfg.PolicyDigest.String() {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: complete receipt policy or service plan mismatch"))
	}
	return nil
}

func (c *Controller) verifyStoredReceipt(ctx context.Context, paths deploymentPaths, receipt storedReceipt) error {
	if receipt.Current == nil {
		return fmt.Errorf("%w: receipt canonical path", ErrInstallState)
	}
	if receipt.Current.Path != paths.canonical {
		return fmt.Errorf("%w: receipt canonical path", ErrInstallState)
	}
	return c.verifyGeneration(ctx, *receipt.Current)
}

func (c *Controller) checkpoint(paths deploymentPaths, tx *deploymentTransaction) error {
	if err := syncDeploymentState(paths.transaction, tx); err != nil {
		return err
	}
	if c.failpoint != nil {
		if err := c.failpoint(string(tx.Phase)); err != nil {
			return checkpointFailure{err: err}
		}
	}
	return nil
}

func (c *Controller) inject(name string) error {
	if c.failpoint == nil {
		return nil
	}
	if err := c.failpoint(name); err != nil {
		return checkpointFailure{err: err}
	}
	return nil
}

type checkpointFailure struct{ err error }

func (e checkpointFailure) Error() string { return e.err.Error() }
func (e checkpointFailure) Unwrap() error { return e.err }

func isCheckpointFailure(err error) bool {
	var failure checkpointFailure
	return errors.As(err, &failure)
}

func (c *Controller) markRecoveryRequired(
	paths deploymentPaths,
	tx *deploymentTransaction,
	cause error,
) (DeploymentReceipt, error) {
	tx.Phase = PhaseRecoveryRequired
	tx.Failure = boundedFailure(cause)
	writeErr := syncDeploymentState(paths.transaction, tx)
	return recoveryRequiredDeploymentReceipt(tx.OperationID, tx.Failure),
		errors.Join(ErrRecoveryRequired, cause, writeErr)
}

func (tx deploymentTransaction) forwardReceipt() (storedReceipt, error) {
	if tx.NextPlan == nil {
		return storedReceipt{}, fmt.Errorf("%w: forward transaction has no target plan", ErrInstallState)
	}
	operation := storedOperation{
		OperationID: tx.OperationID, ConsumerBuild: tx.ConsumerBuild,
		PolicyDigest: tx.PolicyDigest, ReplacementBinding: tx.ReplacementBinding,
	}
	receipt := storedReceipt{
		Identity: receiptIdentity, Schema: deploymentSchema, Fingerprint: receiptFingerprint,
		ArtifactFingerprint: tx.ArtifactFingerprint,
		LastOperation:       operation,
		State:               DeploymentActive, Current: &tx.Candidate, PriorPlan: tx.PriorPlan,
		Plan: *tx.NextPlan, ActivationPlan: *tx.NextPlan, ActivationOperation: operation, Failure: tx.Failure,
	}
	if tx.Mode == modeDeactivate {
		receipt.State = DeploymentInactive
		if tx.PriorReceipt == nil {
			return storedReceipt{}, fmt.Errorf("%w: deactivate transaction has no active receipt", ErrInstallState)
		}
		receipt.ActivationPlan = tx.PriorReceipt.ActivationPlan
		receipt.ActivationOperation = tx.PriorReceipt.ActivationOperation
	}
	return receipt, nil
}

func candidateMatchesConfig(tx deploymentTransaction, cfg Config, requirement string, paths deploymentPaths) error {
	if tx.Mode == modeDeactivate {
		if tx.PriorReceipt == nil || tx.PriorReceipt.Current == nil ||
			!reflect.DeepEqual(tx.Candidate, *tx.PriorReceipt.Current) ||
			tx.Candidate.Path != paths.canonical || tx.Candidate.DesignatedRequirement != requirement {
			return fmt.Errorf("%w: deactivate candidate differs from active receipt", ErrInstallState)
		}
		return nil
	}
	if tx.Candidate.Path != paths.canonical || tx.Candidate.Version != cfg.Release.Version ||
		tx.Candidate.URL != cfg.Release.URL || tx.Candidate.SHA256 != cfg.Release.SHA256.String() ||
		tx.Candidate.DesignatedRequirement != requirement {
		return fmt.Errorf("%w: transaction candidate differs from recovery config", ErrInstallState)
	}
	return nil
}

func verifyPriorReceiptExact(tx *deploymentTransaction, current *storedReceipt) error {
	if tx.Direction == DirectionForward && phaseAtLeast(tx.Phase, PhaseReceiptCommitted) {
		return nil
	}
	if tx.PriorReceipt == nil && current == nil {
		return nil
	}
	if tx.PriorReceipt == nil || current == nil || !reflect.DeepEqual(*tx.PriorReceipt, *current) {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: prior receipt differs from transaction"))
	}
	return nil
}

func validateCompletion(commit *replacementCompletion, tx *deploymentTransaction, prior, next service.Plan) error {
	if commit == nil || commit.OperationID != tx.OperationID ||
		commit.Binding.String() != tx.ReplacementBinding || !samePlan(commit.Prior, prior) ||
		!samePlan(commit.Next, next) {
		return errors.Join(ErrRecoveryRequired, errors.New("deployment: service completion differs from transaction"))
	}
	return nil
}

func cleanupPreparationResidue(paths deploymentPaths) error {
	if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.Join(ErrRecoveryRequired, errors.New("deployment: cannot clean preparation residue with an active transaction"))
		}
		return err
	}
	entries, err := os.ReadDir(paths.metadataDir)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".stage-") && !strings.HasPrefix(name, ".download-") {
			continue
		}
		path := filepath.Join(paths.metadataDir, name)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(name, ".stage-") && info.IsDir() && info.Mode()&os.ModeSymlink == 0:
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		case strings.HasPrefix(name, ".download-") && info.Mode().IsRegular():
			if err := os.Remove(path); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unsafe preparation residue %q", ErrInstallConflict, path)
		}
		removed = true
	}
	if removed {
		return syncVerifiedDirectory(paths.metadataDir)
	}
	return nil
}
