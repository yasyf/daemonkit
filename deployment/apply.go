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

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

// ApplyInstalledCandidateConfig installs or upgrades one fixed signed app candidate.
type ApplyInstalledCandidateConfig struct {
	Target                CurrentInstalledSpec
	CandidateSourcePath   string
	CandidateVersion      string
	CandidateBundleDigest SHA256
	ConsumerBuild         string
	PolicyDigest          SHA256
	Plan                  CandidatePlan
	RuntimeQuiesce        func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error)
	Readiness             func(context.Context, InstalledOperation) (ReadinessProof, error)
}

// CandidatePlan is one exact service policy bound to a packaged app resource.
type CandidatePlan struct {
	sourceRoot string
	stored     storedApplyPlan
}

// NewCandidatePlan validates a service policy against an existing packaged app.
func NewCandidatePlan(sourceRoot string, agents []service.Agent) (CandidatePlan, error) {
	if err := validateCanonicalAppPath(sourceRoot); err != nil {
		return CandidatePlan{}, err
	}
	plan, err := service.NewPlan(agents)
	if err != nil {
		return CandidatePlan{}, err
	}
	stored, err := storeApplyPlan(sourceRoot, plan)
	if err != nil {
		return CandidatePlan{}, err
	}
	return CandidatePlan{sourceRoot: sourceRoot, stored: stored}, nil
}

// ApplyInstalledCandidateReceipt is one terminal installed candidate transaction.
type ApplyInstalledCandidateReceipt struct {
	operationID string
	activation  ActivationReceipt
}

type applyCheckpointError struct{ err error }

func (e applyCheckpointError) Error() string { return e.err.Error() }
func (e applyCheckpointError) Unwrap() error { return e.err }

// OperationID returns the durable apply operation identity.
func (r ApplyInstalledCandidateReceipt) OperationID() string { return r.operationID }

// Activation returns the newly active installed generation.
func (r ApplyInstalledCandidateReceipt) Activation() ActivationReceipt { return r.activation }

func installedCandidatePath(appPath string) (string, error) {
	if appPath == "" || !filepath.IsAbs(appPath) || filepath.Clean(appPath) != appPath ||
		!strings.HasSuffix(filepath.Base(appPath), ".app") || filepath.Base(appPath) == ".app" {
		return "", fmt.Errorf("%w: app path must be an exact absolute .app path", ErrInvalidConfig)
	}
	parent := filepath.Dir(appPath)
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return "", fmt.Errorf("%w: install directory is not a canonical real path", ErrInstallConflict)
	}
	name := strings.TrimSuffix(filepath.Base(appPath), ".app")
	return filepath.Join(parent, "."+name+".daemonkit-candidate.app"), nil
}

// ApplyInstalledCandidate owns first install, upgrade, activation, and rollback.
func (c *Controller) ApplyInstalledCandidate(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
) (ApplyInstalledCandidateReceipt, error) {
	validated, err := validateApplyConfig(config)
	if err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	if c == nil || c.verifier == nil || c.openService == nil || c.operationID == nil {
		return ApplyInstalledCandidateReceipt{}, ErrInvalidConfig
	}
	paths := deploymentPathsForApp(config.Target.AppPath)
	if err := ensureMetadataDir(paths); err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: activationLockDeadline}).Acquire(ctx)
	if err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	defer lock.Close()
	if err := retireRemovedUninstall(paths); err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	existing, err := readApply(paths.apply)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := c.stageInstalledCandidate(ctx, config, validated, paths); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	case err != nil:
		return ApplyInstalledCandidateReceipt{}, err
	case existing.ConfigFingerprint == validated.fingerprint:
	case existing.Phase == applyActive:
		if err := c.retireActiveApply(ctx, existing, paths); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		if err := c.stageInstalledCandidate(ctx, config, validated, paths); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	}
	return c.applyCandidateLocked(ctx, config, validated, paths)
}

func retireRemovedUninstall(paths deploymentPaths) error {
	receipt, err := readUninstall(paths.uninstall)
	if errors.Is(err, os.ErrNotExist) {
		if fileExists(paths.removed) {
			return fmt.Errorf("%w: private removal state lacks a receipt", ErrInstallConflict)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if receipt.Phase != uninstallRemoved || fileExists(paths.canonical) || fileExists(paths.removed) {
		return fmt.Errorf("%w: prior uninstall is not terminal", ErrInstallConflict)
	}
	return removeIfExistsDurable(paths.uninstall)
}

type validatedApply struct {
	candidatePath string
	fingerprint   string
	plan          storedApplyPlan
}

func validateApplyConfig(config ApplyInstalledCandidateConfig) (validatedApply, error) {
	if err := validateCurrentInstalledTarget(config.Target); err != nil {
		return validatedApply{}, err
	}
	if config.CandidateSourcePath == "" || !filepath.IsAbs(config.CandidateSourcePath) || filepath.Clean(config.CandidateSourcePath) != config.CandidateSourcePath ||
		strings.TrimSpace(config.CandidateVersion) == "" || config.CandidateVersion != strings.TrimSpace(config.CandidateVersion) ||
		strings.TrimSpace(config.ConsumerBuild) == "" || config.ConsumerBuild != strings.TrimSpace(config.ConsumerBuild) ||
		config.RuntimeQuiesce == nil || config.Readiness == nil {
		return validatedApply{}, fmt.Errorf("%w: exact candidate source, version, build, quiesce, and readiness are required", ErrInvalidConfig)
	}
	if err := config.CandidateBundleDigest.validate("candidate bundle digest"); err != nil {
		return validatedApply{}, err
	}
	if err := config.PolicyDigest.validate("policy digest"); err != nil {
		return validatedApply{}, err
	}
	if config.Plan.sourceRoot != config.CandidateSourcePath || config.Plan.stored.Digest == "" {
		return validatedApply{}, fmt.Errorf("%w: exact candidate-bound service plan is required", ErrInvalidConfig)
	}
	plan := config.Plan.stored
	candidate, err := installedCandidatePath(config.Target.AppPath)
	if err != nil {
		return validatedApply{}, err
	}
	wire := struct {
		Target, Source, Version, BundleDigest, TeamID, SigningIdentifier, ConsumerBuild, PolicyDigest, PlanDigest string
	}{
		Target: config.Target.AppPath, Source: config.CandidateSourcePath, Version: config.CandidateVersion,
		BundleDigest: config.CandidateBundleDigest.String(),
		TeamID:       config.Target.Identity.TeamID, SigningIdentifier: config.Target.Identity.SigningIdentifier,
		ConsumerBuild: config.ConsumerBuild, PolicyDigest: config.PolicyDigest.String(), PlanDigest: plan.Digest,
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return validatedApply{}, err
	}
	return validatedApply{candidatePath: candidate, fingerprint: fmt.Sprintf("%x", sha256.Sum256(payload)), plan: plan}, nil
}

func validateCurrentInstalledTarget(spec CurrentInstalledSpec) error {
	if spec.AppPath == "" || !filepath.IsAbs(spec.AppPath) || filepath.Clean(spec.AppPath) != spec.AppPath ||
		!strings.HasSuffix(filepath.Base(spec.AppPath), ".app") || filepath.Base(spec.AppPath) == ".app" {
		return fmt.Errorf("%w: target app path must be exact", ErrInvalidConfig)
	}
	if _, err := spec.Identity.DRString(); err != nil {
		return fmt.Errorf("%w: target code identity: %w", ErrInvalidConfig, err)
	}
	return nil
}

func (c *Controller) applyCandidateLocked(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
	validated validatedApply,
	paths deploymentPaths,
) (ApplyInstalledCandidateReceipt, error) {
	receipt, err := readApply(paths.apply)
	if errors.Is(err, os.ErrNotExist) {
		receipt, err = c.prepareApply(ctx, config, validated, paths)
		if err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		if err := c.applyCheckpoint("apply:prepared"); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	} else if err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	if receipt.ConfigFingerprint != validated.fingerprint || receipt.TargetPath != config.Target.AppPath ||
		receipt.ConsumerBuild != config.ConsumerBuild || receipt.PolicyDigest != config.PolicyDigest.String() ||
		receipt.Plan.Digest != validated.plan.Digest || !candidateMatchesTarget(config.Target, receipt.Candidate) {
		return ApplyInstalledCandidateReceipt{}, fmt.Errorf("%w: pending apply differs from request", ErrInstallConflict)
	}
	if receipt.Phase == applyRolledBack {
		return ApplyInstalledCandidateReceipt{}, fmt.Errorf("%w: candidate apply rolled back", ErrInstallState)
	}
	if receipt.Phase == applyRollback {
		if err := c.rollbackApply(ctx, config, receipt, paths, validated.candidatePath); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		return ApplyInstalledCandidateReceipt{}, fmt.Errorf("%w: candidate apply rolled back", ErrInstallState)
	}
	if receipt.Phase == applyPrepared {
		if receipt.Prior != nil {
			if _, err := c.deactivateCurrentLocked(ctx, DeactivateCurrentInstalledConfig{
				Current: config.Target, RuntimeQuiesce: config.RuntimeQuiesce,
			}, paths); err != nil {
				return ApplyInstalledCandidateReceipt{}, err
			}
		}
		receipt.Phase = applyQuiesced
		if err := writeJSONDurable(paths.apply, receipt); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		if err := c.applyCheckpoint("apply:quiesced"); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	}
	if receipt.Phase == applyQuiesced {
		if err := c.swapCandidate(ctx, receipt, paths, validated.candidatePath); err != nil {
			var checkpoint applyCheckpointError
			if errors.As(err, &checkpoint) {
				return ApplyInstalledCandidateReceipt{}, err
			}
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath, err)
		}
		receipt.Phase = applySwapped
		if err := writeJSONDurable(paths.apply, receipt); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		if err := c.applyCheckpoint("apply:swapped"); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	}
	if receipt.Phase == applySwapped {
		generation, err := inspectInstalled(ctx, c.verifier, paths.canonical, config.CandidateVersion, config.Target.Identity)
		if err != nil {
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath, err)
		}
		if !sameGenerationBytes(generation, receipt.Candidate) {
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath,
				fmt.Errorf("%w: canonical candidate changed after swap", ErrInstallConflict))
		}
		plan, err := receipt.Plan.bindInstalled(paths.canonical)
		if err != nil {
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath, err)
		}
		activationConfig := ActivateInstalledConfig{
			Expected: InstalledAttestation{stored: generation}, ConsumerBuild: config.ConsumerBuild,
			PolicyDigest: config.PolicyDigest, Plan: plan, Readiness: config.Readiness,
		}
		validatedActivation, err := validateActivateConfig(activationConfig)
		if err != nil {
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath, err)
		}
		active, err := c.activateLocked(ctx, activationConfig, validatedActivation, paths)
		if err != nil {
			return ApplyInstalledCandidateReceipt{}, c.beginRollback(ctx, config, receipt, paths, validated.candidatePath, err)
		}
		receipt.ActivationOperation = active.OperationID()
		receipt.Phase = applyActive
		if err := writeJSONDurable(paths.apply, receipt); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
		if err := c.applyCheckpoint("apply:active"); err != nil {
			return ApplyInstalledCandidateReceipt{}, err
		}
	}
	activation, err := readActivation(paths.activation)
	if err != nil || activation.OperationID != receipt.ActivationOperation || activation.Phase != activationActive {
		return ApplyInstalledCandidateReceipt{}, fmt.Errorf("%w: active apply lacks exact activation", ErrInstallState)
	}
	public, err := activation.public()
	if err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	if err := removeTreeDurable(paths.prior); err != nil {
		return ApplyInstalledCandidateReceipt{}, err
	}
	return ApplyInstalledCandidateReceipt{operationID: receipt.OperationID, activation: public}, nil
}

func candidateMatchesTarget(target CurrentInstalledSpec, candidate storedGeneration) bool {
	return candidate.TeamID == target.Identity.TeamID && candidate.SigningIdentifier == target.Identity.SigningIdentifier
}

func (c *Controller) retireActiveApply(ctx context.Context, receipt *applyReceiptWire, paths deploymentPaths) error {
	activation, err := readActivation(paths.activation)
	if err != nil || activation.Phase != activationActive || activation.OperationID != receipt.ActivationOperation {
		return fmt.Errorf("%w: completed apply lacks its exact activation", ErrInstallState)
	}
	current, err := inspectInstalled(ctx, c.verifier, paths.canonical, receipt.Candidate.Version, codeidentity.CodeIdentity{
		TeamID: receipt.Candidate.TeamID, SigningIdentifier: receipt.Candidate.SigningIdentifier,
	})
	if err != nil {
		return err
	}
	if !sameGenerationBytes(current, receipt.Candidate) {
		return fmt.Errorf("%w: completed apply generation changed", ErrInstallConflict)
	}
	return errors.Join(removeTreeDurable(paths.prior), removeIfExistsDurable(paths.apply))
}

func (c *Controller) recoverApplyForDeactivation(
	ctx context.Context,
	config DeactivateCurrentInstalledConfig,
	paths deploymentPaths,
) error {
	receipt, err := readApply(paths.apply)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	switch receipt.Phase {
	case applyActive:
		return c.retireActiveApply(ctx, receipt, paths)
	case applyRolledBack:
		return c.retireRolledBackApply(ctx, receipt, paths)
	case applyPrepared:
		if err := c.discardApplyCandidate(ctx, receipt, paths); err != nil {
			return err
		}
		return removeIfExistsDurable(paths.apply)
	case applyQuiesced:
		if receipt.Prior == nil && !fileExists(paths.canonical) {
			if err := c.discardApplyCandidate(ctx, receipt, paths); err != nil {
				return err
			}
			return removeIfExistsDurable(paths.apply)
		}
		if receipt.Prior != nil && !fileExists(paths.prior) {
			current, err := inspectInstalled(ctx, c.verifier, paths.canonical, receipt.Prior.Generation.Version, codeidentity.CodeIdentity{
				TeamID: receipt.Prior.Generation.TeamID, SigningIdentifier: receipt.Prior.Generation.SigningIdentifier,
			})
			if err != nil || !sameGenerationBytes(current, receipt.Prior.Generation) {
				return fmt.Errorf("%w: quiesced prior generation changed", ErrInstallConflict)
			}
			if err := c.discardApplyCandidate(ctx, receipt, paths); err != nil {
				return err
			}
			return removeIfExistsDurable(paths.apply)
		}
	case applySwapped, applyRollback:
	}

	rollbackConfig := ApplyInstalledCandidateConfig{
		Target: config.Current, RuntimeQuiesce: config.RuntimeQuiesce, Readiness: config.Readiness,
	}
	if receipt.Phase != applyRollback {
		receipt.Phase = applyRollback
		if err := writeJSONDurable(paths.apply, receipt); err != nil {
			return err
		}
		if err := c.applyCheckpoint("apply:rollback"); err != nil {
			return err
		}
	}
	if err := c.rollbackApply(ctx, rollbackConfig, receipt, paths, mustInstalledCandidatePath(paths.canonical)); err != nil {
		return err
	}
	return c.retireRolledBackApply(ctx, receipt, paths)
}

func (c *Controller) retireRolledBackApply(ctx context.Context, receipt *applyReceiptWire, paths deploymentPaths) error {
	if receipt.Prior != nil {
		activation, err := readActivation(paths.activation)
		if err != nil || activation.OperationID != receipt.RollbackOperation || activation.Phase != activationActive {
			return fmt.Errorf("%w: rolled-back apply lacks its exact prior activation", ErrInstallState)
		}
		current, err := inspectInstalled(ctx, c.verifier, paths.canonical, receipt.Prior.Generation.Version, codeidentity.CodeIdentity{
			TeamID: receipt.Prior.Generation.TeamID, SigningIdentifier: receipt.Prior.Generation.SigningIdentifier,
		})
		if err != nil || !sameGenerationBytes(current, receipt.Prior.Generation) {
			return fmt.Errorf("%w: rolled-back prior generation changed", ErrInstallConflict)
		}
	}
	if err := c.discardApplyCandidate(ctx, receipt, paths); err != nil {
		return err
	}
	return errors.Join(removeTreeDurable(paths.prior), removeIfExistsDurable(paths.apply))
}

func (c *Controller) discardApplyCandidate(ctx context.Context, receipt *applyReceiptWire, paths deploymentPaths) error {
	candidatePath := mustInstalledCandidatePath(paths.canonical)
	if !fileExists(candidatePath) {
		return nil
	}
	candidate, err := inspectInstalled(ctx, c.verifier, candidatePath, receipt.Candidate.Version, codeidentity.CodeIdentity{
		TeamID: receipt.Candidate.TeamID, SigningIdentifier: receipt.Candidate.SigningIdentifier,
	})
	if err != nil || !sameGenerationBytes(candidate, receipt.Candidate) {
		return fmt.Errorf("%w: private candidate changed", ErrInstallConflict)
	}
	return removeTreeDurable(candidatePath)
}

func mustInstalledCandidatePath(appPath string) string {
	path, err := installedCandidatePath(appPath)
	if err != nil {
		panic(err)
	}
	return path
}

func (c *Controller) prepareApply(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
	validated validatedApply,
	paths deploymentPaths,
) (*applyReceiptWire, error) {
	candidate, err := inspectInstalled(ctx, c.verifier, validated.candidatePath, config.CandidateVersion, config.Target.Identity)
	if err != nil {
		return nil, err
	}
	var prior *activationReceiptWire
	activation, activationErr := readActivation(paths.activation)
	_, targetErr := os.Lstat(paths.canonical)
	switch {
	case targetErr == nil:
		if activationErr != nil {
			return nil, fmt.Errorf("%w: installed app exists without sealed activation", ErrInstallConflict)
		}
		if activation.Phase != activationActive || !currentMatchesGeneration(config.Target, activation.Generation) {
			return nil, fmt.Errorf("%w: installed activation differs from target", ErrInstallConflict)
		}
		if err := c.attestStoredGeneration(ctx, activation.Generation); err != nil {
			return nil, err
		}
		prior = activation
	case errors.Is(targetErr, os.ErrNotExist):
		if !errors.Is(activationErr, os.ErrNotExist) || fileExists(paths.serviceState) || fileExists(paths.serviceProcess) || fileExists(paths.deactivation) {
			return nil, fmt.Errorf("%w: absent app has retained deployment state", ErrInstallConflict)
		}
	default:
		return nil, targetErr
	}
	operationID, err := c.operationID()
	if err != nil {
		return nil, err
	}
	if !validOperationID(operationID) {
		return nil, errors.New("deployment: operation ID source returned a noncanonical value")
	}
	receipt := &applyReceiptWire{
		Identity: applyIdentity, Schema: activationSchema, OperationID: operationID,
		ConfigFingerprint: validated.fingerprint, Phase: applyPrepared, TargetPath: paths.canonical,
		Candidate: candidate, Prior: prior, ConsumerBuild: config.ConsumerBuild,
		PolicyDigest: config.PolicyDigest.String(), Plan: validated.plan,
	}
	if err := writeJSONDurable(paths.apply, receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}

func (c *Controller) swapCandidate(
	ctx context.Context,
	receipt *applyReceiptWire,
	paths deploymentPaths,
	candidatePath string,
) error {
	if receipt.Prior != nil && !fileExists(paths.prior) {
		if err := c.attestStoredGeneration(ctx, receipt.Prior.Generation); err != nil {
			return err
		}
		if err := os.Rename(paths.canonical, paths.prior); err != nil {
			return err
		}
		if err := daemon.SyncDir(filepath.Dir(paths.canonical)); err != nil {
			return err
		}
		if err := c.applyCheckpoint("apply:prior_moved"); err != nil {
			return err
		}
	}
	if fileExists(candidatePath) {
		if err := c.attestStoredGeneration(ctx, receipt.Candidate); err != nil {
			return err
		}
		if fileExists(paths.canonical) {
			return fmt.Errorf("%w: canonical app occupied during swap", ErrInstallConflict)
		}
		if err := os.Rename(candidatePath, paths.canonical); err != nil {
			return err
		}
		if err := daemon.SyncDir(filepath.Dir(paths.canonical)); err != nil {
			return err
		}
		if err := c.applyCheckpoint("apply:candidate_moved"); err != nil {
			return err
		}
	}
	current, err := inspectInstalled(ctx, c.verifier, paths.canonical, receipt.Candidate.Version, codeidentity.CodeIdentity{
		TeamID: receipt.Candidate.TeamID, SigningIdentifier: receipt.Candidate.SigningIdentifier,
	})
	if err != nil {
		return err
	}
	expected := receipt.Candidate
	expected.Path = paths.canonical
	if !reflect.DeepEqual(current, expected) {
		return fmt.Errorf("%w: swapped candidate identity changed", ErrInstallConflict)
	}
	return nil
}

func (c *Controller) beginRollback(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
	receipt *applyReceiptWire,
	paths deploymentPaths,
	candidatePath string,
	cause error,
) error {
	receipt.Phase = applyRollback
	if err := writeJSONDurable(paths.apply, receipt); err != nil {
		return errors.Join(cause, err)
	}
	if err := c.applyCheckpoint("apply:rollback"); err != nil {
		return errors.Join(cause, err)
	}
	return errors.Join(cause, c.rollbackApply(ctx, config, receipt, paths, candidatePath))
}

func (c *Controller) rollbackApply(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
	receipt *applyReceiptWire,
	paths deploymentPaths,
	candidatePath string,
) error {
	priorActive := false
	if activation, err := readActivation(paths.activation); err == nil {
		switch {
		case sameGenerationBytes(activation.Generation, receipt.Candidate):
			if receipt.ActivationOperation != "" && activation.OperationID != receipt.ActivationOperation {
				return fmt.Errorf("%w: rollback activation differs from apply", ErrInstallConflict)
			}
			if _, err := c.deactivateCurrentLocked(ctx, DeactivateCurrentInstalledConfig{
				Current: config.Target, RuntimeQuiesce: config.RuntimeQuiesce,
			}, paths); err != nil {
				return err
			}
		case receipt.Prior != nil && sameGenerationBytes(activation.Generation, receipt.Prior.Generation):
			if receipt.RollbackOperation != "" && activation.OperationID != receipt.RollbackOperation {
				return fmt.Errorf("%w: rollback prior activation differs from receipt", ErrInstallConflict)
			}
			receipt.RollbackOperation = activation.OperationID
			priorActive = true
		default:
			return fmt.Errorf("%w: rollback found an unrelated activation", ErrInstallConflict)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	priorRestored := false
	if receipt.Prior != nil && !fileExists(paths.prior) && fileExists(paths.canonical) {
		current, err := inspectInstalled(ctx, c.verifier, paths.canonical, receipt.Prior.Generation.Version, codeidentity.CodeIdentity{
			TeamID: receipt.Prior.Generation.TeamID, SigningIdentifier: receipt.Prior.Generation.SigningIdentifier,
		})
		priorRestored = err == nil && sameGenerationBytes(current, receipt.Prior.Generation)
		if !priorRestored {
			return fmt.Errorf("%w: rollback canonical generation is not the prior app", ErrInstallConflict)
		}
	}
	if !priorRestored && fileExists(paths.canonical) && (receipt.Prior == nil || fileExists(paths.prior)) {
		if fileExists(candidatePath) {
			return fmt.Errorf("%w: candidate occupied during rollback", ErrInstallConflict)
		}
		if err := os.Rename(paths.canonical, candidatePath); err != nil {
			return err
		}
		if err := daemon.SyncDir(filepath.Dir(paths.canonical)); err != nil {
			return err
		}
	}
	if receipt.Prior != nil {
		if !priorRestored {
			if !fileExists(paths.prior) || fileExists(paths.canonical) {
				return fmt.Errorf("%w: rollback prior generation is unavailable", ErrInstallState)
			}
			if err := os.Rename(paths.prior, paths.canonical); err != nil {
				return err
			}
			if err := daemon.SyncDir(filepath.Dir(paths.canonical)); err != nil {
				return err
			}
			if err := c.applyCheckpoint("apply:rollback_swapped"); err != nil {
				return err
			}
		}
		prior := receipt.Prior
		if err := c.attestStoredGeneration(ctx, prior.Generation); err != nil {
			return err
		}
		if !priorActive {
			plan, err := restorePlan(prior.Plan)
			if err != nil {
				return err
			}
			policy, err := ParseSHA256(prior.PolicyDigest)
			if err != nil {
				return err
			}
			activationConfig := ActivateInstalledConfig{
				Expected: InstalledAttestation{stored: prior.Generation}, ConsumerBuild: prior.ConsumerBuild,
				PolicyDigest: policy, Plan: plan, Readiness: config.Readiness,
			}
			validated, err := validateActivateConfig(activationConfig)
			if err != nil {
				return err
			}
			active, err := c.activateLocked(ctx, activationConfig, validated, paths)
			if err != nil {
				return err
			}
			receipt.RollbackOperation = active.OperationID()
		}
	}
	receipt.Phase = applyRolledBack
	if err := writeJSONDurable(paths.apply, receipt); err != nil {
		return err
	}
	return c.applyCheckpoint("apply:rolled_back")
}

func (c *Controller) applyCheckpoint(point string) error {
	if err := c.inject(point); err != nil {
		return applyCheckpointError{err: err}
	}
	return nil
}
