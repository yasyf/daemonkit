package deployment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// UninstallCurrentInstalledConfig removes one controller-sealed installed app.
type UninstallCurrentInstalledConfig struct {
	Current        CurrentInstalledSpec
	RuntimeQuiesce func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error)
	Readiness      func(context.Context, InstalledOperation) (ReadinessProof, error)
}

// UninstallReceipt proves one exact installed generation was removed.
type UninstallReceipt struct {
	operationID           string
	deactivationOperation string
	generation            InstalledAttestation
	runtime               RuntimeProof
}

// OperationID returns the durable uninstall operation identity.
func (r UninstallReceipt) OperationID() string { return r.operationID }

// DeactivationOperationID returns the exact quiescence operation.
func (r UninstallReceipt) DeactivationOperationID() string { return r.deactivationOperation }

// Generation returns the exact removed app generation.
func (r UninstallReceipt) Generation() InstalledAttestation { return r.generation }

// RuntimeProof returns the exact quiescence evidence.
func (r UninstallReceipt) RuntimeProof() RuntimeProof { return r.runtime }

// UninstallCurrentInstalled quiesces, deactivates, and removes the sealed current app.
func (c *Controller) UninstallCurrentInstalled(
	ctx context.Context,
	config UninstallCurrentInstalledConfig,
) (UninstallReceipt, error) {
	if err := validateCurrentInstalledSpec(config.Current); err != nil {
		return UninstallReceipt{}, err
	}
	if config.RuntimeQuiesce == nil || config.Readiness == nil || c == nil || c.verifier == nil ||
		c.openService == nil || c.operationID == nil {
		return UninstallReceipt{}, ErrInvalidConfig
	}
	paths := deploymentPathsForApp(config.Current.AppPath)
	if err := requireRealDirectory(paths.metadataDir); err != nil {
		return UninstallReceipt{}, fmt.Errorf("%w: sealed deployment state is required", ErrInstallConflict)
	}
	lock, err := (proc.FileLockSpec{Path: paths.lock, Mode: proc.FileLockExclusive, Deadline: activationLockDeadline}).Acquire(ctx)
	if err != nil {
		return UninstallReceipt{}, err
	}
	defer lock.Close()
	return c.uninstallCurrentLocked(ctx, config, paths)
}

func (c *Controller) uninstallCurrentLocked(
	ctx context.Context,
	config UninstallCurrentInstalledConfig,
	paths deploymentPaths,
) (UninstallReceipt, error) {
	receipt, err := readUninstall(paths.uninstall)
	if errors.Is(err, os.ErrNotExist) {
		if err := c.recoverApplyForDeactivation(ctx, DeactivateCurrentInstalledConfig(config), paths); err != nil {
			return UninstallReceipt{}, err
		}
		deactivated, err := c.deactivateCurrentLocked(ctx, DeactivateCurrentInstalledConfig(config), paths)
		if err != nil {
			return UninstallReceipt{}, err
		}
		tombstone, err := readDeactivation(paths.deactivation)
		if err != nil || tombstone.Phase != deactivationInactive || tombstone.OperationID != deactivated.OperationID() {
			return UninstallReceipt{}, fmt.Errorf("%w: exact inactive receipt is required", ErrInstallState)
		}
		operationID, err := c.operationID()
		if err != nil {
			return UninstallReceipt{}, err
		}
		if !validOperationID(operationID) {
			return UninstallReceipt{}, errors.New("deployment: operation ID source returned a noncanonical value")
		}
		receipt = &uninstallReceiptWire{
			Identity: uninstallIdentity, Schema: activationSchema, OperationID: operationID,
			DeactivationOperation: deactivated.OperationID(), Phase: uninstallPrepared,
			Generation: tombstone.Generation, RuntimeProof: *tombstone.RuntimeProof,
		}
		if err := writeJSONDurable(paths.uninstall, receipt); err != nil {
			return UninstallReceipt{}, err
		}
		if err := c.applyCheckpoint("uninstall:prepared"); err != nil {
			return UninstallReceipt{}, err
		}
	} else if err != nil {
		return UninstallReceipt{}, err
	}
	if !currentMatchesGeneration(config.Current, receipt.Generation) {
		return UninstallReceipt{}, fmt.Errorf("%w: uninstall receipt differs from current target", ErrInstallConflict)
	}
	if receipt.Phase == uninstallPrepared {
		if fileExists(paths.canonical) {
			if fileExists(paths.removed) {
				return UninstallReceipt{}, fmt.Errorf("%w: private removal slot is occupied", ErrInstallConflict)
			}
			if err := c.attestStoredGeneration(ctx, receipt.Generation); err != nil {
				return UninstallReceipt{}, err
			}
			if err := os.Rename(paths.canonical, paths.removed); err != nil {
				return UninstallReceipt{}, err
			}
			if err := daemon.SyncDir(filepath.Dir(paths.canonical)); err != nil {
				return UninstallReceipt{}, err
			}
			if err := c.applyCheckpoint("uninstall:moved_namespace"); err != nil {
				return UninstallReceipt{}, err
			}
		}
		if err := c.attestRemovedGeneration(ctx, receipt, paths); err != nil {
			return UninstallReceipt{}, err
		}
		receipt.Phase = uninstallMoved
		if err := writeJSONDurable(paths.uninstall, receipt); err != nil {
			return UninstallReceipt{}, err
		}
		if err := c.applyCheckpoint("uninstall:moved"); err != nil {
			return UninstallReceipt{}, err
		}
	}
	if receipt.Phase == uninstallMoved {
		if fileExists(paths.removed) {
			if err := c.attestRemovedGeneration(ctx, receipt, paths); err != nil {
				return UninstallReceipt{}, err
			}
			if err := removeTreeDurable(paths.removed); err != nil {
				return UninstallReceipt{}, err
			}
			if err := c.applyCheckpoint("uninstall:removed_tree"); err != nil {
				return UninstallReceipt{}, err
			}
		}
		if fileExists(paths.canonical) {
			return UninstallReceipt{}, fmt.Errorf("%w: canonical app returned during uninstall", ErrInstallConflict)
		}
		if err := errors.Join(
			removeIfExistsDurable(paths.activation), removeIfExistsDurable(paths.deactivation),
			removeIfExistsDurable(paths.serviceState), removeIfExistsDurable(paths.serviceProcess),
			removeIfExistsDurable(paths.apply), removeTreeDurable(paths.prior),
		); err != nil {
			return UninstallReceipt{}, err
		}
		receipt.Phase = uninstallRemoved
		if err := writeJSONDurable(paths.uninstall, receipt); err != nil {
			return UninstallReceipt{}, err
		}
		if err := c.applyCheckpoint("uninstall:removed"); err != nil {
			return UninstallReceipt{}, err
		}
	}
	if fileExists(paths.canonical) || fileExists(paths.removed) {
		return UninstallReceipt{}, fmt.Errorf("%w: uninstall receipt disagrees with namespace", ErrInstallState)
	}
	return receipt.public()
}

func (c *Controller) attestRemovedGeneration(ctx context.Context, receipt *uninstallReceiptWire, paths deploymentPaths) error {
	removed, err := inspectInstalled(ctx, c.verifier, paths.removed, receipt.Generation.Version, codeidentity.CodeIdentity{
		TeamID: receipt.Generation.TeamID, SigningIdentifier: receipt.Generation.SigningIdentifier,
	})
	if err != nil || !sameGenerationBytes(removed, receipt.Generation) {
		return fmt.Errorf("%w: private removed generation changed", ErrInstallConflict)
	}
	return nil
}

func (receipt uninstallReceiptWire) public() (UninstallReceipt, error) {
	digest, err := ParseSHA256(receipt.RuntimeProof.Digest)
	if err != nil {
		return UninstallReceipt{}, err
	}
	var generation proc.OwnerGeneration
	if receipt.RuntimeProof.ProcessGeneration != nil {
		generation = *receipt.RuntimeProof.ProcessGeneration
	}
	proof, err := NewRuntimeProof(receipt.RuntimeProof.Absent, generation, digest)
	if err != nil {
		return UninstallReceipt{}, err
	}
	return UninstallReceipt{
		operationID: receipt.OperationID, deactivationOperation: receipt.DeactivationOperation,
		generation: InstalledAttestation{stored: receipt.Generation}, runtime: proof,
	}, nil
}
