package deployment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/bundle"
)

func (c *Controller) verifyGeneration(ctx context.Context, expected storedGeneration) error {
	if err := requireRealDirectory(expected.Path); err != nil {
		return err
	}
	id, err := identifyPath(expected.Path)
	if err != nil {
		return fmt.Errorf("deployment: identify canonical generation: %w", err)
	}
	if id != expected.FileID {
		return fmt.Errorf("%w: canonical generation changed", ErrInstallConflict)
	}
	cdHash, err := c.verifyBundle(ctx, expected.Path, expected.Version, expected.DesignatedRequirement)
	if err != nil {
		return err
	}
	if cdHash != expected.CDHash {
		return fmt.Errorf("%w: canonical generation CDHash changed", ErrInstallConflict)
	}
	bundleDigest, err := bundleTreeDigest(expected.Path)
	if err != nil {
		return err
	}
	if bundleDigest.String() != expected.BundleDigest {
		return fmt.Errorf("%w: canonical generation bundle digest changed", ErrInstallConflict)
	}
	finalID, err := identifyPath(expected.Path)
	if err != nil {
		return fmt.Errorf("deployment: re-identify canonical generation: %w", err)
	}
	if finalID != expected.FileID {
		return fmt.Errorf("%w: canonical generation changed during verification", ErrInstallConflict)
	}
	return nil
}

func verifyGenerationIdentity(path string, expected fileID) error {
	id, err := identifyPath(path)
	if err != nil {
		return err
	}
	if id != expected {
		return fmt.Errorf("%w: generation identity changed at %q", ErrInstallConflict, path)
	}
	return requireRealDirectory(path)
}

func stageApp(paths deploymentPaths, tx *deploymentTransaction) string {
	return bundle.AppPath(filepath.Join(paths.metadataDir, tx.Stage), filepath.Base(paths.canonical[:len(paths.canonical)-len(".app")]))
}

func (c *Controller) publishCandidate(paths deploymentPaths, tx *deploymentTransaction) error {
	stageRoot := filepath.Join(paths.metadataDir, tx.Stage)
	staged := bundle.AppPath(stageRoot, filepath.Base(paths.canonical[:len(paths.canonical)-len(".app")]))
	canonicalParent, err := openVerifiedDirectory(filepath.Dir(paths.canonical))
	if err != nil {
		return err
	}
	defer canonicalParent.Close()
	stageParent, err := openVerifiedDirectory(stageRoot)
	if err != nil {
		return err
	}
	defer stageParent.Close()
	canonicalName := filepath.Base(paths.canonical)
	stageName := filepath.Base(staged)
	if err := verifyGenerationIdentity(staged, tx.Candidate.FileID); err != nil {
		return fmt.Errorf("deployment: staged candidate: %w", err)
	}
	if tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil {
		prior := tx.PriorReceipt.Current
		if prior == nil {
			return fmt.Errorf("%w: prior receipt has no generation", ErrInstallState)
		}
		if err := verifyGenerationIdentity(paths.canonical, prior.FileID); err != nil {
			return fmt.Errorf("deployment: prior canonical: %w", err)
		}
		exchange := c.exchange
		if exchange == nil {
			exchange = exchangePaths
		}
		if err := exchange(canonicalParent, canonicalName, stageParent, stageName); err != nil {
			return fmt.Errorf("deployment: exchange canonical bundle: %w", err)
		}
	} else {
		if _, err := os.Lstat(paths.canonical); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return fmt.Errorf("%w: canonical app appeared before first deployment", ErrInstallConflict)
			}
			return err
		}
		publish := c.publishFirst
		if publish == nil {
			publish = publishExclusive
		}
		if err := publish(stageParent, stageName, canonicalParent, canonicalName); err != nil {
			return fmt.Errorf("deployment: publish first canonical bundle: %w", err)
		}
	}
	if err := errors.Join(canonicalParent.Sync(), stageParent.Sync()); err != nil {
		return fmt.Errorf("deployment: persist canonical exchange: %w", err)
	}
	return verifyGenerationIdentity(paths.canonical, tx.Candidate.FileID)
}

func (c *Controller) restorePrior(paths deploymentPaths, tx *deploymentTransaction) error {
	stageRoot := filepath.Join(paths.metadataDir, tx.Stage)
	staged := bundle.AppPath(stageRoot, filepath.Base(paths.canonical[:len(paths.canonical)-len(".app")]))
	if err := verifyGenerationIdentity(paths.canonical, tx.Candidate.FileID); err != nil {
		return fmt.Errorf("deployment: rollback candidate canonical: %w", err)
	}
	canonicalParent, err := openVerifiedDirectory(filepath.Dir(paths.canonical))
	if err != nil {
		return err
	}
	defer canonicalParent.Close()
	stageParent, err := openVerifiedDirectory(stageRoot)
	if err != nil {
		return err
	}
	defer stageParent.Close()
	canonicalName := filepath.Base(paths.canonical)
	stageName := filepath.Base(staged)
	if tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil {
		prior := tx.PriorReceipt.Current
		if prior == nil {
			return fmt.Errorf("%w: prior receipt has no generation", ErrInstallState)
		}
		if err := verifyGenerationIdentity(staged, prior.FileID); err != nil {
			return fmt.Errorf("deployment: rollback prior stage: %w", err)
		}
		exchange := c.exchange
		if exchange == nil {
			exchange = exchangePaths
		}
		if err := exchange(canonicalParent, canonicalName, stageParent, stageName); err != nil {
			return fmt.Errorf("deployment: reverse canonical exchange: %w", err)
		}
	} else {
		if _, err := os.Lstat(staged); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: first-deployment rollback stage is occupied", ErrInstallConflict)
		}
		publish := c.publishFirst
		if publish == nil {
			publish = publishExclusive
		}
		if err := publish(canonicalParent, canonicalName, stageParent, stageName); err != nil {
			return fmt.Errorf("deployment: restore managed absence: %w", err)
		}
	}
	if err := errors.Join(canonicalParent.Sync(), stageParent.Sync()); err != nil {
		return fmt.Errorf("deployment: persist rollback exchange: %w", err)
	}
	if tx.PriorReceipt == nil || tx.PriorReceipt.Current == nil {
		if _, err := os.Lstat(paths.canonical); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: canonical remains after absence rollback", ErrInstallConflict)
		}
		return nil
	}
	return verifyGenerationIdentity(paths.canonical, tx.PriorReceipt.Current.FileID)
}

func exactNamespaceState(paths deploymentPaths, tx *deploymentTransaction) (canonical, staged *fileID, err error) {
	canonicalID, canonicalErr := identifyPath(paths.canonical)
	if canonicalErr == nil {
		canonical = &canonicalID
	} else if !errors.Is(canonicalErr, os.ErrNotExist) {
		return nil, nil, canonicalErr
	}
	if tx.Mode != modeReplace {
		return canonical, nil, nil
	}
	stageID, stageErr := identifyPath(stageApp(paths, tx))
	if stageErr == nil {
		staged = &stageID
	} else if !errors.Is(stageErr, os.ErrNotExist) {
		return nil, nil, stageErr
	}
	return canonical, staged, nil
}

func classifyNamespace(paths deploymentPaths, tx *deploymentTransaction) (string, error) {
	canonical, staged, err := exactNamespaceState(paths, tx)
	if err != nil {
		return "", err
	}
	var prior *fileID
	if tx.PriorReceipt != nil && tx.PriorReceipt.Current != nil {
		value := tx.PriorReceipt.Current.FileID
		prior = &value
	}
	match := func(got, want *fileID) bool {
		return (got == nil && want == nil) || (got != nil && want != nil && *got == *want)
	}
	candidate := tx.Candidate.FileID
	switch {
	case match(canonical, prior) && staged != nil && *staged == candidate:
		return "pre_swap", nil
	case canonical != nil && *canonical == candidate && match(staged, prior):
		return "post_swap", nil
	default:
		return "", errors.Join(ErrRecoveryRequired, ErrAmbiguousState)
	}
}

func cleanupJournaledStage(paths deploymentPaths, tx *deploymentTransaction, expected *fileID, allowAbsent bool) error {
	stageRoot := filepath.Join(paths.metadataDir, tx.Stage)
	staged := stageApp(paths, tx)
	if _, err := os.Lstat(stageRoot); errors.Is(err, os.ErrNotExist) {
		if allowAbsent {
			return nil
		}
		return fmt.Errorf("%w: journaled stage is absent before cleanup authorization", ErrInstallConflict)
	} else if err != nil {
		return err
	}
	if expected == nil {
		if _, err := os.Lstat(staged); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: unexpected staged generation during cleanup", ErrInstallConflict)
		}
	} else if err := verifyGenerationIdentity(staged, *expected); err != nil {
		return err
	}
	entries, err := os.ReadDir(stageRoot)
	if err != nil {
		return err
	}
	if expected == nil && len(entries) != 0 || expected != nil && (len(entries) != 1 || entries[0].Name() != filepath.Base(staged)) {
		return fmt.Errorf("%w: stage contains unjournaled residue", ErrInstallConflict)
	}
	if err := os.RemoveAll(stageRoot); err != nil {
		return err
	}
	return syncVerifiedDirectory(paths.metadataDir)
}
