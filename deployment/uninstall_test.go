package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestUninstallCurrentInstalledIsSealedAndLostResponseIdempotent(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
		t.Fatal(err)
	}
	config := uninstallConfig(fixture)
	first, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID() != second.OperationID() || first.DeactivationOperationID() != second.DeactivationOperationID() {
		t.Fatal("uninstall replay changed durable operation identity")
	}
	if first.Generation().Version() != "1.0.0" || !first.RuntimeProof().Absent() {
		t.Fatal("uninstall receipt lost generation or quiescence proof")
	}
	paths := deploymentPathsForApp(fixture.appPath)
	if fileExists(paths.canonical) || fileExists(paths.removed) || fileExists(paths.deactivation) {
		t.Fatal("uninstall retained app or deactivation state")
	}
}

func TestUninstallCurrentInstalledRecoversEveryCheckpoint(t *testing.T) {
	for _, point := range []string{
		"uninstall:prepared", "uninstall:moved_namespace", "uninstall:moved", "uninstall:removed_tree", "uninstall:removed",
	} {
		t.Run(point, func(t *testing.T) {
			fixture := newActivationFixture(t)
			if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
				t.Fatal(err)
			}
			failed := false
			fixture.controller.failpoint = func(got string) error {
				if got == point && !failed {
					failed = true
					return errors.New("simulated uninstall process death")
				}
				return nil
			}
			config := uninstallConfig(fixture)
			if _, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config); err == nil {
				t.Fatal("simulated uninstall process death was not returned")
			}
			fixture.controller.failpoint = nil
			receipt, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Generation().Version() != "1.0.0" {
				t.Fatalf("removed version = %q", receipt.Generation().Version())
			}
			paths := deploymentPathsForApp(fixture.appPath)
			if fileExists(paths.canonical) || fileExists(paths.removed) {
				t.Fatal("uninstall recovery retained namespace state")
			}
		})
	}
}

func TestUninstallCurrentInstalledRejectsUnsealedApp(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := fixture.controller.UninstallCurrentInstalled(t.Context(), uninstallConfig(fixture)); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v", err)
	}
	if !fileExists(fixture.appPath) {
		t.Fatal("unsealed app was removed")
	}
}

func TestApplyInstalledCandidateRetiresTerminalUninstall(t *testing.T) {
	fixture := newActivationFixture(t)
	source := filepath.Join(filepath.Dir(fixture.appPath), "Package.app")
	copyDirectory(t, fixture.appPath, source)
	digest, err := bundleTreeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.UninstallCurrentInstalled(t.Context(), uninstallConfig(fixture)); err != nil {
		t.Fatal(err)
	}
	apply := ApplyInstalledCandidateConfig{
		Target:              CurrentInstalledSpec{AppPath: fixture.appPath, Identity: fixture.spec.Identity},
		CandidateSourcePath: source, CandidateVersion: "1.0.0", CandidateBundleDigest: digest,
		ConsumerBuild: "consumer-v2", PolicyDigest: SHA256{4}, Plan: fixture.config.Plan,
		RuntimeQuiesce: uninstallConfig(fixture).RuntimeQuiesce, Readiness: fixture.config.Readiness,
	}
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), apply); err != nil {
		t.Fatal(err)
	}
	paths := deploymentPathsForApp(fixture.appPath)
	if !fileExists(paths.canonical) || fileExists(paths.uninstall) {
		t.Fatal("apply did not retire terminal uninstall")
	}
}

func uninstallConfig(fixture *activationFixture) UninstallCurrentInstalledConfig {
	return UninstallCurrentInstalledConfig{
		Current: CurrentInstalledSpec{AppPath: fixture.appPath, Identity: fixture.spec.Identity},
		RuntimeQuiesce: func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error) {
			return NewRuntimeProof(true, proc.OwnerGeneration{}, SHA256{8})
		},
		Readiness: fixture.config.Readiness,
	}
}

func TestUninstallCurrentInstalledRejectsSubstitutedRemovalSlot(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
		t.Fatal(err)
	}
	failed := false
	fixture.controller.failpoint = func(point string) error {
		if point == "uninstall:moved_namespace" && !failed {
			failed = true
			return errors.New("stop after namespace move")
		}
		return nil
	}
	config := uninstallConfig(fixture)
	if _, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config); err == nil {
		t.Fatal("namespace checkpoint did not stop")
	}
	fixture.controller.failpoint = nil
	program := filepath.Join(deploymentPathsForApp(fixture.appPath).removed, "Contents", "MacOS", "Helper")
	if err := os.WriteFile(program, []byte("substituted"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.UninstallCurrentInstalled(t.Context(), config); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v", err)
	}
}
