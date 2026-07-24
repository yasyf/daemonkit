package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestApplyInstalledCandidateFirstInstallIsExactAndIdempotent(t *testing.T) {
	fixture := newActivationFixture(t)
	config := newApplyConfig(t, fixture, "1.0.0", false)

	first, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID() != second.OperationID() || first.Activation().OperationID() != second.Activation().OperationID() {
		t.Fatal("lost-response replay changed durable operation identity")
	}
	if first.Activation().Generation().Version() != "1.0.0" {
		t.Fatalf("installed version = %q", first.Activation().Generation().Version())
	}
	if fileExists(mustCandidatePath(t, fixture.appPath)) {
		t.Fatal("private candidate remained after apply")
	}
}

func TestApplyInstalledCandidateUpgradeAndRollbackReactivation(t *testing.T) {
	fixture := newActivationFixture(t)
	prior, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	config := newApplyConfig(t, fixture, "2.0.0", true)
	config.Readiness = func(_ context.Context, operation InstalledOperation) (ReadinessProof, error) {
		if operation.Generation().Version() == "2.0.0" {
			return ReadinessProof{}, errors.New("new runtime failed readiness")
		}
		return NewReadinessProof("runtime-v1", proc.OwnerGeneration{1}, SHA256{3})
	}

	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); err == nil || !strings.Contains(err.Error(), "new runtime failed readiness") {
		t.Fatalf("upgrade error = %v", err)
	}
	active, err := readActivation(deploymentPathsForApp(fixture.appPath).activation)
	if err != nil {
		t.Fatal(err)
	}
	if active.OperationID == prior.OperationID() || active.Generation.Version != "1.0.0" {
		t.Fatalf("rollback activation = %#v", active)
	}
	if got, err := bundleVersion(fixture.appPath); err != nil || got != "1.0.0" {
		t.Fatalf("canonical version = %q, %v", got, err)
	}
	if got, err := bundleVersion(mustCandidatePath(t, fixture.appPath)); err != nil || got != "2.0.0" {
		t.Fatalf("retained candidate version = %q, %v", got, err)
	}
}

func TestApplyInstalledCandidateRecoversEveryForwardCheckpoint(t *testing.T) {
	for _, point := range []string{
		"apply:prepared", "apply:quiesced", "apply:candidate_moved", "apply:swapped", "apply:active",
	} {
		t.Run(point, func(t *testing.T) {
			fixture := newActivationFixture(t)
			config := newApplyConfig(t, fixture, "1.0.0", false)
			failed := false
			fixture.controller.failpoint = func(got string) error {
				if got == point && !failed {
					failed = true
					return errors.New("simulated process death")
				}
				return nil
			}
			if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); err == nil {
				t.Fatal("simulated process death was not returned")
			}
			fixture.controller.failpoint = nil
			result, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if result.Activation().Generation().Version() != "1.0.0" {
				t.Fatalf("recovered version = %q", result.Activation().Generation().Version())
			}
		})
	}
}

func TestApplyInstalledCandidateRecoversPriorMovedCheckpoint(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
		t.Fatal(err)
	}
	config := newApplyConfig(t, fixture, "2.0.0", true)
	failed := false
	fixture.controller.failpoint = func(point string) error {
		if point == "apply:prior_moved" && !failed {
			failed = true
			return errors.New("simulated process death")
		}
		return nil
	}
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); err == nil {
		t.Fatal("simulated process death was not returned")
	}
	fixture.controller.failpoint = nil
	result, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Activation().Generation().Version() != "2.0.0" {
		t.Fatalf("recovered version = %q", result.Activation().Generation().Version())
	}
}

func TestApplyInstalledCandidateRejectsUnsealedCurrentInstall(t *testing.T) {
	fixture := newActivationFixture(t)
	config := newApplyConfig(t, fixture, "2.0.0", true)
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v", err)
	}
	if got, err := bundleVersion(fixture.appPath); err != nil || got != "1.0.0" {
		t.Fatalf("unsealed current changed to %q, %v", got, err)
	}
}

func TestApplyInstalledCandidateRejectsSourceMutationAndCleansPartialStage(t *testing.T) {
	fixture := newActivationFixture(t)
	config := newApplyConfig(t, fixture, "1.0.0", false)
	mutated := false
	fixture.controller.failpoint = func(point string) error {
		if point == "apply:source_attested" && !mutated {
			mutated = true
			return os.WriteFile(filepath.Join(config.CandidateSourcePath, "Contents", "MacOS", "Helper"), []byte("substituted"), 0o755)
		}
		return nil
	}
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("error = %v", err)
	}
	assertNoCandidateStage(t, fixture.appPath)
}

func TestApplyInstalledCandidateRejectsEscapingSourceSymlink(t *testing.T) {
	fixture := newActivationFixture(t)
	config := newApplyConfig(t, fixture, "1.0.0", false)
	link := filepath.Join(config.CandidateSourcePath, "Contents", "escape")
	if err := os.Symlink("/etc/passwd", link); err != nil {
		t.Fatal(err)
	}
	digest, err := bundleTreeDigest(config.CandidateSourcePath)
	if err != nil {
		t.Fatal(err)
	}
	config.CandidateBundleDigest = digest
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); err == nil || !strings.Contains(err.Error(), "escapes its bundle") {
		t.Fatalf("error = %v", err)
	}
	assertNoCandidateStage(t, fixture.appPath)
}

func TestApplyInstalledCandidateCleansInterruptedPrivateCopy(t *testing.T) {
	fixture := newActivationFixture(t)
	config := newApplyConfig(t, fixture, "1.0.0", false)
	fixture.controller.failpoint = func(point string) error {
		if strings.HasPrefix(point, "apply:copied:") {
			return errors.New("copy interrupted")
		}
		return nil
	}
	if _, err := fixture.controller.ApplyInstalledCandidate(t.Context(), config); err == nil || !strings.Contains(err.Error(), "copy interrupted") {
		t.Fatalf("error = %v", err)
	}
	assertNoCandidateStage(t, fixture.appPath)
}

func newApplyConfig(t *testing.T, fixture *activationFixture, version string, preserveTarget bool) ApplyInstalledCandidateConfig {
	t.Helper()
	source := filepath.Join(filepath.Dir(fixture.appPath), "Package-"+strings.ReplaceAll(version, ".", "-")+".app")
	copyDirectory(t, fixture.appPath, source)
	if version != "1.0.0" {
		setBundleVersion(t, source, version)
	}
	if !preserveTarget {
		if err := os.RemoveAll(fixture.appPath); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := bundleTreeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	return ApplyInstalledCandidateConfig{
		Target:              CurrentInstalledSpec{AppPath: fixture.appPath, Identity: fixture.spec.Identity},
		CandidateSourcePath: source, CandidateVersion: version, CandidateBundleDigest: digest,
		ConsumerBuild: "consumer-v2", PolicyDigest: SHA256{4}, Plan: fixture.config.Plan,
		RuntimeQuiesce: func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error) {
			return NewRuntimeProof(true, proc.OwnerGeneration{}, SHA256{5})
		},
		Readiness: func(_ context.Context, operation InstalledOperation) (ReadinessProof, error) {
			return NewReadinessProof("runtime-"+operation.Generation().Version(), proc.OwnerGeneration{2}, SHA256{6})
		},
	}
}

func mustCandidatePath(t *testing.T, appPath string) string {
	t.Helper()
	path, err := installedCandidatePath(appPath)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func setBundleVersion(t *testing.T, appPath, version string) {
	t.Helper()
	path := filepath.Join(appPath, "Contents", "Info.plist")
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload = []byte(strings.Replace(string(payload), "1.0.0", version, 1))
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
}

func bundleVersion(appPath string) (string, error) {
	payload, err := os.ReadFile(filepath.Join(appPath, "Contents", "Info.plist"))
	if err != nil {
		return "", err
	}
	for _, version := range []string{"1.0.0", "2.0.0"} {
		if strings.Contains(string(payload), ">"+version+"<") {
			return version, nil
		}
	}
	return "", errors.New("version missing")
}

func assertNoCandidateStage(t *testing.T, appPath string) {
	t.Helper()
	paths := deploymentPathsForApp(appPath)
	entries, err := os.ReadDir(paths.metadataDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".candidate-") || entry.Name() == filepath.Base(mustCandidatePath(t, appPath)) {
			t.Fatalf("candidate staging residue: %s", entry.Name())
		}
	}
	if fileExists(mustCandidatePath(t, appPath)) {
		t.Fatal("published private candidate remained")
	}
}
