package deployment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

type activationVerifier func(context.Context, string, string) (signatureAttestation, error)

func (f activationVerifier) Verify(ctx context.Context, path, requirement string) (signatureAttestation, error) {
	return f(ctx, path, requirement)
}

type activationServices struct {
	mu       sync.Mutex
	agents   map[string]service.Agent
	config   service.ControllerConfig
	closed   int
	converge int
	stops    int
}

func (s *activationServices) Converge(_ context.Context, agents []service.Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.converge++
	s.agents = make(map[string]service.Agent, len(agents))
	for _, agent := range agents {
		s.agents[agent.Label] = agent
	}
	return nil
}

func (s *activationServices) Status(_ context.Context, label string) (service.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.agents[label]
	return service.Status{Label: label, Desired: exists, Applied: exists, Loaded: exists, Exact: exists}, nil
}

func (s *activationServices) StopRuntime(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error) {
	s.mu.Lock()
	s.stops++
	s.mu.Unlock()
	return service.StopReceipt{}, nil
}

func (s *activationServices) Close(context.Context) error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}

type activationFixture struct {
	controller      *Controller
	services        *activationServices
	config          ActivateInstalledConfig
	spec            InstalledSpec
	appPath         string
	readiness       int
	entitlements    SHA256
	setEntitlements func(SHA256)
}

func newActivationFixture(t *testing.T) *activationFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	appPath := filepath.Join(root, "Helper.app")
	program := filepath.Join(appPath, "Contents", "MacOS", "Helper")
	if err := os.MkdirAll(filepath.Dir(program), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(program, []byte("helper"), 0o755); err != nil {
		t.Fatal(err)
	}
	info := `<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"><dict><key>CFBundleShortVersionString</key><string>1.0.0</string></dict></plist>`
	if err := os.WriteFile(filepath.Join(appPath, "Contents", "Info.plist"), []byte(info), 0o644); err != nil {
		t.Fatal(err)
	}
	entitlements := SHA256{1}
	verifiedEntitlements := entitlements
	plan, err := service.NewPlan([]service.Agent{{
		Label: "com.example.helper", Program: program,
		LogPath: filepath.Join(root, "helper.log"), RestartPolicy: service.RestartAlways,
	}})
	if err != nil {
		t.Fatal(err)
	}
	services := &activationServices{}
	controller := New()
	controller.verifier = activationVerifier(func(context.Context, string, string) (signatureAttestation, error) {
		return signatureAttestation{CDHash: "0123456789abcdef0123456789abcdef01234567", EntitlementsDigest: verifiedEntitlements}, nil
	})
	operation := 0
	controller.operationID = func() (string, error) {
		operation++
		return fmt.Sprintf("%064x", operation), nil
	}
	controller.openService = func(_ context.Context, config service.ControllerConfig) (serviceController, error) {
		services.config = config
		if err := os.WriteFile(config.StatePath, []byte("state"), 0o600); err != nil {
			return nil, err
		}
		if err := os.WriteFile(config.ProcessPath, []byte("process"), 0o600); err != nil {
			return nil, err
		}
		return services, nil
	}
	fixture := &activationFixture{
		controller: controller, services: services, appPath: appPath, entitlements: entitlements,
		setEntitlements: func(value SHA256) { verifiedEntitlements = value },
	}
	fixture.spec = InstalledSpec{
		AppPath: appPath, Version: "1.0.0",
		Identity: codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.Helper"},
	}
	attestation, err := controller.AttestInstalled(t.Context(), fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config = ActivateInstalledConfig{
		Expected: attestation, ConsumerBuild: "consumer-v1", PolicyDigest: SHA256{2}, Plan: plan,
		Readiness: func(context.Context, InstalledOperation) (ReadinessProof, error) {
			fixture.readiness++
			return NewReadinessProof("runtime-v1", proc.OwnerGeneration{1}, SHA256{3})
		},
	}
	return fixture
}

func TestStatusInstalledReportsVerifiedUnactivatedWithoutWrites(t *testing.T) {
	fixture := newActivationFixture(t)
	status, err := fixture.controller.StatusInstalled(t.Context(), fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	if status.State() != InstalledVerifiedUnactivated {
		t.Fatalf("state = %q", status.State())
	}
	if !reflect.DeepEqual(status.Attestation(), fixture.config.Expected) {
		t.Fatal("status did not return the exact attestation")
	}
	paths := deploymentPathsForApp(fixture.appPath)
	if _, err := os.Lstat(paths.metadataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status created metadata: %v", err)
	}
}

func TestActivateInstalledIsExactAndLostResponseIdempotent(t *testing.T) {
	fixture := newActivationFixture(t)
	fail := true
	fixture.controller.failpoint = func(point string) error {
		if point == "activate:active" && fail {
			fail = false
			return errors.New("lost response")
		}
		return nil
	}
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err == nil {
		t.Fatal("lost response was not surfaced")
	}
	first, err := readActivation(deploymentPathsForApp(fixture.appPath).activation)
	if err != nil || first.Phase != activationActive {
		t.Fatalf("durable active receipt = %#v, %v", first, err)
	}
	replayed, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.OperationID() != first.OperationID || len(replayed.OperationID()) != 64 || !replayed.Active() || fixture.readiness != 2 {
		t.Fatalf("replay = %#v, readiness = %d", replayed, fixture.readiness)
	}
	second, err := readActivation(deploymentPathsForApp(fixture.appPath).activation)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("receipt changed on replay: %#v != %#v (%v)", first, second, err)
	}
}

func TestActivateInstalledCrashCheckpointsReplayExactly(t *testing.T) {
	for _, point := range []string{"activate:prepared", "activate:converged", "activate:healthy", "activate:active"} {
		t.Run(point, func(t *testing.T) {
			fixture := newActivationFixture(t)
			failed := false
			fixture.controller.failpoint = func(got string) error {
				if got == point && !failed {
					failed = true
					return errors.New("crash")
				}
				return nil
			}
			if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err == nil {
				t.Fatal("crash checkpoint returned success")
			}
			receipt, err := readActivation(deploymentPathsForApp(fixture.appPath).activation)
			if err != nil {
				t.Fatal(err)
			}
			if point == "activate:active" && receipt.Phase != activationActive {
				t.Fatalf("phase = %q", receipt.Phase)
			}
			fixture.controller.failpoint = nil
			result, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config)
			if err != nil || !result.Active() || result.OperationID() != receipt.OperationID {
				t.Fatalf("replay = %#v, %v", result, err)
			}
		})
	}
}

func TestActivateInstalledRollbackNeverDeletesPackagedApp(t *testing.T) {
	fixture := newActivationFixture(t)
	fixture.config.Readiness = func(context.Context, InstalledOperation) (ReadinessProof, error) {
		return ReadinessProof{}, errors.New("not ready")
	}
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err == nil {
		t.Fatal("readiness failure returned success")
	}
	paths := deploymentPathsForApp(fixture.appPath)
	for _, path := range []string{paths.activation, paths.serviceState, paths.serviceProcess} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rollback retained %s: %v", path, err)
		}
	}
	if err := requireRealDirectory(fixture.appPath); err != nil {
		t.Fatalf("rollback removed packaged app: %v", err)
	}
}

func TestActivateInstalledRejectsConfigBytesEntitlementsAndInodeDrift(t *testing.T) {
	fixture := newActivationFixture(t)
	active, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if len(active.OperationID()) != 64 {
		t.Fatalf("operation ID = %q", active.OperationID())
	}
	changedPolicy := fixture.config
	changedPolicy.PolicyDigest = SHA256{9}
	if _, err := fixture.controller.ActivateInstalled(t.Context(), changedPolicy); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("policy conflict = %v", err)
	}
	program := filepath.Join(fixture.appPath, "Contents", "MacOS", "Helper")
	if err := os.WriteFile(program, []byte("changed"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("changed bytes = %v", err)
	}
	if err := os.WriteFile(program, []byte("helper"), 0o755); err != nil {
		t.Fatal(err)
	}
	fixture.setEntitlements(SHA256{9})
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("changed entitlements = %v", err)
	}
	fixture.setEntitlements(fixture.entitlements)
	prior := fixture.appPath + ".prior"
	if err := os.Rename(fixture.appPath, prior); err != nil {
		t.Fatal(err)
	}
	copyDirectory(t, prior, fixture.appPath)
	if _, err := fixture.controller.StatusInstalled(t.Context(), fixture.spec); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("inode drift = %v", err)
	}
}

func TestActivationStateRejectsLegacyShortOperationID(t *testing.T) {
	fixture := newActivationFixture(t)
	if _, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config); err != nil {
		t.Fatal(err)
	}
	path := deploymentPathsForApp(fixture.appPath).activation
	receipt, err := readActivation(path)
	if err != nil {
		t.Fatal(err)
	}
	receipt.OperationID = "00000000000000000000000000000001"
	if err := writeJSONDurable(path, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := readActivation(path); !errors.Is(err, ErrInstallState) {
		t.Fatalf("legacy operation ID = %v", err)
	}
}

func TestDeactivateCurrentInstalledUsesSealedActivationAndEnforcesUpgradeOrder(t *testing.T) {
	fixture := newActivationFixture(t)
	deactivate := DeactivateCurrentInstalledConfig{
		Current: CurrentInstalledSpec{AppPath: fixture.appPath, Identity: fixture.spec.Identity},
		RuntimeQuiesce: func(context.Context, RuntimeStopper, DeactivateInstalledOperation) (RuntimeProof, error) {
			return NewRuntimeProof(true, proc.OwnerGeneration{}, SHA256{8})
		},
	}
	if _, err := fixture.controller.DeactivateCurrentInstalled(t.Context(), deactivate); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("deactivate without receipt = %v", err)
	}
	active, err := fixture.controller.ActivateInstalled(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := fixture.controller.DeactivateCurrentInstalled(t.Context(), deactivate)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed.OperationID()) != 64 || removed.OperationID() == active.OperationID() || !removed.RuntimeProof().Absent() {
		t.Fatalf("deactivation = %#v", removed)
	}
	paths := deploymentPathsForApp(fixture.appPath)
	for _, path := range []string{paths.activation, paths.serviceState, paths.serviceProcess} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deactivation retained %s: %v", path, err)
		}
	}
	if err := requireRealDirectory(fixture.appPath); err != nil {
		t.Fatalf("deactivation removed app: %v", err)
	}
	replayed, err := fixture.controller.DeactivateCurrentInstalled(t.Context(), deactivate)
	if err != nil || replayed.OperationID() != removed.OperationID() {
		t.Fatalf("deactivation replay = %#v, %v", replayed, err)
	}
	if err := os.Rename(fixture.appPath, fixture.appPath+".old"); err != nil {
		t.Fatal(err)
	}
	copyDirectory(t, fixture.appPath+".old", fixture.appPath)
	nextAttestation, err := fixture.controller.AttestInstalled(t.Context(), fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	next := fixture.config
	next.Expected = nextAttestation
	nextActive, err := fixture.controller.ActivateInstalled(t.Context(), next)
	if err != nil {
		t.Fatalf("new packaged generation activation: %v", err)
	}
	if nextActive.OperationID() == active.OperationID() {
		t.Fatal("new packaged generation reused the prior operation ID")
	}
}

func copyDirectory(t *testing.T, source, target string) {
	t.Helper()
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(destination, payload, info.Mode().Perm())
	}); err != nil {
		t.Fatal(err)
	}
}
