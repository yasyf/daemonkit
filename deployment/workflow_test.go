package deployment

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/service"
)

type verifierStub struct{}

func (verifierStub) Verify(context.Context, string, string) (string, error) {
	return strings.Repeat("a", 40), nil
}

type verifierFunc func(context.Context, string, string) (string, error)

func (f verifierFunc) Verify(ctx context.Context, path, requirement string) (string, error) {
	return f(ctx, path, requirement)
}

type closeHookController struct {
	deploymentController
	close func()
}

func (c closeHookController) Close(context.Context) error {
	c.close()
	return nil
}

type deploymentServiceStub struct {
	mu           sync.Mutex
	plan         service.Plan
	status       *service.ReplacementStatus
	completion   *replacementCompletion
	acknowledged *replacementCompletion
	stopRuntime  func(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error)
	events       []string
	close        int
}

func newDeploymentServiceStub(t *testing.T) *deploymentServiceStub {
	t.Helper()
	plan, err := service.NewPlan(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &deploymentServiceStub{plan: plan}
}

func (s *deploymentServiceStub) Snapshot(context.Context) (service.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return service.NewPlan(s.plan.Agents())
}

func (s *deploymentServiceStub) ReplacementStatus(context.Context) (*service.ReplacementStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil {
		return nil, nil
	}
	result := *s.status
	return &result, nil
}

func (s *deploymentServiceStub) DeploymentCompletion(context.Context) (*replacementCompletion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completion == nil {
		return nil, nil
	}
	result := *s.completion
	return &result, nil
}

func (s *deploymentServiceStub) DeploymentAcknowledgement(context.Context) (*replacementCompletion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acknowledged == nil {
		return nil, nil
	}
	result := *s.acknowledged
	return &result, nil
}

func (s *deploymentServiceStub) Quiesce(
	_ context.Context,
	id string,
	binding service.ReplacementBinding,
	expected service.Plan,
) (service.QuiesceReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil {
		if s.plan.Digest() != expected.Digest() {
			return service.QuiesceReceipt{}, service.ErrReplacementMismatch
		}
		s.status = &service.ReplacementStatus{
			OperationID: id, Binding: binding, Phase: service.ReplacementUnloaded,
			Epoch: 1, Prior: expected, Current: expected,
		}
	} else if s.status.OperationID != id || s.status.Binding != binding || s.status.Prior.Digest() != expected.Digest() {
		return service.QuiesceReceipt{}, service.ErrReplacementMismatch
	}
	s.events = append(s.events, "quiesce")
	return service.QuiesceReceipt{OperationID: id, Binding: binding, Epoch: s.status.Epoch, Plan: s.status.Current}, nil
}

func (s *deploymentServiceStub) ProveQuiesced(
	_ context.Context,
	receipt service.QuiesceReceipt,
	_ []string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil || s.status.OperationID != receipt.OperationID || s.status.Epoch != receipt.Epoch {
		return service.ErrReplacementMismatch
	}
	s.status.Phase = service.ReplacementQuiesced
	s.events = append(s.events, "proved-quiet")
	return nil
}

func (s *deploymentServiceStub) ApplyReplacement(
	_ context.Context,
	id string,
	binding service.ReplacementBinding,
	plan service.Plan,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil || s.status.OperationID != id || s.status.Binding != binding ||
		s.status.Phase != service.ReplacementQuiesced {
		return service.ErrReplacementMismatch
	}
	s.plan = plan
	s.status.Current = plan
	s.status.Phase = service.ReplacementRunningOwned
	s.events = append(s.events, "apply")
	return nil
}

func (s *deploymentServiceStub) Requiesce(
	_ context.Context,
	id string,
	binding service.ReplacementBinding,
) (service.QuiesceReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil || s.status.OperationID != id || s.status.Binding != binding {
		return service.QuiesceReceipt{}, service.ErrReplacementMismatch
	}
	if s.status.Phase == service.ReplacementRunningOwned {
		s.status.Phase = service.ReplacementUnloaded
		s.status.Epoch++
	}
	s.events = append(s.events, "requiesce")
	return service.QuiesceReceipt{OperationID: id, Binding: binding, Epoch: s.status.Epoch, Plan: s.status.Current}, nil
}

func (s *deploymentServiceStub) RestoreReplacement(
	_ context.Context,
	id string,
	binding service.ReplacementBinding,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil || s.status.OperationID != id || s.status.Binding != binding ||
		s.status.Phase != service.ReplacementQuiesced {
		return service.ErrReplacementMismatch
	}
	s.plan = s.status.Prior
	s.status.Current = s.status.Prior
	s.status.Phase = service.ReplacementRunningOwned
	s.events = append(s.events, "restore")
	return nil
}

func (s *deploymentServiceStub) CommitDeploymentReplacement(
	_ context.Context,
	id string,
	binding service.ReplacementBinding,
	expected service.Plan,
) (replacementCompletion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completion != nil {
		if s.completion.OperationID != id || s.completion.Binding != binding ||
			s.completion.Next.Digest() != expected.Digest() {
			return replacementCompletion{}, service.ErrReplacementCommitPending
		}
		return *s.completion, nil
	}
	if s.status == nil || s.status.OperationID != id || s.status.Binding != binding ||
		s.status.Phase != service.ReplacementRunningOwned || s.status.Current.Digest() != expected.Digest() {
		return replacementCompletion{}, service.ErrReplacementMismatch
	}
	commit := replacementCompletion{OperationID: id, Binding: binding, Prior: s.status.Prior, Next: expected}
	s.plan = expected
	s.status = nil
	s.completion = &commit
	s.events = append(s.events, "commit")
	return commit, nil
}

func (s *deploymentServiceStub) AcknowledgeDeploymentReplacement(_ context.Context, commit replacementCompletion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completion == nil || s.completion.OperationID != commit.OperationID ||
		s.completion.Binding != commit.Binding || s.completion.Prior.Digest() != commit.Prior.Digest() ||
		s.completion.Next.Digest() != commit.Next.Digest() {
		return service.ErrReplacementMismatch
	}
	s.completion = nil
	acknowledged := commit
	s.acknowledged = &acknowledged
	s.events = append(s.events, "ack")
	return nil
}

func (s *deploymentServiceStub) StopRuntime(
	ctx context.Context,
	request service.StopRuntimeRequest,
) (service.StopReceipt, error) {
	if s.stopRuntime != nil {
		return s.stopRuntime(ctx, request)
	}
	return service.StopReceipt{}, nil
}

func (s *deploymentServiceStub) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.close++
	return nil
}

func appArchive(t *testing.T, app, version, payload string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	files := map[string]string{
		filepath.Join(app+".app", "Contents", "Info.plist"): fmt.Sprintf(
			`<plist><dict><key>CFBundleShortVersionString</key><string>%s</string></dict></plist>`, version,
		),
		filepath.Join(app+".app", "Contents", "MacOS", app): payload,
	}
	for name, body := range files {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		if strings.Contains(name, "/MacOS/") {
			header.SetMode(0o755)
		} else {
			header.SetMode(0o644)
		}
		entry, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func releaseFixture(t *testing.T, archive []byte) (Release, *http.Client, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = writer.Write(archive)
	}))
	t.Cleanup(server.Close)
	digest := sha256.Sum256(archive)
	return Release{Version: archiveVersion(t, archive), URL: server.URL + "/app.zip", SHA256: digest}, server.Client(), &hits
}

func archiveVersion(t *testing.T, archive []byte) string {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range reader.File {
		if filepath.Base(file.Name) != "Info.plist" {
			continue
		}
		body, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		var data bytes.Buffer
		_, _ = data.ReadFrom(body)
		_ = body.Close()
		text := data.String()
		start := strings.Index(text, "<string>") + len("<string>")
		end := strings.Index(text[start:], "</string>")
		return text[start : start+end]
	}
	t.Fatal("archive has no Info.plist")
	return ""
}

func testController(client *http.Client, services *deploymentServiceStub) *Controller {
	controller := New()
	var operation atomic.Uint64
	controller.client = client
	controller.verifier = verifierStub{}
	controller.openController = func(context.Context, service.ControllerConfig) (deploymentController, error) {
		return services, nil
	}
	controller.operationID = func() (string, error) { return fmt.Sprintf("%032x", operation.Add(1)), nil }
	return controller
}

func realDeploymentController(client *http.Client) *Controller {
	controller := New()
	controller.client = client
	controller.verifier = verifierStub{}
	return controller
}

func testConfig(t *testing.T, dir string, release Release) (*Config, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var err error
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	var proofs atomic.Int64
	var readiness atomic.Int64
	policy := sha256.Sum256([]byte("policy-v1"))
	proofDigest := sha256.Sum256([]byte("proof-v1"))
	cfg := &Config{
		Dir: dir, AppName: "Helper", Release: release,
		Identity:      codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.Helper"},
		ConsumerBuild: "com.example.updater/callback-schema-v1/build-abc", PolicyDigest: policy,
		RuntimeQuiesce: func(_ context.Context, _ RuntimeStopper, operation RuntimeQuiesceOperation) (RuntimeProof, error) {
			return RuntimeProof{Role: operation.Role, Absent: true, Digest: proofDigest}, nil
		},
		PostInstallProof: func(_ context.Context, operation Operation) (Proof, error) {
			proofs.Add(1)
			return Proof{Role: operation.Role, PlanDigest: operation.PlanDigest, Digest: proofDigest}, nil
		},
		PriorAppRestoreProof: func(_ context.Context, operation Operation) (Proof, error) {
			return Proof{Role: operation.Role, PlanDigest: operation.PlanDigest, Digest: proofDigest}, nil
		},
		BuildPlan: func(_ context.Context, operation Operation) (service.Plan, error) {
			return service.NewPlan([]service.Agent{{
				Label: "com.example.helper", Program: filepath.Join(operation.Generation.Path, "Contents", "MacOS", "Helper"),
				LogPath: filepath.Join(dir, "helper.log"), RestartPolicy: service.RestartAlways,
			}})
		},
		Readiness: func(_ context.Context, operation Operation, _ service.Plan) (Proof, error) {
			readiness.Add(1)
			return Proof{Role: operation.Role, PlanDigest: operation.PlanDigest, Digest: proofDigest}, nil
		},
	}
	return cfg, &proofs, &readiness
}

func emptyServiceConfig(t *testing.T, dir string, release Release) Config {
	t.Helper()
	cfg, _, _ := testConfig(t, dir, release)
	cfg.BuildPlan = func(context.Context, Operation) (service.Plan, error) {
		return service.NewPlan(nil)
	}
	return *cfg
}

func TestPublicResultTypesExposeNoMutableFields(t *testing.T) {
	for _, resultType := range []reflect.Type{
		reflect.TypeFor[DeploymentReceipt](),
		reflect.TypeFor[DeactivationResult](),
		reflect.TypeFor[RecoveryResult](),
		reflect.TypeFor[DeploymentStatus](),
	} {
		for index := 0; index < resultType.NumField(); index++ {
			if resultType.Field(index).IsExported() {
				t.Fatalf("%s exposes mutable field %s", resultType, resultType.Field(index).Name)
			}
		}
	}

	empty, err := service.NewPlan(nil)
	if err != nil {
		t.Fatal(err)
	}
	generation := CanonicalGeneration{Path: "/Applications/Helper.app"}
	receipt := completedDeploymentReceipt("00000000000000000000000000000001", DeploymentInactive, generation, empty, empty)
	if receipt.OperationID() == "" || receipt.State() != DeploymentInactive || receipt.Plan().Digest() != empty.Digest() ||
		receipt.ActivationPlan().Digest() != empty.Digest() || receipt.Failure() != "" {
		t.Fatalf("receipt accessors returned inconsistent values")
	}
	if current, ok := receipt.Current(); !ok || current.Path != generation.Path {
		t.Fatalf("receipt Current = %#v, %v", current, ok)
	}
	deactivation := inactiveDeactivationResult(receipt)
	if deactivation.State() != DeactivationInactive {
		t.Fatalf("deactivation state = %q", deactivation.State())
	}
	if retained, ok := deactivation.Receipt(); !ok || retained.OperationID() != receipt.OperationID() {
		t.Fatalf("deactivation receipt = %#v, %v", retained, ok)
	}
	recovery := completedRecoveryResult(receipt)
	if recovery.State() != RecoveryInactive {
		t.Fatalf("recovery state = %q", recovery.State())
	}
	if recovered, ok := recovery.Receipt(); !ok || recovered.OperationID() != receipt.OperationID() {
		t.Fatalf("recovery receipt = %#v, %v", recovered, ok)
	}
	status := DeploymentStatus{operationID: receipt.OperationID(), receipt: &receipt, canonical: &generation, configMatches: true}
	if status.OperationID() != receipt.OperationID() || !status.ConfigMatches() || status.RecoveryRequired() {
		t.Fatalf("status scalar accessors returned inconsistent values")
	}
	if observed, ok := status.Receipt(); !ok || observed.OperationID() != receipt.OperationID() {
		t.Fatalf("status receipt = %#v, %v", observed, ok)
	}
	if canonical, ok := status.Canonical(); !ok || canonical.Path != generation.Path {
		t.Fatalf("status canonical = %#v, %v", canonical, ok)
	}
}

func deactivateConfig(cfg Config) DeactivateConfig {
	return DeactivateConfig{
		Dir: cfg.Dir, AppName: cfg.AppName, Identity: cfg.Identity,
		ConsumerBuild: cfg.ConsumerBuild, PolicyDigest: cfg.PolicyDigest,
		RuntimeQuiesce: cfg.RuntimeQuiesce, Readiness: cfg.Readiness,
	}
}

func TestDeployFreshAndRepeatRepairsProofAndReadinessWithoutSwap(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, hits := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, proofs, readiness := testConfig(t, dir, release)
	first, err := controller.Deploy(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first.state != DeploymentActive || first.current == nil || first.current.Release != release {
		t.Fatalf("first receipt = %#v", first)
	}
	firstID := fileID{Device: first.current.Device, Inode: first.current.Inode}
	second, err := controller.Deploy(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if (fileID{Device: second.current.Device, Inode: second.current.Inode}) != firstID {
		t.Fatal("repeat deployment exchanged the canonical app")
	}
	if hits.Load() != 1 || proofs.Load() != 2 || readiness.Load() != 2 {
		t.Fatalf("hits/proofs/readiness = %d/%d/%d, want 1/2/2", hits.Load(), proofs.Load(), readiness.Load())
	}
	if _, err := os.Stat(deploymentPathsFor(*cfg).transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction remains: %v", err)
	}
}

func TestDeployRejectsProgramsOutsideCanonicalGenerationBeforeServiceApply(t *testing.T) {
	for _, test := range []struct {
		name    string
		program func(string, string) string
	}{
		{name: "empty", program: func(string, string) string { return "" }},
		{name: "relative", program: func(string, string) string { return "bin/helper" }},
		{name: "outside", program: func(_ string, outside string) string { return outside }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			priorArchive := appArchive(t, "Helper", "1.0.0", "prior")
			priorRelease, priorClient, _ := releaseFixture(t, priorArchive)
			services := newDeploymentServiceStub(t)
			controller := testController(priorClient, services)
			priorConfig, _, _ := testConfig(t, dir, priorRelease)
			priorReceipt, err := controller.Deploy(t.Context(), *priorConfig)
			if err != nil {
				t.Fatal(err)
			}
			priorCanonical, err := os.ReadFile(filepath.Join(
				bundle.AppPath(priorConfig.Dir, priorConfig.AppName), "Contents", "MacOS", "Helper",
			))
			if err != nil {
				t.Fatal(err)
			}

			candidateArchive := appArchive(t, "Helper", "2.0.0", "candidate")
			candidateRelease, candidateClient, _ := releaseFixture(t, candidateArchive)
			controller.client = candidateClient
			cfg, _, _ := testConfig(t, dir, candidateRelease)
			paths := deploymentPathsFor(*cfg)
			outside := filepath.Join(dir, "outside-helper")
			if err := os.WriteFile(outside, []byte("outside"), 0o700); err != nil {
				t.Fatal(err)
			}

			var receiptAtRejection, canonicalAtRejection []byte
			var rejectedPhase Phase
			cfg.BuildPlan = func(_ context.Context, operation Operation) (service.Plan, error) {
				tx, err := readDeploymentTransaction(paths.transaction)
				if err != nil {
					return service.Plan{}, err
				}
				rejectedPhase = tx.Phase
				receiptAtRejection, err = os.ReadFile(paths.receipt)
				if err != nil {
					return service.Plan{}, err
				}
				canonicalAtRejection, err = os.ReadFile(filepath.Join(paths.canonical, "Contents", "MacOS", "Helper"))
				if err != nil {
					return service.Plan{}, err
				}
				return service.NewPlan([]service.Agent{{
					Label: "com.example.helper", Program: test.program(operation.Generation.Path, outside),
					LogPath: filepath.Join(dir, "helper.log"), RestartPolicy: service.RestartAlways,
				}})
			}

			services.mu.Lock()
			beforeEvents := len(services.events)
			services.mu.Unlock()
			if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
				t.Fatal("Deploy accepted an invalid service Program")
			}
			if rejectedPhase != PhaseCandidateProved {
				t.Fatalf("BuildPlan transaction phase = %q, want %q", rejectedPhase, PhaseCandidateProved)
			}
			if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid Program rollback left transaction residue: %v", err)
			}
			afterReceipt, err := os.ReadFile(paths.receipt)
			if err != nil || !bytes.Equal(afterReceipt, receiptAtRejection) {
				t.Fatalf("invalid Program mutated prior receipt: %v", err)
			}
			persistedReceipt, err := readDeploymentReceipt(paths.receipt)
			if err != nil || persistedReceipt.LastOperation.OperationID != priorReceipt.operationID {
				t.Fatalf("persisted prior receipt = %#v, %v", persistedReceipt, err)
			}
			afterCanonical, err := os.ReadFile(filepath.Join(paths.canonical, "Contents", "MacOS", "Helper"))
			if err != nil || !bytes.Equal(afterCanonical, priorCanonical) || bytes.Equal(afterCanonical, canonicalAtRejection) {
				t.Fatalf("invalid Program did not restore exact prior canonical generation: %v", err)
			}
			services.mu.Lock()
			events := append([]string(nil), services.events[beforeEvents:]...)
			status := services.status
			servicePlan := services.plan
			services.mu.Unlock()
			if slices.Contains(events, "apply") {
				t.Fatalf("service Apply ran for invalid Program: %v", events)
			}
			if status != nil || servicePlan.Digest() != priorReceipt.plan.Digest() {
				t.Fatalf("invalid Program rollback left service state: status=%#v plan=%s", status, servicePlan.Digest())
			}
			assertDeploymentResidueClean(t, paths)

			cfg.BuildPlan = func(_ context.Context, operation Operation) (service.Plan, error) {
				return service.NewPlan([]service.Agent{{
					Label:   "com.example.helper",
					Program: filepath.Join(operation.Generation.Path, "Contents", "MacOS", "Helper"),
					LogPath: filepath.Join(dir, "helper.log"), RestartPolicy: service.RestartAlways,
				}})
			}
			result, err := controller.Deploy(t.Context(), *cfg)
			if err != nil || result.state != DeploymentActive || result.current == nil || result.current.Release != candidateRelease {
				t.Fatalf("Deploy with canonical Program = %#v, %v", result, err)
			}
			assertDeploymentResidueClean(t, paths)
		})
	}
}

func TestDeactivateAndDeployReactivateWithoutDownloadOrSwap(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, hits := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	var runtimeStops atomic.Int64
	var builds atomic.Int64
	proofDigest := sha256.Sum256([]byte("runtime-proof"))
	buildPlan := cfg.BuildPlan
	cfg.BuildPlan = func(ctx context.Context, operation Operation) (service.Plan, error) {
		builds.Add(1)
		return buildPlan(ctx, operation)
	}
	cfg.RuntimeQuiesce = func(_ context.Context, _ RuntimeStopper, operation RuntimeQuiesceOperation) (RuntimeProof, error) {
		runtimeStops.Add(1)
		return RuntimeProof{Role: operation.Role, Absent: true, Digest: proofDigest}, nil
	}
	active, err := controller.Deploy(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	activeID := fileID{Device: active.current.Device, Inode: active.current.Inode}
	deactivated, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg))
	if err != nil {
		t.Fatal(err)
	}
	if deactivated.state != DeactivationInactive || deactivated.receipt == nil {
		t.Fatalf("deactivation result = %#v", deactivated)
	}
	inactive := *deactivated.receipt
	if inactive.state != DeploymentInactive || len(inactive.plan.Agents()) != 0 ||
		!samePlan(inactive.activationPlan, active.plan) {
		t.Fatalf("inactive receipt = %#v", inactive)
	}
	paths := deploymentPathsFor(*cfg)
	if id, err := identifyPath(paths.canonical); err != nil || id != activeID {
		t.Fatalf("deactivate changed installed app: %v, %v", id, err)
	}
	if _, err := readDeploymentReceipt(paths.receipt); err != nil {
		t.Fatalf("deactivate did not retain receipt: %v", err)
	}
	again, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg))
	if err != nil || again.state != DeactivationInactive || again.receipt == nil {
		t.Fatal(err)
	}
	reactivated, err := controller.Deploy(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if reactivated.state != DeploymentActive ||
		(fileID{Device: reactivated.current.Device, Inode: reactivated.current.Inode}) != activeID {
		t.Fatalf("reactivated receipt = %#v", reactivated)
	}
	if hits.Load() != 1 {
		t.Fatalf("download hits = %d, want 1", hits.Load())
	}
	if builds.Load() != 1 || !samePlan(reactivated.plan, active.plan) {
		t.Fatalf("reactivation rebuilt or changed saved plan: builds=%d", builds.Load())
	}
	if runtimeStops.Load() != 1 {
		t.Fatalf("runtime quiesce calls = %d, want deactivate only", runtimeStops.Load())
	}
	if services.completion != nil {
		t.Fatal("service completion remains after acknowledgement")
	}
}

func TestDeactivateNeverInstalledIsZeroWriteAbsent(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int64
	policy := sha256.Sum256([]byte("policy"))
	result, err := New().Deactivate(t.Context(), DeactivateConfig{
		Dir: dir, AppName: "Helper",
		Identity:      codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.Helper"},
		ConsumerBuild: "callback-v1", PolicyDigest: policy,
		RuntimeQuiesce: func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error) {
			callbacks.Add(1)
			return RuntimeProof{}, errors.New("must not run")
		},
		Readiness: func(context.Context, Operation, service.Plan) (Proof, error) {
			callbacks.Add(1)
			return Proof{}, errors.New("must not run")
		},
	})
	if err != nil || result.state != DeactivationAbsent || result.receipt != nil {
		t.Fatalf("Deactivate = %#v, %v", result, err)
	}
	if callbacks.Load() != 0 {
		t.Fatal("absent deactivation invoked callback")
	}
	if _, err := os.Lstat(filepath.Join(dir, ".daemonkit-deployment")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent deactivation created state: %v", err)
	}
}

func TestDeactivateRejectsUnreceiptedCanonicalAndPartialMetadata(t *testing.T) {
	for _, state := range []string{"canonical", "metadata"} {
		t.Run(state, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if state == "canonical" {
				if err := os.Mkdir(bundle.AppPath(dir, "Helper"), 0o700); err != nil {
					t.Fatal(err)
				}
			} else if err := os.MkdirAll(filepath.Join(dir, ".daemonkit-deployment", "Helper"), 0o700); err != nil {
				t.Fatal(err)
			}
			policy := sha256.Sum256([]byte("policy"))
			_, err = New().Deactivate(t.Context(), DeactivateConfig{
				Dir: dir, AppName: "Helper",
				Identity:      codeidentity.CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.Helper"},
				ConsumerBuild: "callback-v1", PolicyDigest: policy,
				RuntimeQuiesce: func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error) {
					return RuntimeProof{}, errors.New("must not run")
				},
				Readiness: func(context.Context, Operation, service.Plan) (Proof, error) {
					return Proof{}, errors.New("must not run")
				},
			})
			if state == "canonical" && !errors.Is(err, ErrInstallConflict) {
				t.Fatalf("Deactivate error = %v, want ErrInstallConflict", err)
			}
			if state == "metadata" && !errors.Is(err, ErrInstallState) {
				t.Fatalf("Deactivate error = %v, want ErrInstallState", err)
			}
		})
	}
}

func TestDeactivateAllowsNewConsumerBuildWithSamePolicy(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, hits := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	var builds atomic.Int64
	buildPlan := cfg.BuildPlan
	cfg.BuildPlan = func(ctx context.Context, operation Operation) (service.Plan, error) {
		builds.Add(1)
		return buildPlan(ctx, operation)
	}
	active, err := controller.Deploy(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	deactivate := deactivateConfig(*cfg)
	deactivate.ConsumerBuild = "com.example.updater/callback-schema-v1/build-new"
	deactivated, err := controller.Deactivate(t.Context(), deactivate)
	if err != nil || deactivated.state != DeactivationInactive || deactivated.receipt == nil {
		t.Fatalf("Deactivate = %#v, %v", deactivated, err)
	}
	inactive := *deactivated.receipt
	if hits.Load() != 1 || inactive.current.Device != active.current.Device || inactive.current.Inode != active.current.Inode {
		t.Fatal("new consumer build fetched or replaced the installed artifact")
	}
	cfg.ConsumerBuild = deactivate.ConsumerBuild
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	if builds.Load() != 2 {
		t.Fatalf("changed consumer build did not rebuild activation plan: builds=%d", builds.Load())
	}
}

func TestDeactivateRejectsChangedCompletedPolicyBeforeCallback(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int64
	deactivate := deactivateConfig(*cfg)
	deactivate.PolicyDigest = sha256.Sum256([]byte("different-policy"))
	deactivate.RuntimeQuiesce = func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error) {
		callbacks.Add(1)
		return RuntimeProof{}, errors.New("must not run")
	}
	if _, err := controller.Deactivate(t.Context(), deactivate); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Deactivate error = %v, want ErrInvalidConfig", err)
	}
	if callbacks.Load() != 0 {
		t.Fatal("policy mismatch invoked RuntimeQuiesce")
	}
	if _, err := os.Lstat(deploymentPathsFor(*cfg).transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("policy mismatch created transaction: %v", err)
	}
}

func TestDeactivateMissingInstallDirIsAbsentWithoutWrites(t *testing.T) {
	root := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, root, release)
	missing := filepath.Join(cfg.Dir, "missing", "Applications")
	deactivate := deactivateConfig(*cfg)
	deactivate.Dir = missing

	result, err := controller.Deactivate(t.Context(), deactivate)
	if err != nil || result.state != DeactivationAbsent || result.receipt != nil {
		t.Fatalf("Deactivate = %#v, %v; want absent", result, err)
	}
	if _, err := os.Lstat(filepath.Join(cfg.Dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Deactivate created missing install path: %v", err)
	}
}

func TestDeactivateMissingInstallDirRejectsSymlinkAncestor(t *testing.T) {
	root := t.TempDir()
	var err error
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, root, release)
	deactivate := deactivateConfig(*cfg)
	deactivate.Dir = filepath.Join(link, "missing")

	if _, err := controller.Deactivate(t.Context(), deactivate); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Deactivate error = %v, want ErrInvalidConfig", err)
	}
	if _, err := os.Lstat(filepath.Join(target, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Deactivate followed symlink ancestor: %v", err)
	}
}

func TestRecoverReportsRecoveryRequiredWithoutCallbacksOrMutation(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	cfg.ConsumerBuild += "/new"
	controller.failpoint = func(name string) error {
		if name == string(PhasePrepared) {
			return errors.New("simulated crash")
		}
		return nil
	}
	if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
		t.Fatal("Deploy succeeded across simulated crash")
	}
	paths := deploymentPathsFor(*cfg)
	tx, err := readDeploymentTransaction(paths.transaction)
	if err != nil {
		t.Fatal(err)
	}
	tx.Phase = PhaseRecoveryRequired
	tx.Failure = "ambiguous test state"
	if err := syncDeploymentState(paths.transaction, tx); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(paths.transaction)
	if err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int64
	cfg.RuntimeQuiesce = func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error) {
		callbacks.Add(1)
		return RuntimeProof{}, errors.New("must not run")
	}
	cfg.PostInstallProof = func(context.Context, Operation) (Proof, error) {
		callbacks.Add(1)
		return Proof{}, errors.New("must not run")
	}
	cfg.PriorAppRestoreProof = cfg.PostInstallProof
	cfg.BuildPlan = func(context.Context, Operation) (service.Plan, error) {
		callbacks.Add(1)
		return service.Plan{}, errors.New("must not run")
	}
	cfg.Readiness = func(context.Context, Operation, service.Plan) (Proof, error) {
		callbacks.Add(1)
		return Proof{}, errors.New("must not run")
	}
	controller.failpoint = nil
	result, err := controller.Recover(t.Context(), *cfg)
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, ErrAmbiguousState) {
		t.Fatalf("Recover error = %v, want recovery-required ambiguous state", err)
	}
	if result.state != RecoveryRequired || result.receipt == nil || result.receipt.state != DeploymentRecoveryRequired {
		t.Fatalf("Recover = %#v, want recovery-required", result)
	}
	if callbacks.Load() != 0 {
		t.Fatalf("Recover invoked %d callbacks", callbacks.Load())
	}
	after, err := os.ReadFile(paths.transaction)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("Recover mutated recovery-required transaction: %v", err)
	}
}

func TestDeactivateRejectsActiveTransactionCallbackMismatch(t *testing.T) {
	for _, mismatch := range []string{"consumer_build", "policy_digest"} {
		t.Run(mismatch, func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			cfg, _, _ := testConfig(t, dir, release)
			if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
				t.Fatal(err)
			}
			controller.failpoint = func(got string) error {
				if got == string(PhasePrepared) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg)); err == nil {
				t.Fatal("Deactivate succeeded across simulated crash")
			}
			controller.failpoint = nil
			paths := deploymentPathsFor(*cfg)
			before, err := readDeploymentTransaction(paths.transaction)
			if err != nil {
				t.Fatal(err)
			}
			var callbacks atomic.Int64
			changed := deactivateConfig(*cfg)
			changed.RuntimeQuiesce = func(context.Context, RuntimeStopper, RuntimeQuiesceOperation) (RuntimeProof, error) {
				callbacks.Add(1)
				return RuntimeProof{}, errors.New("must not run")
			}
			if mismatch == "consumer_build" {
				changed.ConsumerBuild += "/new"
			} else {
				changed.PolicyDigest = sha256.Sum256([]byte("new-policy"))
			}
			if _, err := controller.Deactivate(t.Context(), changed); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("Deactivate error = %v, want ErrRecoveryRequired", err)
			}
			after, err := readDeploymentTransaction(paths.transaction)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("active transaction mutated: %v", err)
			}
			if callbacks.Load() != 0 {
				t.Fatal("active transaction mismatch invoked RuntimeQuiesce")
			}
		})
	}
}

func TestDeactivateTransactionRejectsReceiptIdentityDrift(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	controller.failpoint = func(name string) error {
		if name == string(PhasePrepared) {
			return errors.New("simulated crash")
		}
		return nil
	}
	if _, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg)); err == nil {
		t.Fatal("Deactivate succeeded across simulated crash")
	}
	tx, err := readDeploymentTransaction(deploymentPathsFor(*cfg).transaction)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name string
		edit func(*deploymentTransaction)
	}{
		{"artifact", func(value *deploymentTransaction) { value.ArtifactFingerprint = strings.Repeat("1", 64) }},
		{"policy", func(value *deploymentTransaction) { value.PolicyDigest = strings.Repeat("2", 64) }},
		{"candidate", func(value *deploymentTransaction) { value.Candidate.Version += ".drift" }},
		{"prior_plan", func(value *deploymentTransaction) { value.PriorPlan.Digest = strings.Repeat("3", 64) }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			mutated := *tx
			mutation.edit(&mutated)
			if err := mutated.validate(); !errors.Is(err, ErrInstallState) {
				t.Fatalf("validate error = %v, want ErrInstallState", err)
			}
		})
	}
}

func TestDeactivateCrashCheckpointsRecoverExactly(t *testing.T) {
	phases := []Phase{
		PhasePrepared, PhasePriorQuiesced, PhaseTargetPlanned, PhaseCandidateActivated,
		PhaseCandidateReady, PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			cfg, _, _ := testConfig(t, dir, release)
			if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
				t.Fatal(err)
			}
			var injected atomic.Bool
			controller.failpoint = func(got string) error {
				if got == string(phase) && injected.CompareAndSwap(false, true) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg)); err == nil {
				t.Fatal("Deactivate succeeded across simulated crash")
			}
			controller = testController(client, services)
			recovered, recoverErr := controller.Recover(t.Context(), *cfg)
			if phaseAtLeast(phase, PhaseCandidateReady) {
				if recoverErr != nil || recovered.state != RecoveryInactive || recovered.receipt == nil {
					t.Fatalf("Recover = %#v, %v; want inactive", recovered, recoverErr)
				}
			} else if recovered.state != RecoveryActive || recovered.receipt == nil {
				t.Fatalf("Recover = %#v, %v; want active rollback", recovered, recoverErr)
			}
			if services.completion != nil {
				t.Fatal("service completion remains after recovery")
			}
		})
	}
}

func TestDeactivateRecoversAfterTransactionRemovalBeforeServiceAck(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	controller.failpoint = func(name string) error {
		if name == "transaction_removed" {
			return errors.New("simulated crash")
		}
		return nil
	}
	if _, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg)); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Deactivate error = %v, want ErrRecoveryRequired", err)
	}
	paths := deploymentPathsFor(*cfg)
	if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction remains: %v", err)
	}
	if services.completion == nil {
		t.Fatal("service completion was not retained across crash")
	}
	controller = testController(client, services)
	result, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg))
	if err != nil || result.state != DeactivationInactive || result.receipt == nil {
		t.Fatalf("Deactivate recovery = %#v, %v", result, err)
	}
	if services.completion != nil {
		t.Fatal("service completion remains after recovery acknowledgement")
	}
}

func TestRealServiceControllerDeactivateCrashMatrix(t *testing.T) {
	phases := []Phase{
		PhasePrepared, PhasePriorQuiesced, PhaseTargetPlanned, PhaseCandidateActivated,
		PhaseCandidateReady, PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			cfg := emptyServiceConfig(t, dir, release)
			controller := realDeploymentController(client)
			if _, err := controller.Deploy(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			var injected atomic.Bool
			controller.failpoint = func(got string) error {
				if got == string(phase) && injected.CompareAndSwap(false, true) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deactivate(t.Context(), deactivateConfig(cfg)); err == nil {
				t.Fatal("Deactivate succeeded across simulated crash")
			}
			controller = realDeploymentController(client)
			recovered, err := controller.Recover(t.Context(), cfg)
			if phaseAtLeast(phase, PhaseCandidateReady) {
				if err != nil || recovered.state != RecoveryInactive {
					t.Fatalf("Recover = %#v, %v; want inactive", recovered, err)
				}
			} else if recovered.state != RecoveryActive {
				t.Fatalf("Recover = %#v, %v; want active rollback", recovered, err)
			}
		})
	}
}

func TestInactiveDeployReactivationCrashCheckpointsRecoverExactly(t *testing.T) {
	phases := []Phase{
		PhasePrepared, PhasePriorQuiesced, PhaseNamespaceCandidate, PhaseCandidateProved,
		PhaseTargetPlanned, PhaseCandidateActivated, PhaseCandidateReady,
		PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, hits := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			cfg, _, _ := testConfig(t, dir, release)
			active, err := controller.Deploy(t.Context(), *cfg)
			if err != nil {
				t.Fatal(err)
			}
			activeID := fileID{Device: active.current.Device, Inode: active.current.Inode}
			if _, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg)); err != nil {
				t.Fatal(err)
			}
			var injected atomic.Bool
			controller.failpoint = func(got string) error {
				if got == string(phase) && injected.CompareAndSwap(false, true) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
				t.Fatal("Deploy succeeded across simulated crash")
			}
			controller = testController(client, services)
			recovered, recoverErr := controller.Recover(t.Context(), *cfg)
			if phaseAtLeast(phase, PhaseCandidateReady) {
				if recoverErr != nil || recovered.state != RecoveryActive || recovered.receipt == nil {
					t.Fatalf("Recover = %#v, %v; want active", recovered, recoverErr)
				}
			} else if recovered.state != RecoveryInactive || recovered.receipt == nil {
				t.Fatalf("Recover = %#v, %v; want inactive rollback", recovered, recoverErr)
			}
			current := recovered.receipt.current
			if current == nil || (fileID{Device: current.Device, Inode: current.Inode}) != activeID {
				t.Fatal("reactivation recovery replaced the installed app")
			}
			if hits.Load() != 1 {
				t.Fatalf("download hits = %d, want 1", hits.Load())
			}
		})
	}
}

func TestDeployRejectsUnreceiptedCanonical(t *testing.T) {
	dir := t.TempDir()
	canonical := bundle.AppPath(dir, "Helper")
	if err := os.Mkdir(canonical, 0o700); err != nil {
		t.Fatal(err)
	}
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	cfg, _, _ := testConfig(t, dir, release)
	_, err := testController(client, newDeploymentServiceStub(t)).Deploy(t.Context(), *cfg)
	if !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("Deploy error = %v, want ErrInstallConflict", err)
	}
}

func TestStatusAbsentIsReadOnly(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	cfg, _, _ := testConfig(t, dir, release)
	status, err := testController(client, newDeploymentServiceStub(t)).Status(t.Context(), *cfg)
	if err != nil || !status.configMatches {
		t.Fatalf("Status = %#v, %v", status, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".daemonkit-deployment")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Status created state: %v", err)
	}
}

func TestStatusOperationMismatchDoesNotClaimConfigMatch(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	controller := testController(client, newDeploymentServiceStub(t))
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	cfg.ConsumerBuild += "/new"
	status, err := controller.Status(t.Context(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if status.configMatches || status.configMismatch == "" || status.recoveryRequired {
		t.Fatalf("Status = %#v", status)
	}
}

func TestStatusRejectsUnreceiptedOrDriftedCanonical(t *testing.T) {
	t.Run("unreceipted", func(t *testing.T) {
		dir := t.TempDir()
		archive := appArchive(t, "Helper", "1.0.0", "one")
		release, client, _ := releaseFixture(t, archive)
		cfg, _, _ := testConfig(t, dir, release)
		if err := os.Mkdir(bundle.AppPath(cfg.Dir, cfg.AppName), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := testController(client, newDeploymentServiceStub(t)).Status(t.Context(), *cfg); !errors.Is(err, ErrInstallConflict) {
			t.Fatalf("Status error = %v, want ErrInstallConflict", err)
		}
	})
	t.Run("partial_metadata", func(t *testing.T) {
		dir := t.TempDir()
		archive := appArchive(t, "Helper", "1.0.0", "one")
		release, client, _ := releaseFixture(t, archive)
		cfg, _, _ := testConfig(t, dir, release)
		paths := deploymentPathsFor(*cfg)
		if err := os.MkdirAll(paths.metadataDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(paths.canonical, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := testController(client, newDeploymentServiceStub(t)).Status(t.Context(), *cfg); !errors.Is(err, ErrInstallState) {
			t.Fatalf("Status error = %v, want ErrInstallState", err)
		}
	})
	for _, drift := range []string{"missing", "replaced"} {
		t.Run(drift, func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			controller := testController(client, newDeploymentServiceStub(t))
			cfg, _, _ := testConfig(t, dir, release)
			if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
				t.Fatal(err)
			}
			canonical := bundle.AppPath(cfg.Dir, cfg.AppName)
			moved := canonical + ".moved"
			if err := os.Rename(canonical, moved); err != nil {
				t.Fatal(err)
			}
			if drift == "replaced" {
				if err := os.Mkdir(canonical, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := controller.Status(t.Context(), *cfg); err == nil {
				t.Fatal("Status accepted canonical drift")
			}
		})
	}
}

func TestStoredPlanDigestUsesHexForArbitraryBytes(t *testing.T) {
	var plan service.Plan
	for index := 0; ; index++ {
		candidate, err := service.NewPlan([]service.Agent{{
			Label: fmt.Sprintf("com.example.%d", index), Program: "/usr/bin/true",
			LogPath: filepath.Join(t.TempDir(), "log"), RestartPolicy: service.RestartAlways,
		}})
		if err != nil {
			t.Fatal(err)
		}
		digest := candidate.Digest()
		if !utf8.Valid(digest[:]) {
			plan = candidate
			break
		}
	}
	stored := storePlan(plan)
	if len(stored.Digest) != 64 || strings.ToLower(stored.Digest) != stored.Digest {
		t.Fatalf("stored digest = %q", stored.Digest)
	}
	restored, err := restorePlan(stored)
	if err != nil || restored.Digest() != plan.Digest() {
		t.Fatalf("restore = %s, %v", restored.Digest(), err)
	}
}

func TestDeploymentSchemaFingerprintsAreDerivedAndPinned(t *testing.T) {
	if got := transactionSchemaFingerprint(); got != deploymentFingerprint {
		t.Fatalf("transaction fingerprint = %s, want %s", got, deploymentFingerprint)
	}
	if got := receiptSchemaFingerprint(); got != receiptFingerprint {
		t.Fatalf("receipt fingerprint = %s, want %s", got, receiptFingerprint)
	}
}

func TestDeploymentReadersRejectSchemaDrift(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	paths := deploymentPathsFor(*cfg)
	receiptData, err := os.ReadFile(paths.receipt)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal(receiptData, &receipt); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"old_fingerprint", func(value map[string]any) {
			value["fingerprint"] = "1b60bdb5d2f91782673bf2147c4964d70d19ae4d14cf34c6b874a8ddfe2c52d3"
		}},
		{"missing_field", func(value map[string]any) { delete(value, "activation_operation") }},
		{"extra_field", func(value map[string]any) { value["unexpected"] = true }},
		{"wrong_shape", func(value map[string]any) { value["activation_operation"] = "invalid" }},
	} {
		t.Run("receipt_"+mutation.name, func(t *testing.T) {
			mutated := maps.Clone(receipt)
			mutation.edit(mutated)
			data, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "receipt.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readDeploymentReceipt(path); !errors.Is(err, ErrInstallState) {
				t.Fatalf("read error = %v, want ErrInstallState", err)
			}
		})
	}

	controller = testController(client, services)
	controller.failpoint = func(name string) error {
		if name == string(PhasePrepared) {
			return errors.New("simulated crash")
		}
		return nil
	}
	cfg.ConsumerBuild += "/new"
	if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
		t.Fatal("Deploy succeeded across simulated crash")
	}
	txData, err := os.ReadFile(paths.transaction)
	if err != nil {
		t.Fatal(err)
	}
	var transaction map[string]any
	if err := json.Unmarshal(txData, &transaction); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"identity", func(value map[string]any) { value["identity"] = "daemonkit.fetch.transaction.v1" }},
		{"schema", func(value map[string]any) { value["schema"] = float64(2) }},
		{"fingerprint", func(value map[string]any) { value["fingerprint"] = strings.Repeat("0", 64) }},
		{"missing", func(value map[string]any) { delete(value, "mode") }},
		{"unknown", func(value map[string]any) { value["unexpected"] = true }},
	} {
		t.Run("transaction_"+mutation.name, func(t *testing.T) {
			mutated := maps.Clone(transaction)
			mutation.edit(mutated)
			data, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "transaction.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readDeploymentTransaction(path); !errors.Is(err, ErrInstallState) {
				t.Fatalf("read error = %v, want ErrInstallState", err)
			}
		})
	}
}

func TestForwardCrashCheckpointsRecoverToOneCompleteState(t *testing.T) {
	phases := []Phase{
		PhasePrepared, PhasePriorQuiesced, PhaseNamespaceCandidate,
		PhaseCandidateProved, PhaseTargetPlanned, PhaseCandidateActivated, PhaseCandidateReady,
		PhaseReceiptCommitted, PhaseServiceCommitPending, PhaseCleanupComplete,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			cfg, _, _ := testConfig(t, dir, release)
			var injected atomic.Bool
			controller.failpoint = func(got string) error {
				if got == string(phase) && injected.CompareAndSwap(false, true) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
				t.Fatal("Deploy succeeded across simulated crash")
			}
			controller.failpoint = nil
			recovered, recoverErr := controller.Recover(t.Context(), *cfg)
			if phaseAtLeast(phase, PhaseCandidateReady) {
				if recoverErr != nil || recovered.state != RecoveryActive || recovered.receipt == nil {
					t.Fatalf("Recover = %#v, %v; want active", recovered, recoverErr)
				}
			} else if recovered.state != RecoveryAbsent || recovered.receipt != nil {
				t.Fatalf("Recover = %#v, %v; want absence", recovered, recoverErr)
			}
			if _, err := os.Stat(deploymentPathsFor(*cfg).transaction); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("transaction remains: %v", err)
			}
		})
	}
}

func TestRollbackCrashCheckpointsRestoreExactPriorState(t *testing.T) {
	checkpoints := []string{
		"direction_flip", string(PhaseRollbackQuiesced), string(PhasePriorRestored),
		string(PhasePriorProved), string(PhasePriorActivated), string(PhasePriorReady),
		string(PhaseReceiptCommitted), string(PhaseServiceCommitPending), string(PhaseCleanupComplete),
		"service_acknowledged",
	}
	for _, scenario := range []string{"fresh", "replace", "reconfigure", "reactivate"} {
		for _, checkpoint := range checkpoints {
			t.Run(scenario+"/"+checkpoint, func(t *testing.T) {
				dir := t.TempDir()
				archive := appArchive(t, "Helper", "1.0.0", "one")
				release, client, _ := releaseFixture(t, archive)
				services := newDeploymentServiceStub(t)
				var operation atomic.Uint64
				newController := func(client *http.Client) *Controller {
					controller := testController(client, services)
					controller.operationID = func() (string, error) {
						return fmt.Sprintf("%032x", operation.Add(1)), nil
					}
					return controller
				}
				controller := newController(client)
				cfg, _, _ := testConfig(t, dir, release)
				expectedState := RecoveryAbsent
				var priorReceipt []byte
				var priorCanonical []byte
				priorPlan, _ := service.NewPlan(nil)
				var runtimeRoles []ProofRole
				recordRuntime := func(
					_ context.Context, _ RuntimeStopper, operation RuntimeQuiesceOperation,
				) (RuntimeProof, error) {
					runtimeRoles = append(runtimeRoles, operation.Role)
					return RuntimeProof{Role: operation.Role, Absent: true, Digest: sha256.Sum256([]byte("runtime"))}, nil
				}
				cfg.RuntimeQuiesce = recordRuntime
				if scenario != "fresh" {
					active, err := controller.Deploy(t.Context(), *cfg)
					if err != nil {
						t.Fatal(err)
					}
					priorPlan = active.plan
					expectedState = RecoveryActive
					if scenario == "reactivate" {
						inactive, err := controller.Deactivate(t.Context(), deactivateConfig(*cfg))
						if err != nil || inactive.receipt == nil {
							t.Fatalf("Deactivate = %#v, %v", inactive, err)
						}
						priorPlan = inactive.receipt.plan
						expectedState = RecoveryInactive
					}
					runtimeRoles = nil
					paths := deploymentPathsFor(*cfg)
					priorReceipt, err = os.ReadFile(paths.receipt)
					if err != nil {
						t.Fatal(err)
					}
					priorCanonical, err = os.ReadFile(filepath.Join(paths.canonical, "Contents", "MacOS", cfg.AppName))
					if err != nil {
						t.Fatal(err)
					}
				}
				switch scenario {
				case "replace":
					replacement := appArchive(t, "Helper", "2.0.0", "two")
					replacementRelease, replacementClient, _ := releaseFixture(t, replacement)
					cfg, _, _ = testConfig(t, dir, replacementRelease)
					client = replacementClient
					controller = newController(client)
				case "reconfigure":
					cfg.ConsumerBuild += "/new"
				}
				cfg.RuntimeQuiesce = recordRuntime
				baseReadiness := cfg.Readiness
				cfg.Readiness = func(ctx context.Context, operation Operation, plan service.Plan) (Proof, error) {
					if operation.Role == ProofCandidateReady {
						return Proof{}, errors.New("candidate readiness failed")
					}
					return baseReadiness(ctx, operation, plan)
				}
				var candidateActivated atomic.Int64
				var injected atomic.Bool
				controller.failpoint = func(got string) error {
					match := got == checkpoint
					if checkpoint == "direction_flip" && got == string(PhaseCandidateActivated) {
						match = candidateActivated.Add(1) == 2
					}
					if match && injected.CompareAndSwap(false, true) {
						return errors.New("simulated rollback crash")
					}
					return nil
				}
				if _, err := controller.Deploy(t.Context(), *cfg); err == nil {
					t.Fatal("Deploy succeeded across rollback crash")
				}
				controller = newController(client)
				recovered, recoverErr := controller.Recover(t.Context(), *cfg)
				if recovered.state != expectedState || (expectedState == RecoveryAbsent) != (recovered.receipt == nil) {
					t.Fatalf("Recover = %#v, %v; want %s", recovered, recoverErr, expectedState)
				}
				paths := deploymentPathsFor(*cfg)
				if expectedState == RecoveryAbsent {
					if _, err := os.Lstat(paths.receipt); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("fresh rollback receipt = %v", err)
					}
					if _, err := os.Lstat(paths.canonical); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("fresh rollback canonical = %v", err)
					}
				} else {
					gotReceipt, err := os.ReadFile(paths.receipt)
					if err != nil || !bytes.Equal(gotReceipt, priorReceipt) {
						t.Fatalf("rollback changed prior receipt: %v", err)
					}
					gotCanonical, err := os.ReadFile(filepath.Join(paths.canonical, "Contents", "MacOS", cfg.AppName))
					if err != nil || !bytes.Equal(gotCanonical, priorCanonical) {
						t.Fatalf("rollback changed prior canonical: %v", err)
					}
				}
				assertDeploymentResidueClean(t, paths)
				status, err := services.ReplacementStatus(t.Context())
				if err != nil || status != nil {
					t.Fatalf("replacement status = %#v, %v", status, err)
				}
				completion, err := services.DeploymentCompletion(t.Context())
				if err != nil || completion != nil {
					t.Fatalf("completion = %#v, %v", completion, err)
				}
				snapshot, err := services.Snapshot(t.Context())
				if err != nil || !samePlan(snapshot, priorPlan) {
					t.Fatalf("service snapshot = %#v, %v", snapshot, err)
				}
				assertRollbackRuntimeRoles(t, scenario, runtimeRoles)
				cfg.Readiness = baseReadiness
				controller = newController(client)
				if active, err := controller.Deploy(t.Context(), *cfg); err != nil || active.state != DeploymentActive {
					t.Fatalf("retry Deploy = %#v, %v", active, err)
				}
				assertDeploymentResidueClean(t, paths)
			})
		}
	}
}

func assertRollbackRuntimeRoles(t *testing.T, scenario string, got []ProofRole) {
	t.Helper()
	want := map[string][]ProofRole{
		"fresh":       {ProofRollbackRuntime},
		"replace":     {ProofPriorRuntime, ProofRollbackRuntime},
		"reconfigure": {ProofPriorRuntime, ProofRollbackRuntime},
		"reactivate":  {ProofRollbackRuntime},
	}[scenario]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime quiesce roles = %#v, want %#v", got, want)
	}
}

func assertDeploymentResidueClean(t *testing.T, paths deploymentPaths) {
	t.Helper()
	if _, err := os.Lstat(paths.transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("transaction residue = %v", err)
	}
	entries, err := os.ReadDir(paths.metadataDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") || strings.HasPrefix(entry.Name(), ".download-") ||
			strings.HasPrefix(entry.Name(), ".durable-") {
			t.Fatalf("preparation residue remains: %s", entry.Name())
		}
	}
}

func TestRuntimeStopAccessIsRequestBoundRevocableAndCancelOwning(t *testing.T) {
	services := newDeploymentServiceStub(t)
	access := serviceAccess{open: func(context.Context, service.ControllerConfig) (deploymentController, error) {
		return services, nil
	}}
	var calls atomic.Int64
	services.stopRuntime = func(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error) {
		calls.Add(1)
		return service.StopReceipt{}, nil
	}
	stopper := newRuntimeStopAccess(t.Context(), access, "stop-operation", "runtime-v1")
	request := service.StopRuntimeRequest{OperationID: "stop-operation", ExpectedRuntimeBuild: "runtime-v1"}
	mismatch := request
	mismatch.OperationID = "other-operation"
	if _, err := stopper.StopRuntime(t.Context(), mismatch); err == nil {
		t.Fatal("StopRuntime accepted a request outside the callback scope")
	}
	if calls.Load() != 0 {
		t.Fatal("mismatched request reached service controller")
	}

	entered := make(chan struct{})
	services.stopRuntime = func(ctx context.Context, _ service.StopRuntimeRequest) (service.StopReceipt, error) {
		close(entered)
		<-ctx.Done()
		return service.StopReceipt{}, ctx.Err()
	}
	callDone := make(chan error, 1)
	go func() {
		_, err := stopper.StopRuntime(context.Background(), request)
		callDone <- err
	}()
	<-entered
	revoked := make(chan struct{})
	go func() {
		stopper.revoke()
		close(revoked)
	}()
	select {
	case <-revoked:
	case <-time.After(time.Second):
		t.Fatal("revoke did not cancel and join background StopRuntime")
	}
	if err := <-callDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight StopRuntime error = %v, want context canceled", err)
	}
	if _, err := stopper.StopRuntime(context.Background(), request); !errors.Is(err, errRuntimeStopperExpired) {
		t.Fatalf("retained StopRuntime error = %v, want expired capability", err)
	}
}

func TestRuntimeStopAccessRejectsCanceledCallsBeforeControllerOpen(t *testing.T) {
	services := newDeploymentServiceStub(t)
	var opens atomic.Int64
	var calls atomic.Int64
	access := serviceAccess{open: func(context.Context, service.ControllerConfig) (deploymentController, error) {
		opens.Add(1)
		return services, nil
	}}
	services.stopRuntime = func(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error) {
		calls.Add(1)
		return service.StopReceipt{}, nil
	}
	stopper := newRuntimeStopAccess(t.Context(), access, "stop-operation", "runtime-v1")
	defer stopper.revoke()
	request := service.StopRuntimeRequest{OperationID: "stop-operation", ExpectedRuntimeBuild: "runtime-v1"}

	preCanceled, cancelPreCanceled := context.WithCancel(t.Context())
	cancelPreCanceled()
	if _, err := stopper.StopRuntime(preCanceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled StopRuntime error = %v, want context canceled", err)
	}
	if opens.Load() != 0 || calls.Load() != 0 {
		t.Fatalf("pre-canceled StopRuntime opened/called controller = %d/%d", opens.Load(), calls.Load())
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	services.stopRuntime = func(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error) {
		calls.Add(1)
		close(entered)
		<-release
		return service.StopReceipt{}, nil
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := stopper.StopRuntime(t.Context(), request)
		firstDone <- err
	}()
	<-entered

	queuedCtx, cancelQueued := context.WithCancel(t.Context())
	queuedDone := make(chan error, 1)
	go func() {
		_, err := stopper.StopRuntime(queuedCtx, request)
		queuedDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		stopper.mu.Lock()
		inFlight := stopper.inFlight
		stopper.mu.Unlock()
		if inFlight == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second StopRuntime did not queue")
		}
		runtime.Gosched()
	}
	cancelQueued()
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-queuedDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued-canceled StopRuntime error = %v, want context canceled", err)
	}
	if opens.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("queued-canceled StopRuntime opened/called controller = %d/%d, want 1/1", opens.Load(), calls.Load())
	}
}

func TestRuntimeQuiesceRevokesRetainedStopperAfterCallbackPanic(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	receipt, err := readDeploymentReceipt(deploymentPathsFor(*cfg).receipt)
	if err != nil || receipt.Current == nil {
		t.Fatalf("read deployed generation = %#v, %v", receipt, err)
	}
	var opens atomic.Int64
	controller.openController = func(context.Context, service.ControllerConfig) (deploymentController, error) {
		opens.Add(1)
		return services, nil
	}
	var retained RuntimeStopper
	cfg.RuntimeQuiesce = func(_ context.Context, stopper RuntimeStopper, _ RuntimeQuiesceOperation) (RuntimeProof, error) {
		retained = stopper
		panic("consumer panic")
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = controller.runtimeQuiesce(
			t.Context(), *cfg, serviceAccess{open: controller.openController}, "panic-operation",
			*receipt.Current, ProofPriorRuntime,
		)
	}()
	if recovered == nil || retained == nil {
		t.Fatalf("runtime callback panic/retained stopper = %v/%v", recovered, retained)
	}
	request := service.StopRuntimeRequest{
		OperationID: "panic-operation", ExpectedRuntimeBuild: receipt.Current.Version,
	}
	if _, err := retained.StopRuntime(t.Context(), request); !errors.Is(err, errRuntimeStopperExpired) {
		t.Fatalf("retained StopRuntime after panic = %v, want expired capability", err)
	}
	if opens.Load() != 0 {
		t.Fatalf("retained StopRuntime after panic opened %d controllers", opens.Load())
	}
}

func TestRuntimeStopAccessSerializesControllerOwnership(t *testing.T) {
	services := newDeploymentServiceStub(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	services.stopRuntime = func(context.Context, service.StopRuntimeRequest) (service.StopReceipt, error) {
		entered <- struct{}{}
		<-release
		return service.StopReceipt{}, nil
	}
	var open atomic.Int64
	var maximum atomic.Int64
	access := serviceAccess{open: func(context.Context, service.ControllerConfig) (deploymentController, error) {
		current := open.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		return closeHookController{deploymentController: services, close: func() { open.Add(-1) }}, nil
	}}
	stopper := newRuntimeStopAccess(t.Context(), access, "stop-operation", "runtime-v1")
	defer stopper.revoke()
	request := service.StopRuntimeRequest{OperationID: "stop-operation", ExpectedRuntimeBuild: "runtime-v1"}
	done := make(chan error, 2)
	call := func() {
		_, err := stopper.StopRuntime(t.Context(), request)
		done <- err
	}
	go call()
	<-entered
	go call()
	select {
	case <-entered:
		t.Fatal("second StopRuntime entered before exclusive controller ownership settled")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent service controllers = %d, want 1", maximum.Load())
	}
}

func TestProofValidationRequiresExactSemanticDomain(t *testing.T) {
	evidence := sha256.Sum256([]byte("evidence"))
	plan := sha256.Sum256([]byte("plan"))
	proof := Proof{Role: ProofCandidateReady, PlanDigest: plan, Digest: evidence}
	if err := validateProof(proof, ProofCandidateReady, plan); err != nil {
		t.Fatal(err)
	}
	if err := validateProof(proof, ProofPriorReady, plan); err == nil {
		t.Fatal("proof crossed semantic roles")
	}
	if err := validateProof(proof, ProofCandidateReady, sha256.Sum256([]byte("other"))); err == nil {
		t.Fatal("readiness proof crossed plan digests")
	}
	runtime := RuntimeProof{Role: ProofPriorRuntime, Absent: true, Digest: evidence}
	if err := validateRuntimeProof(runtime, ProofPriorRuntime); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeProof(runtime, ProofRollbackRuntime); err == nil {
		t.Fatal("runtime proof crossed semantic roles")
	}
}

func TestGenerationSealRejectsSameInodeBundleAndCDHashDrift(t *testing.T) {
	for _, drift := range []string{"bundle", "cdhash"} {
		t.Run(drift, func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			var cdHash atomic.Value
			cdHash.Store(strings.Repeat("a", 40))
			controller.verifier = verifierFunc(func(context.Context, string, string) (string, error) {
				return cdHash.Load().(string), nil
			})
			cfg, _, _ := testConfig(t, dir, release)
			if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
				t.Fatal(err)
			}
			paths := deploymentPathsFor(*cfg)
			before, err := identifyPath(paths.canonical)
			if err != nil {
				t.Fatal(err)
			}
			if drift == "bundle" {
				executable := filepath.Join(paths.canonical, "Contents", "MacOS", cfg.AppName)
				if err := os.WriteFile(executable, []byte("mutated"), 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				cdHash.Store(strings.Repeat("b", 40))
			}
			after, err := identifyPath(paths.canonical)
			if err != nil || after != before {
				t.Fatalf("test changed directory identity: %v, %v != %v", err, after, before)
			}
			if _, err := controller.Status(t.Context(), *cfg); !errors.Is(err, ErrInstallConflict) {
				t.Fatalf("Status error = %v, want exact generation conflict", err)
			}
		})
	}
}

func TestStatusRejectsSealedDriftInActiveTransactionSlots(t *testing.T) {
	for _, slot := range []string{"canonical", "staged"} {
		t.Run(slot, func(t *testing.T) {
			dir := t.TempDir()
			archive := appArchive(t, "Helper", "1.0.0", "one")
			release, client, _ := releaseFixture(t, archive)
			services := newDeploymentServiceStub(t)
			controller := testController(client, services)
			cfg, _, _ := testConfig(t, dir, release)
			if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
				t.Fatal(err)
			}
			replacement := appArchive(t, "Helper", "2.0.0", "two")
			nextRelease, nextClient, _ := releaseFixture(t, replacement)
			next, _, _ := testConfig(t, dir, nextRelease)
			controller.client = nextClient
			controller.failpoint = func(name string) error {
				if name == string(PhasePrepared) {
					return errors.New("simulated crash")
				}
				return nil
			}
			if _, err := controller.Deploy(t.Context(), *next); err == nil {
				t.Fatal("Deploy succeeded across simulated crash")
			}
			paths := deploymentPathsFor(*next)
			tx, err := readDeploymentTransaction(paths.transaction)
			if err != nil {
				t.Fatal(err)
			}
			app := paths.canonical
			if slot == "staged" {
				app = stageApp(paths, tx)
			}
			executable := filepath.Join(app, "Contents", "MacOS", next.AppName)
			if err := os.WriteFile(executable, []byte("drift"), 0o755); err != nil {
				t.Fatal(err)
			}
			controller.failpoint = nil
			if _, err := controller.Status(t.Context(), *next); !errors.Is(err, ErrRecoveryRequired) ||
				!errors.Is(err, ErrInstallConflict) {
				t.Fatalf("Status error = %v, want recovery-required install conflict", err)
			}
		})
	}
}

func TestRecoverCleansOnlySafePreparationResidue(t *testing.T) {
	dir := t.TempDir()
	archive := appArchive(t, "Helper", "1.0.0", "one")
	release, client, _ := releaseFixture(t, archive)
	services := newDeploymentServiceStub(t)
	controller := testController(client, services)
	cfg, _, _ := testConfig(t, dir, release)
	if _, err := controller.Deploy(t.Context(), *cfg); err != nil {
		t.Fatal(err)
	}
	paths := deploymentPathsFor(*cfg)
	if err := os.Mkdir(filepath.Join(paths.metadataDir, ".stage-orphan"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.metadataDir, ".download-orphan"), []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := controller.Recover(t.Context(), *cfg); err != nil || result.state != RecoveryActive {
		t.Fatalf("Recover = %#v, %v", result, err)
	}
	assertDeploymentResidueClean(t, paths)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	unsafe := filepath.Join(paths.metadataDir, ".stage-unsafe")
	if err := os.Symlink(target, unsafe); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Recover(t.Context(), *cfg); !errors.Is(err, ErrInstallConflict) {
		t.Fatalf("Recover unsafe residue error = %v, want ErrInstallConflict", err)
	}
	if body, err := os.ReadFile(target); err != nil || string(body) != "keep" {
		t.Fatalf("unsafe residue target changed: %q, %v", body, err)
	}
}
