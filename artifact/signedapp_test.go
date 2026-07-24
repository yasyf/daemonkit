package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/deployment"
)

func withDeployer(d deployer) Option { return func(o *options) { o.deployer = d } }

type stubDeployer struct {
	captured deployment.Config
	err      error
}

func (s *stubDeployer) Deploy(_ context.Context, cfg deployment.Config) (deployment.DeploymentReceipt, error) {
	s.captured = cfg
	return deployment.DeploymentReceipt{}, s.err
}

func writeApp(t *testing.T, dir, name, version, exec string) {
	t.Helper()
	appPath := filepath.Join(dir, name+".app")
	if err := os.MkdirAll(filepath.Join(appPath, "Contents"), 0o755); err != nil {
		t.Fatal(err)
	}
	execPath := filepath.Join(appPath, exec)
	if err := os.MkdirAll(filepath.Dir(execPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(execPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	plist := `<plist><dict><key>CFBundleShortVersionString</key><string>` + version + `</string></dict></plist>`
	if err := os.WriteFile(filepath.Join(appPath, "Contents", "Info.plist"), []byte(plist), 0o644); err != nil {
		t.Fatal(err)
	}
}

func signedAppDescriptor(dir, version string) *Descriptor {
	return &Descriptor{
		Schema: 1, Name: "Captain Hook", Kind: SignedApp,
		Version: VersionSource{Static: version},
		App:     &AppSpec{Dir: dir, AppName: "Captain Hook", Exec: "Contents/Helpers/capt-hookd", Cask: "captain-hook"},
	}
}

func TestResolveSignedAppAttestMatches(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := signedAppDescriptor(dir, "12.15.3")

	path, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	want := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	if path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
}

func TestResolveSignedAppAttestTagBareSpelling(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := signedAppDescriptor(dir, "v12.15.3") // TAG spelling matches BARE installed version

	if _, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc); err != nil {
		t.Fatalf("Resolve() = %v, want nil (TAG/BARE equal)", err)
	}
}

func TestResolveSignedAppAttestVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := signedAppDescriptor(dir, "12.15.4")

	_, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc)
	if !errors.Is(err, ErrManualUpgrade) {
		t.Fatalf("Resolve() = %v, want ErrManualUpgrade", err)
	}
	var upgrade *ManualUpgradeError
	if !errors.As(err, &upgrade) || upgrade.Got != "12.15.3" || upgrade.Want != "12.15.4" || upgrade.Cask != "captain-hook" {
		t.Fatalf("ManualUpgradeError = %+v", upgrade)
	}
}

func TestResolveSignedAppAttestMissingApp(t *testing.T) {
	desc := signedAppDescriptor(t.TempDir(), "12.15.3") // no app written

	_, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc)
	var upgrade *ManualUpgradeError
	if !errors.As(err, &upgrade) || upgrade.Got != "" {
		t.Fatalf("Resolve() = %v, want ManualUpgradeError with empty Got", err)
	}
}

func TestResolveSignedAppDeployAssemblesConfig(t *testing.T) {
	digest := strings.Repeat("a", 64)
	platform, err := CurrentPlatform()
	if err != nil {
		t.Fatal(err)
	}
	desc := &Descriptor{
		Schema: 1, Name: "Captain Hook", Kind: SignedApp,
		Version: VersionSource{Static: "12.15.3"},
		App:     &AppSpec{Dir: "/opt/apps", AppName: "Captain Hook", Exec: "Contents/Helpers/capt-hookd", Cask: "captain-hook"},
		Platforms: map[Platform]PlatformEntry{
			platform: {
				Size: 10, Hash: "sha256", Digest: digest, Format: Zip, Path: "Captain Hook.app",
				Providers: []Provider{{Type: GitHubRelease, Repo: "yasyf/captain-hook", Tag: "v12.15.3", Name: "Captain Hook.zip"}},
			},
		},
	}
	stub := &stubDeployer{err: errors.New("deploy boom")}
	base := deployment.Config{ConsumerBuild: "cb-123"}

	_, err = (Store{Root: t.TempDir()}).Resolve(context.Background(), desc,
		WithSignedAppDeploy(base), withDeployer(stub))
	if err == nil || !strings.Contains(err.Error(), "deploy boom") {
		t.Fatalf("Resolve() = %v, want wrapped deploy error", err)
	}
	if stub.captured.Dir != "/opt/apps" || stub.captured.AppName != "Captain Hook" {
		t.Fatalf("captured dir/app = %q/%q", stub.captured.Dir, stub.captured.AppName)
	}
	if stub.captured.Release.Version != "12.15.3" || stub.captured.Release.SHA256.String() != digest {
		t.Fatalf("captured release = %+v", stub.captured.Release)
	}
	if stub.captured.Release.URL != "https://github.com/yasyf/captain-hook/releases/download/v12.15.3/Captain%20Hook.zip" {
		t.Fatalf("captured release URL = %q", stub.captured.Release.URL)
	}
	if stub.captured.ConsumerBuild != "cb-123" {
		t.Fatalf("base ConsumerBuild not preserved: %q", stub.captured.ConsumerBuild)
	}
}
