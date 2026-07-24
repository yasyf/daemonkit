package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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
