package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
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

func TestResolveSignedAppFormulaUpgradeHint(t *testing.T) {
	desc := signedAppDescriptor(t.TempDir(), "12.15.3")
	desc.App.Cask = ""
	desc.App.Formula = "yasyf/tap/captain-hook"

	_, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc)
	var upgrade *ManualUpgradeError
	if !errors.As(err, &upgrade) || upgrade.Formula != "yasyf/tap/captain-hook" || upgrade.Cask != "" {
		t.Fatalf("Resolve() = %v, want ManualUpgradeError naming the formula", err)
	}
}

func minVersionDescriptor(dir, minVersion string) *Descriptor {
	return &Descriptor{
		Schema: 1, Name: "capt-hook", Kind: SignedApp,
		Version: VersionSource{File: filepath.Join(dir, "Captain Hook.app", "Contents", "Info.plist"), PlistKey: "CFBundleShortVersionString"},
		App:     &AppSpec{Dir: dir, AppName: "Captain Hook", Exec: "Contents/Helpers/capt-hookd", Formula: "yasyf/tap/captain-hook", MinVersion: minVersion},
	}
}

func TestResolveSignedAppBelowMinVersion(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.27.4", "Contents/Helpers/capt-hookd")

	_, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), minVersionDescriptor(dir, "12.28.0"))
	if !errors.Is(err, ErrManualUpgrade) {
		t.Fatalf("Resolve() = %v, want ErrManualUpgrade", err)
	}
	var upgrade *ManualUpgradeError
	if !errors.As(err, &upgrade) || !upgrade.AtLeast || upgrade.Want != "12.28.0" || upgrade.Got != "12.27.4" || upgrade.Formula != "yasyf/tap/captain-hook" {
		t.Fatalf("ManualUpgradeError = %+v", upgrade)
	}
	want := `artifact: signed app "capt-hook" is version 12.27.4, want at least 12.28.0; run: brew upgrade yasyf/tap/captain-hook`
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestResolveSignedAppMeetsMinVersion(t *testing.T) {
	tests := []struct {
		name      string
		installed string
	}{
		{"equal", "12.28.0"},
		{"above", "12.29.1"},
		{"dev build", "9999.1757840000000000000.0-dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeApp(t, dir, "Captain Hook", tt.installed, "Contents/Helpers/capt-hookd")

			path, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), minVersionDescriptor(dir, "12.28.0"))
			if err != nil {
				t.Fatalf("Resolve() = %v, want nil", err)
			}
			if want := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd"); path != want {
				t.Fatalf("path = %q, want %q", path, want)
			}
		})
	}
}

func TestManualUpgradeErrorRendersBrewCommand(t *testing.T) {
	tests := []struct {
		name string
		err  *ManualUpgradeError
		want string
	}{
		{"cask absent", &ManualUpgradeError{Name: "cap", Cask: "captain-hook"}, `artifact: signed app "cap" is not installed; run: brew upgrade --cask captain-hook`},
		{"cask stale", &ManualUpgradeError{Name: "cap", Cask: "captain-hook", Want: "1.2.0", Got: "1.1.0"}, `artifact: signed app "cap" is version 1.1.0, want 1.2.0; run: brew upgrade --cask captain-hook`},
		{"formula absent", &ManualUpgradeError{Name: "cap", Formula: "yasyf/tap/captain-hook"}, `artifact: signed app "cap" is not installed; run: brew upgrade yasyf/tap/captain-hook`},
		{"formula stale", &ManualUpgradeError{Name: "cap", Formula: "yasyf/tap/captain-hook", Want: "1.2.0", Got: "1.1.0"}, `artifact: signed app "cap" is version 1.1.0, want 1.2.0; run: brew upgrade yasyf/tap/captain-hook`},
		{"formula below minimum", &ManualUpgradeError{Name: "cap", Formula: "yasyf/tap/captain-hook", Want: "1.2.0", Got: "1.1.0", AtLeast: true}, `artifact: signed app "cap" is version 1.1.0, want at least 1.2.0; run: brew upgrade yasyf/tap/captain-hook`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSignedAppExpandsHomeInDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	t.Setenv("HOME", t.TempDir())
	dir := filepath.Join(home, "Applications")
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := signedAppDescriptor("~/Applications", "12.15.3")

	path, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	want := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	if path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
}

func TestSignedAppAbsoluteDirIgnoresHome(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())
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

func TestSignedAppHomeDirRefusesEscape(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	writeApp(t, home, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	desc := signedAppDescriptor("~/Applications", "12.15.3")
	desc.App.AppName = "../Captain Hook"

	if _, err := (Store{Root: t.TempDir()}).Resolve(context.Background(), desc); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("Resolve() = %v, want ErrUnsafeArchive", err)
	}
}
