package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func copyExecDescriptor(dir, version string) *Descriptor {
	desc := signedAppDescriptor(dir, version)
	desc.App.CopyExec = true
	return desc
}

func readMeta(t *testing.T, dir string) cacheMeta {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta cacheMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestResolveSignedAppWithoutCopyExecTouchesNoCache(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	store := Store{Root: t.TempDir()}

	path, err := store.Resolve(context.Background(), signedAppDescriptor(dir, "12.15.3"))
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if want := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd"); path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	if _, err := os.Stat(store.CacheDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache dir stat = %v, want ErrNotExist", err)
	}
}

func TestResolveSignedAppCopyExecReturnsCachedCopy(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	payload := []byte("#!/bin/sh\necho capt-hookd\n")
	if err := os.WriteFile(source, payload, 0o755); err != nil {
		t.Fatal(err)
	}
	store := Store{Root: t.TempDir()}

	path, err := store.Resolve(context.Background(), copyExecDescriptor(dir, "12.15.3"))
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if !strings.HasPrefix(path, store.CacheDir()+string(os.PathSeparator)) {
		t.Fatalf("path = %q, want under %q", path, store.CacheDir())
	}
	if filepath.Base(path) != "capt-hookd" {
		t.Fatalf("basename = %q, want capt-hookd", filepath.Base(path))
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("copy = %q, want %q", got, payload)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("mode = %o, want 755", info.Mode().Perm())
	}
	sum := sha256.Sum256(payload)
	meta := readMeta(t, filepath.Dir(path))
	if meta.Name != "Captain Hook" || meta.Tag != "12.15.3" || meta.Source != source || meta.Digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("meta = %+v", meta)
	}
	entries, err := store.CacheEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Source != source || entries[0].Dir != filepath.Dir(path) {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestResolveSignedAppCopyExecHitNeverReadsSource(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	store := Store{Root: t.TempDir()}
	desc := copyExecDescriptor(dir, "12.15.3")

	first, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("first Resolve() = %v", err)
	}
	if err := os.Chmod(source, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(source, 0o755) })

	second, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("second Resolve() = %v, want a hit that never opens the source", err)
	}
	if second != first {
		t.Fatalf("second path = %q, want %q", second, first)
	}
}

func TestResolveSignedAppCopyExecUpgradePrunesOldCopy(t *testing.T) {
	tests := []struct {
		name    string
		upgrade func(t *testing.T, dir string) *Descriptor
	}{
		{"new version", func(t *testing.T, dir string) *Descriptor {
			writeApp(t, dir, "Captain Hook", "12.16.0", "Contents/Helpers/capt-hookd")
			return copyExecDescriptor(dir, "12.16.0")
		}},
		{"replaced file", func(t *testing.T, dir string) *Descriptor {
			source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
			if err := os.WriteFile(source, []byte("#!/bin/sh\necho rebuilt\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			return copyExecDescriptor(dir, "12.15.3")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
			source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
			store := Store{Root: t.TempDir()}

			old, err := store.Resolve(context.Background(), copyExecDescriptor(dir, "12.15.3"))
			if err != nil {
				t.Fatalf("Resolve() = %v", err)
			}
			desc := tt.upgrade(t, dir)
			current, err := store.Resolve(context.Background(), desc)
			if err != nil {
				t.Fatalf("Resolve() after upgrade = %v", err)
			}
			if current == old {
				t.Fatalf("path = %q after upgrade, want a new entry", current)
			}
			want, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(current)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("copy = %q, want %q", got, want)
			}
			if _, err := os.Stat(filepath.Dir(old)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old entry stat = %v, want ErrNotExist", err)
			}
			entries, err := store.CacheEntries()
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Dir != filepath.Dir(current) {
				t.Fatalf("entries = %+v, want only the current copy", entries)
			}
		})
	}
}

func TestResolveSignedAppCopyExecHitDoesNotPrune(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	store := Store{Root: t.TempDir()}
	stale := seedCacheEntry(t, store, "Captain Hook", "12.14.0", strings.Repeat("c", 64))
	desc := copyExecDescriptor(dir, "12.15.3")

	if _, err := store.Resolve(context.Background(), desc); err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if _, err := store.Resolve(context.Background(), desc); err != nil {
		t.Fatalf("second Resolve() = %v", err)
	}
	if _, err := os.Stat(stale.Dir); err != nil {
		t.Fatalf("unrelated entry stat = %v, want it untouched", err)
	}
}

func TestResolveSignedAppCopyExecConcurrentResolvesConverge(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	store := Store{Root: t.TempDir()}
	desc := copyExecDescriptor(dir, "12.15.3")

	const resolvers = 8
	paths := make([]string, resolvers)
	errs := make([]error, resolvers)
	var wg sync.WaitGroup
	for i := range resolvers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			paths[i], errs[i] = store.Resolve(context.Background(), desc)
		}()
	}
	wg.Wait()
	for i := range resolvers {
		if errs[i] != nil {
			t.Fatalf("resolver %d: %v", i, errs[i])
		}
		if paths[i] != paths[0] {
			t.Fatalf("resolver %d path = %q, want %q", i, paths[i], paths[0])
		}
	}
	entries, err := store.CacheEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !regular(paths[0]) {
		t.Fatalf("%q is not a regular file", paths[0])
	}
}

func TestResolveSignedAppCopyExecKeepsManualUpgradePaths(t *testing.T) {
	tests := []struct {
		name string
		desc func(t *testing.T) *Descriptor
		got  string
	}{
		{"version mismatch", func(t *testing.T) *Descriptor {
			dir := t.TempDir()
			writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
			return copyExecDescriptor(dir, "12.15.4")
		}, "12.15.3"},
		{"missing app", func(t *testing.T) *Descriptor {
			return copyExecDescriptor(t.TempDir(), "12.15.3")
		}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := Store{Root: t.TempDir()}

			_, err := store.Resolve(context.Background(), tt.desc(t))
			var upgrade *ManualUpgradeError
			if !errors.As(err, &upgrade) || upgrade.Got != tt.got || upgrade.Cask != "captain-hook" {
				t.Fatalf("Resolve() = %v, want ManualUpgradeError with Got %q", err, tt.got)
			}
			if _, err := os.Stat(store.CacheDir()); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cache dir stat = %v, want ErrNotExist", err)
			}
		})
	}
}
