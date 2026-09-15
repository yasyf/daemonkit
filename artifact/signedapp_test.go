package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

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

func TestResolveSignedAppCopyExecHitNeverOpensSource(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	store := Store{Root: t.TempDir()}
	desc := copyExecDescriptor(dir, "12.15.3")

	first, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("first Resolve() = %v", err)
	}
	openEntrypoint = func(path string) (*os.File, error) {
		return nil, fmt.Errorf("warm resolve opened %q", path)
	}
	t.Cleanup(func() { openEntrypoint = os.Open })

	second, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("second Resolve() = %v, want a hit that never opens the source", err)
	}
	if second != first {
		t.Fatalf("second path = %q, want %q", second, first)
	}
}

func backdateEntry(t *testing.T, dir string) {
	t.Helper()
	meta := readMeta(t, dir)
	meta.FetchedAt = time.Now().UTC().Add(-2 * execCopyRetention)
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func seedExecCopy(t *testing.T, store Store, source, digest string, fetched time.Time) string {
	t.Helper()
	dir := filepath.Join(store.CacheDir(), digest[:2], digest)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, filepath.Base(source)), []byte("#!/bin/sh\necho seeded\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(cacheMeta{Name: "Captain Hook", Tag: "12.0.0", Digest: digest, Source: source, FetchedAt: fetched})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestResolveSignedAppCopyExecUpgradeMintsNewEntry(t *testing.T) {
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
		{"same-length rewrite with mtime preserved", func(t *testing.T, dir string) *Descriptor {
			source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
			before, err := os.Stat(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(source, []byte("#!/bin/SH\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(source, before.ModTime(), before.ModTime()); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(source)
			if err != nil {
				t.Fatal(err)
			}
			if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
				t.Fatalf("rewrite changed size or mtime: %d/%v -> %d/%v", before.Size(), before.ModTime(), after.Size(), after.ModTime())
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
			if !regular(old) {
				t.Fatalf("fresh prior entry %q was pruned, want it kept for %v", old, execCopyRetention)
			}
		})
	}
}

func TestResolveSignedAppCopyExecPrunesOnlyOldEntriesOnMiss(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	store := Store{Root: t.TempDir()}
	desc := copyExecDescriptor(dir, "12.15.3")

	first, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	stale := seedExecCopy(t, store, source, strings.Repeat("c", 64), time.Now().UTC().Add(-2*execCopyRetention))
	fresh := seedExecCopy(t, store, source, strings.Repeat("d", 64), time.Now().UTC())
	unrelated := seedCacheEntry(t, store, "tool", "v1", strings.Repeat("e", 64))

	hit, err := store.Resolve(context.Background(), desc)
	if err != nil {
		t.Fatalf("warm Resolve() = %v", err)
	}
	if hit != first {
		t.Fatalf("warm path = %q, want %q", hit, first)
	}
	for _, dir := range []string{stale, fresh, unrelated.Dir} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("hit pruned %q: %v", dir, err)
		}
	}

	backdateEntry(t, filepath.Dir(first))
	writeApp(t, dir, "Captain Hook", "12.16.0", "Contents/Helpers/capt-hookd")
	current, err := store.Resolve(context.Background(), copyExecDescriptor(dir, "12.16.0"))
	if err != nil {
		t.Fatalf("Resolve() after upgrade = %v", err)
	}
	for _, dir := range []string{stale, filepath.Dir(first)} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("miss kept old entry %q: %v", dir, err)
		}
	}
	for _, dir := range []string{fresh, unrelated.Dir, filepath.Dir(current)} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("miss pruned %q: %v", dir, err)
		}
	}
}

func TestCopyExecutableRefusesChangedSource(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "capt-hookd")
	if err := os.WriteFile(source, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(source, &stat); err != nil {
		t.Fatal(err)
	}
	attested := identify(&stat)
	attested.ChangeTimeNS++

	_, err := copyExecutable(source, filepath.Join(dir, "copy"), attested)
	if !errors.Is(err, ErrEntrypointChanged) {
		t.Fatalf("copyExecutable() = %v, want ErrEntrypointChanged", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("dir = %v, want only the source left behind", entries)
	}
}

func buildSignedFixture(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	sources := map[string]string{
		"go.mod":  "module fixture\n\ngo 1.22\n",
		"main.go": "package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc main() { fmt.Println(os.Args[1]) }\n",
	}
	for name, body := range sources {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	bin := filepath.Join(dir, "fixture")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off", "GOTOOLCHAIN=local")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build fixture: %v\n%s", err, out)
	}
	if out, err := exec.Command("codesign", "--force", "--sign", "-", bin).CombinedOutput(); err != nil {
		t.Fatalf("codesign fixture: %v\n%s", err, out)
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestResolveSignedAppCopyExecRelocatesSignedMachO(t *testing.T) {
	dir := t.TempDir()
	writeApp(t, dir, "Captain Hook", "12.15.3", "Contents/Helpers/capt-hookd")
	source := filepath.Join(dir, "Captain Hook.app", "Contents", "Helpers", "capt-hookd")
	if err := os.WriteFile(source, buildSignedFixture(t), 0o755); err != nil {
		t.Fatal(err)
	}
	store := Store{Root: t.TempDir()}

	path, err := store.Resolve(context.Background(), copyExecDescriptor(dir, "12.15.3"))
	if err != nil {
		t.Fatalf("Resolve() = %v", err)
	}
	if out, err := exec.Command("codesign", "--verify", "--strict", "--verbose=2", path).CombinedOutput(); err != nil {
		t.Fatalf("codesign --verify %q: %v\n%s", path, err, out)
	}
	out, err := exec.Command(path, "relocated").Output()
	if err != nil {
		t.Fatalf("run %q: %v", path, err)
	}
	if string(out) != "relocated\n" {
		t.Fatalf("output = %q, want relocated", out)
	}
}
