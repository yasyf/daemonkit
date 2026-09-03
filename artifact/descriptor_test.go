package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
)

func TestParseToleratesShebang(t *testing.T) {
	body := []byte(`#!/usr/bin/env binrun
{"schema":1,"name":"tool","kind":"python-tool","version":{"static":"1.2.3"},"tool":{"dist":"tool"}}`)
	desc, err := Parse(body)
	if err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	if desc.Name != "tool" || desc.Kind != PythonTool || desc.Version.Static != "1.2.3" {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestValidate(t *testing.T) {
	rbEntry := map[Platform]PlatformEntry{
		"macos-aarch64": {
			Size: 10, Hash: "sha256", Digest: strings.Repeat("a", 64), Format: Raw, Path: "tool",
			Providers: []Provider{{Type: GitHubRelease, Repo: "o/r", Tag: "v1", Name: "a"}},
		},
	}
	tests := []struct {
		name string
		desc Descriptor
		want error
	}{
		{"release-binary static", Descriptor{Schema: 1, Name: "t", Kind: ReleaseBinary, Version: VersionSource{Static: "1"}, Platforms: rbEntry}, nil},
		{"python-tool dynamic ok", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Command: []string{"host"}, JSONField: "build"}, Tool: &ToolSpec{Dist: "t"}}, nil},
		{"signed-app dynamic ok", Descriptor{Schema: 1, Name: "t", Kind: SignedApp, Version: VersionSource{Command: []string{"host"}, JSONField: "build"}, App: &AppSpec{Dir: "/Applications", AppName: "T"}}, nil},
		{"wrong schema", Descriptor{Schema: 2, Name: "t", Kind: PythonTool, Version: VersionSource{Static: "1"}, Tool: &ToolSpec{Dist: "t"}}, ErrSchemaVersion},
		{"dynamic release-binary refused", Descriptor{Schema: 1, Name: "t", Kind: ReleaseBinary, Version: VersionSource{Command: []string{"host"}, JSONField: "build"}, Platforms: rbEntry}, ErrDynamicIntegrity},
		{"missing name", Descriptor{Schema: 1, Kind: PythonTool, Version: VersionSource{Static: "1"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"version both static and command", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Static: "1", Command: []string{"host"}, JSONField: "b"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"dynamic missing json_field", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Command: []string{"host"}}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"python-tool file ok", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "/A.app/Contents/Info.plist", PlistKey: "CFBundleShortVersionString"}, Tool: &ToolSpec{Dist: "t"}}, nil},
		{"python-tool home-relative file ok", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "~/A.app/Contents/Info.plist", PlistKey: "CFBundleShortVersionString"}, Tool: &ToolSpec{Dist: "t"}}, nil},
		{"file release-binary refused", Descriptor{Schema: 1, Name: "t", Kind: ReleaseBinary, Version: VersionSource{File: "/v.json", JSONField: "build"}, Platforms: rbEntry}, ErrDynamicIntegrity},
		{"version both file and command", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "/v.json", Command: []string{"host"}, JSONField: "build"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"file with no extractor", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "/v.json"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"file with both extractors", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "/v.json", JSONField: "build", PlistKey: "K"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"relative file", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{File: "Info.plist", PlistKey: "K"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"plist_key on a command", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Command: []string{"host"}, JSONField: "build", PlistKey: "K"}, Tool: &ToolSpec{Dist: "t"}}, ErrInvalidDescriptor},
		{"python-tool missing dist", Descriptor{Schema: 1, Name: "t", Kind: PythonTool, Version: VersionSource{Static: "1"}, Tool: &ToolSpec{}}, ErrInvalidDescriptor},
		{"signed-app missing app", Descriptor{Schema: 1, Name: "t", Kind: SignedApp, Version: VersionSource{Static: "1"}}, ErrInvalidDescriptor},
		{"unknown kind", Descriptor{Schema: 1, Name: "t", Kind: "mystery", Version: VersionSource{Static: "1"}}, ErrInvalidDescriptor},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.desc.Validate()
			if tt.want == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestProviderURLEscapesAssetName(t *testing.T) {
	got, err := (Provider{Type: GitHubRelease, Repo: "yasyf/x", Tag: "v1.0.0", Name: "Captain Hook.zip"}).URL()
	if err != nil {
		t.Fatalf("URL() = %v", err)
	}
	want := "https://github.com/yasyf/x/releases/download/v1.0.0/Captain%20Hook.zip"
	if got != want {
		t.Fatalf("URL() = %q, want %q", got, want)
	}
}

func TestResolveVersionStatic(t *testing.T) {
	desc := &Descriptor{Version: VersionSource{Static: "9.9.9"}}
	got, err := desc.ResolveVersion(context.Background())
	if err != nil || got != "9.9.9" {
		t.Fatalf("ResolveVersion() = %q, %v; want 9.9.9, nil", got, err)
	}
}

func TestResolveVersionDynamic(t *testing.T) {
	desc := &Descriptor{Version: VersionSource{
		Command:   []string{"/bin/sh", "-c", `printf '{"build":"12.15.3","other":1}'`},
		JSONField: "build",
	}}
	got, err := desc.ResolveVersion(context.Background())
	if err != nil || got != "12.15.3" {
		t.Fatalf("ResolveVersion() = %q, %v; want 12.15.3, nil", got, err)
	}
}

func TestResolveVersionDynamicMissingField(t *testing.T) {
	desc := &Descriptor{Version: VersionSource{
		Command:   []string{"/bin/sh", "-c", `printf '{"other":1}'`},
		JSONField: "build",
	}}
	if _, err := desc.ResolveVersion(context.Background()); err == nil {
		t.Fatal("ResolveVersion() = nil, want error on missing field")
	}
}

func TestCurrentPlatform(t *testing.T) {
	got, err := CurrentPlatform()
	if err != nil {
		t.Fatalf("CurrentPlatform() = %v", err)
	}
	if got == "" {
		t.Fatal("CurrentPlatform() is empty")
	}
}

func writeVersionFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const versionPlist = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
  <key>CFBundleShortVersionString</key><string>12.22.5</string>
  <key>CaptHookBuild</key><string>v12.22.5</string>
</dict></plist>
`

func TestResolveVersionFile(t *testing.T) {
	plistPath := writeVersionFile(t, "Info.plist", versionPlist)
	jsonPath := writeVersionFile(t, "version.json", `{"build":"12.22.5","schema":1}`)
	tests := []struct {
		name    string
		version VersionSource
		want    string
		wantErr bool
	}{
		{"plist key", VersionSource{File: plistPath, PlistKey: "CFBundleShortVersionString"}, "12.22.5", false},
		{"another plist key", VersionSource{File: plistPath, PlistKey: "CaptHookBuild"}, "v12.22.5", false},
		{"json field", VersionSource{File: jsonPath, JSONField: "build"}, "12.22.5", false},
		{"missing plist key", VersionSource{File: plistPath, PlistKey: "NoSuchKey"}, "", true},
		{"missing json field", VersionSource{File: jsonPath, JSONField: "nope"}, "", true},
		{"json field is not a string", VersionSource{File: jsonPath, JSONField: "schema"}, "", true},
		{"absent file", VersionSource{File: filepath.Join(t.TempDir(), "gone.plist"), PlistKey: "CFBundleShortVersionString"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&Descriptor{Version: tt.version}).ResolveVersion(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ResolveVersion() = %q, want an error", got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ResolveVersion() = %q, %v; want %q, nil", got, err, tt.want)
			}
		})
	}
}

func TestResolveVersionFileSpawnsNoProcess(t *testing.T) {
	path := writeVersionFile(t, "Info.plist", versionPlist)
	desc := &Descriptor{Version: VersionSource{File: path, PlistKey: "CFBundleShortVersionString"}}

	before := runtime.NumGoroutine()
	for range 100 {
		if _, err := desc.ResolveVersion(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// os/exec leaks a wait goroutine per spawned child until it is reaped; a
	// hundred file reads must not move the count at all.
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines = %d after 100 resolutions, was %d: the file source spawned a process", after, before)
	}
}

func TestResolveVersionFileExpandsHomeThroughPasswd(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	t.Setenv("HOME", t.TempDir())
	if err := os.WriteFile(filepath.Join(home, "Info.plist"), []byte(versionPlist), 0o600); err != nil {
		t.Fatal(err)
	}

	desc := &Descriptor{Version: VersionSource{File: "~/Info.plist", PlistKey: "CFBundleShortVersionString"}}
	got, err := desc.ResolveVersion(context.Background())
	if err != nil || got != "12.22.5" {
		t.Fatalf("ResolveVersion() = %q, %v; want 12.22.5, nil", got, err)
	}
}

func TestFileVersionIsHostAuthoritative(t *testing.T) {
	source := VersionSource{File: "/tmp/Info.plist", PlistKey: "CFBundleShortVersionString"}
	if !source.Dynamic() {
		t.Fatal("Dynamic() = false for a file source; a host-read version carries no baked digest")
	}
}
