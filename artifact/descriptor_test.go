package artifact

import (
	"context"
	"errors"
	"strings"
	"testing"
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
