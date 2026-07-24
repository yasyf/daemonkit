package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"time"
)

const versionCommandTimeout = 10 * time.Second

// Kind selects the materialization backend a descriptor uses.
type Kind string

// The kinds a descriptor may declare.
const (
	ReleaseBinary Kind = "release-binary"
	PythonTool    Kind = "python-tool"
	SignedApp     Kind = "signed-app"
)

// Platform is a dotslash platform key such as "macos-aarch64".
type Platform string

// Format is the packaging of a downloaded artifact.
type Format string

// The archive formats a downloaded artifact may use.
const (
	Raw   Format = ""       // a single executable file
	TarGz Format = "tar.gz" // a gzip-compressed tar archive
	Zip   Format = "zip"    // a zip archive
)

// ProviderType names an artifact source. Only github-release is defined in v1.
type ProviderType string

// GitHubRelease is the only provider type defined in v1.
const GitHubRelease ProviderType = "github-release"

// Descriptor is a parsed, schema-v1 binrun descriptor.
type Descriptor struct {
	Schema    int                        `json:"schema"`
	Name      string                     `json:"name"`
	Kind      Kind                       `json:"kind"`
	Version   VersionSource              `json:"version"`
	Platforms map[Platform]PlatformEntry `json:"platforms,omitempty"`
	Tool      *ToolSpec                  `json:"tool,omitempty"`
	App       *AppSpec                   `json:"app,omitempty"`
}

// VersionSource is a baked Static version or a dynamic host-authority Command.
// Exactly one is set. Command's stdout is JSON; JSONField names the field the
// version is read from.
type VersionSource struct {
	Static    string   `json:"static,omitempty"`
	Command   []string `json:"command,omitempty"`
	JSONField string   `json:"json_field,omitempty"`
}

// Dynamic reports whether the version is resolved from a host command.
func (v VersionSource) Dynamic() bool { return len(v.Command) > 0 }

// PlatformEntry is one platform's baked artifact for a release-binary or a
// static signed-app. Digest is empty only for a dynamic template.
type PlatformEntry struct {
	Size      int64      `json:"size"`
	Hash      string     `json:"hash"`
	Digest    string     `json:"digest,omitempty"`
	Format    Format     `json:"format,omitempty"`
	Path      string     `json:"path"`
	Providers []Provider `json:"providers"`
}

// Provider locates a platform artifact.
type Provider struct {
	Type ProviderType `json:"type"`
	Repo string       `json:"repo,omitempty"`
	Tag  string       `json:"tag,omitempty"`
	Name string       `json:"name,omitempty"`
}

// ToolSpec is the python-tool payload: a PyPI distribution and its entrypoint.
type ToolSpec struct {
	Dist       string `json:"dist"`
	Entrypoint string `json:"entrypoint,omitempty"`
}

// AppSpec is the signed-app payload.
type AppSpec struct {
	Dir     string `json:"dir"`
	AppName string `json:"app_name"`
	Exec    string `json:"exec,omitempty"`
	Cask    string `json:"cask,omitempty"`
}

// CurrentPlatform returns the dotslash platform key for the running host.
func CurrentPlatform() (Platform, error) {
	var name string
	switch runtime.GOOS {
	case "darwin":
		name = "macos"
	case "linux":
		name = "linux"
	case "windows":
		name = "windows"
	default:
		return "", fmt.Errorf("%w: os %q", ErrUnsupportedPlatform, runtime.GOOS)
	}
	switch runtime.GOARCH {
	case "arm64":
		return Platform(name + "-aarch64"), nil
	case "amd64":
		return Platform(name + "-x86_64"), nil
	default:
		return "", fmt.Errorf("%w: arch %q", ErrUnsupportedPlatform, runtime.GOARCH)
	}
}

// URL is the concrete download URL for the provider.
func (p Provider) URL() (string, error) {
	if p.Type != GitHubRelease {
		return "", fmt.Errorf("%w: unsupported provider type %q", ErrInvalidDescriptor, p.Type)
	}
	location := url.URL{
		Scheme: "https",
		Host:   "github.com",
		Path:   fmt.Sprintf("/%s/releases/download/%s/%s", p.Repo, p.Tag, p.Name),
	}
	return location.String(), nil
}

// ParseFile reads and parses a descriptor file.
func ParseFile(path string) (*Descriptor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("artifact: read descriptor: %w", err)
	}
	return Parse(data)
}

// Parse parses a descriptor body, tolerating a leading "#!" shebang line, and
// validates it.
func Parse(data []byte) (*Descriptor, error) {
	body := data
	if bytes.HasPrefix(body, []byte("#!")) {
		if idx := bytes.IndexByte(body, '\n'); idx >= 0 {
			body = body[idx+1:]
		} else {
			body = nil
		}
	}
	var desc Descriptor
	if err := json.Unmarshal(body, &desc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDescriptor, err)
	}
	if err := desc.Validate(); err != nil {
		return nil, err
	}
	return &desc, nil
}

// Validate enforces schema-v1 invariants, including the supply-chain rule that a
// dynamic version is valid only for python-tool and signed-app.
func (d *Descriptor) Validate() error {
	if d.Schema != 1 {
		return fmt.Errorf("%w: got %d", ErrSchemaVersion, d.Schema)
	}
	if d.Name == "" {
		return fmt.Errorf("%w: missing name", ErrInvalidDescriptor)
	}
	if err := d.Version.validate(); err != nil {
		return err
	}
	switch d.Kind {
	case ReleaseBinary:
		if d.Version.Dynamic() {
			return fmt.Errorf("%w: release-binary %q", ErrDynamicIntegrity, d.Name)
		}
		if len(d.Platforms) == 0 {
			return fmt.Errorf("%w: release-binary %q has no platforms", ErrInvalidDescriptor, d.Name)
		}
		for platform, entry := range d.Platforms {
			if err := entry.validate(platform, false); err != nil {
				return err
			}
		}
	case PythonTool:
		if d.Tool == nil || d.Tool.Dist == "" {
			return fmt.Errorf("%w: python-tool %q missing tool.dist", ErrInvalidDescriptor, d.Name)
		}
	case SignedApp:
		if d.App == nil || d.App.Dir == "" || d.App.AppName == "" {
			return fmt.Errorf("%w: signed-app %q missing app.dir or app.app_name", ErrInvalidDescriptor, d.Name)
		}
		for platform, entry := range d.Platforms {
			if err := entry.validate(platform, d.Version.Dynamic()); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidDescriptor, d.Kind)
	}
	return nil
}

func (v VersionSource) validate() error {
	switch {
	case v.Static != "" && len(v.Command) > 0:
		return fmt.Errorf("%w: version has both static and command", ErrInvalidDescriptor)
	case v.Static == "" && len(v.Command) == 0:
		return fmt.Errorf("%w: version has neither static nor command", ErrInvalidDescriptor)
	case len(v.Command) > 0 && v.JSONField == "":
		return fmt.Errorf("%w: dynamic version missing json_field", ErrInvalidDescriptor)
	}
	return nil
}

func (e PlatformEntry) validate(platform Platform, template bool) error {
	if len(e.Providers) == 0 {
		return fmt.Errorf("%w: platform %q has no providers", ErrInvalidDescriptor, platform)
	}
	for _, provider := range e.Providers {
		if err := provider.validate(); err != nil {
			return fmt.Errorf("platform %q: %w", platform, err)
		}
	}
	if e.Path == "" {
		return fmt.Errorf("%w: platform %q missing path", ErrInvalidDescriptor, platform)
	}
	if template {
		return nil
	}
	if e.Hash != "sha256" {
		return fmt.Errorf("%w: platform %q hash %q is not sha256", ErrInvalidDescriptor, platform, e.Hash)
	}
	if len(e.Digest) != 64 || !isLowerHex(e.Digest) {
		return fmt.Errorf("%w: platform %q digest is not a lowercase sha256 hex", ErrInvalidDescriptor, platform)
	}
	if e.Size <= 0 {
		return fmt.Errorf("%w: platform %q size must be positive", ErrInvalidDescriptor, platform)
	}
	return nil
}

func isLowerHex(s string) bool {
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

func (p Provider) validate() error {
	if p.Type != GitHubRelease {
		return fmt.Errorf("%w: unsupported provider type %q", ErrInvalidDescriptor, p.Type)
	}
	if p.Repo == "" || p.Tag == "" || p.Name == "" {
		return fmt.Errorf("%w: github-release provider missing repo, tag, or name", ErrInvalidDescriptor)
	}
	return nil
}

// ResolveVersion returns the concrete version: the Static value, or the JSON
// field read from the dynamic command's stdout, bounded by ctx.
func (d *Descriptor) ResolveVersion(ctx context.Context) (string, error) {
	if !d.Version.Dynamic() {
		return d.Version.Static, nil
	}
	return d.Version.resolveCommand(ctx)
}

func (v VersionSource) resolveCommand(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, versionCommandTimeout)
	defer cancel()
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, v.Command[0], v.Command[1:]...)
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("artifact: run version command: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &fields); err != nil {
		return "", fmt.Errorf("artifact: parse version command output: %w", err)
	}
	raw, ok := fields[v.JSONField]
	if !ok {
		return "", fmt.Errorf("artifact: version command output has no field %q", v.JSONField)
	}
	var version string
	if err := json.Unmarshal(raw, &version); err != nil {
		return "", fmt.Errorf("artifact: version field %q is not a string: %w", v.JSONField, err)
	}
	if version == "" {
		return "", fmt.Errorf("artifact: version field %q is empty", v.JSONField)
	}
	return version, nil
}
