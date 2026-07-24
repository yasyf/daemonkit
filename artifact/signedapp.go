package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/deployment"
	dkversion "github.com/yasyf/daemonkit/version"
)

type deployer interface {
	Deploy(context.Context, deployment.Config) (deployment.DeploymentReceipt, error)
}

type realDeployer struct{}

func (realDeployer) Deploy(ctx context.Context, cfg deployment.Config) (deployment.DeploymentReceipt, error) {
	return deployment.New().Deploy(ctx, cfg)
}

func (s Store) resolveSignedApp(ctx context.Context, desc *Descriptor, version string, o options) (string, error) {
	exec := desc.App.Exec
	if exec == "" {
		exec = filepath.Join("Contents", "MacOS", desc.App.AppName)
	}
	if o.signedApp != nil && !desc.Version.Dynamic() {
		return s.deploySignedApp(ctx, desc, version, exec, o)
	}
	return attestSignedApp(desc, version, exec)
}

func attestSignedApp(desc *Descriptor, version, exec string) (string, error) {
	appPath, err := safeJoin(desc.App.Dir, desc.App.AppName+".app")
	if err != nil {
		return "", err
	}
	want := ""
	if !desc.Version.Dynamic() {
		want = version
	}
	if _, err := os.Stat(appPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", &ManualUpgradeError{Name: desc.Name, Cask: desc.App.Cask, Want: want}
		}
		return "", fmt.Errorf("artifact: inspect installed app: %w", err)
	}
	if want != "" {
		installed, err := bundle.ShortVersion(appPath)
		if err != nil {
			return "", fmt.Errorf("artifact: read installed app version: %w", err)
		}
		if !dkversion.Equal(installed, want) {
			return "", &ManualUpgradeError{Name: desc.Name, Cask: desc.App.Cask, Want: want, Got: installed}
		}
	}
	entrypoint, err := safeJoin(appPath, exec)
	if err != nil {
		return "", err
	}
	if !regular(entrypoint) {
		return "", fmt.Errorf("%w: installed app entrypoint %q missing", ErrInvalidDescriptor, exec)
	}
	return entrypoint, nil
}

func (s Store) deploySignedApp(ctx context.Context, desc *Descriptor, version, exec string, o options) (string, error) {
	platform, err := CurrentPlatform()
	if err != nil {
		return "", err
	}
	entry, ok := desc.Platforms[platform]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedPlatform, platform)
	}
	url, err := entry.Providers[0].URL()
	if err != nil {
		return "", err
	}
	sha, err := deployment.ParseSHA256(entry.Digest)
	if err != nil {
		return "", fmt.Errorf("artifact: parse app digest: %w", err)
	}
	cfg := *o.signedApp
	cfg.Dir = desc.App.Dir
	cfg.AppName = desc.App.AppName
	cfg.Release = deployment.Release{Version: version, URL: url, SHA256: sha}
	receipt, err := o.deployer.Deploy(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("artifact: deploy signed app %q: %w", desc.Name, err)
	}
	current, ok := receipt.Current()
	if !ok {
		return "", fmt.Errorf("artifact: signed app %q deploy produced no active generation", desc.Name)
	}
	return safeJoin(current.Path, exec)
}
