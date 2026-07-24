package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/bundle"
	dkversion "github.com/yasyf/daemonkit/version"
)

func (s Store) resolveSignedApp(ctx context.Context, desc *Descriptor, version string, o options) (string, error) {
	exec := desc.App.Exec
	if exec == "" {
		exec = filepath.Join("Contents", "MacOS", desc.App.AppName)
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
