package deployment

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
)

type stateLocation struct {
	Dir     string
	AppName string
}

func (l stateLocation) validate() error {
	if err := l.validateLexical(); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(l.Dir)
	if err != nil {
		return fmt.Errorf("%w: resolve install dir: %w", ErrInvalidConfig, err)
	}
	if resolved != l.Dir {
		return fmt.Errorf("%w: install dir must not contain symlink ancestors", ErrInvalidConfig)
	}
	return nil
}

func (l stateLocation) validateAllowMissing() error {
	if err := l.validateLexical(); err != nil {
		return err
	}
	for path := l.Dir; ; path = filepath.Dir(path) {
		info, err := os.Lstat(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect install dir ancestor %s: %w", ErrInvalidConfig, path, err)
		}
		if err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
			return fmt.Errorf("%w: install dir ancestor %s is not a real directory", ErrInvalidConfig, path)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
	}
}

func (l stateLocation) validateLexical() error {
	if l.Dir == "" || !filepath.IsAbs(l.Dir) || filepath.Clean(l.Dir) != l.Dir || l.Dir == string(filepath.Separator) {
		return fmt.Errorf("%w: install dir must be exact, absolute, and non-root", ErrInvalidConfig)
	}
	if l.AppName == "" || l.AppName == "." || l.AppName == ".." ||
		filepath.Base(l.AppName) != l.AppName || strings.HasSuffix(l.AppName, ".app") {
		return fmt.Errorf("%w: app name must be a basename without .app", ErrInvalidConfig)
	}
	if !within(l.Dir, filepath.Join(l.Dir, ".daemonkit-deployment", l.AppName)) ||
		!within(l.Dir, bundle.AppPath(l.Dir, l.AppName)) {
		return fmt.Errorf("%w: app paths escape install dir", ErrInvalidConfig)
	}
	return nil
}

func (l stateLocation) stopControlStore() (*proc.FileStore, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	paths := deploymentPathsForLocation(l)
	return &proc.FileStore{Path: paths.serviceProcess}, nil
}

var runtimeExecutable = service.CanonicalExecutable

// RuntimeStopControlStore derives the containing fixed app from the running
// direct-child executable and returns the exact file-backed authority store
// used by its deployment-owned service controller.
func RuntimeStopControlStore() (*proc.FileStore, error) {
	executable, err := runtimeExecutable()
	if err != nil {
		return nil, fmt.Errorf("deployment: resolve runtime executable: %w", err)
	}
	macOS := filepath.Dir(executable)
	contents := filepath.Dir(macOS)
	app := filepath.Dir(contents)
	if filepath.Base(macOS) != "MacOS" || filepath.Base(contents) != "Contents" ||
		filepath.Dir(executable) != macOS || !strings.HasSuffix(filepath.Base(app), ".app") {
		return nil, errors.New("deployment: runtime executable is not a direct child of an app Contents/MacOS directory")
	}
	if err := requireRealDirectory(app); err != nil {
		return nil, err
	}
	location := stateLocation{
		Dir: filepath.Dir(app), AppName: strings.TrimSuffix(filepath.Base(app), ".app"),
	}
	return location.stopControlStore()
}
