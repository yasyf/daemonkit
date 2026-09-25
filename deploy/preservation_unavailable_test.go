package deploy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit"
)

func TestPreservationUnavailableBeforeAnyDeploymentMutation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "unchanged")
	deployment := &Deployment{config: Config{Daemon: daemonkit.Daemon{ShutdownPolicy: daemonkit.PreserveOwned}}, layout: layoutFor(filepath.Join(root, "Example.app")), run: func(context.Context, string, ...string) (string, int, error) {
		t.Fatal("unavailable preservation invoked launchctl")
		return "", 0, nil
	}}
	callback := func(context.Context) error {
		t.Fatal("unavailable preservation invoked application callback")
		return nil
	}
	operations := map[string]func() error{
		"install":   func() error { _, err := deployment.Install(t.Context(), Candidate{}); return err },
		"supersede": func() error { _, err := deployment.Supersede(t.Context(), Candidate{}); return err },
		"activate":  func() error { _, err := deployment.Activate(t.Context()); return err },
		"uninstall": func() error { _, err := deployment.Uninstall(t.Context()); return err },
		"reset":     func() error { return deployment.Reset(t.Context()) },
		"quiesce":   func() error { _, err := deployment.Quiesce(t.Context()); return err },
		"replace":   func() error { _, err := deployment.Replace(t.Context(), Candidate{}, callback); return err },
		"remove":    func() error { _, err := deployment.Remove(t.Context(), callback); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, daemonkit.ErrPreservationUnavailable) {
				t.Fatalf("transition=%v", err)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("transition changed filesystem: %v", err)
			}
		})
	}
}
