// Package realhome resolves the invoking user's home directory through the
// passwd database, so a sandboxed caller environment — Homebrew postinstall's
// temp HOME — cannot redirect durable machine state.
package realhome

import (
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"sync"
)

// EnvOverride relocates every daemonkit home-derived path when set. It exists
// for test harnesses — Dir never reads HOME, so sandboxing tests set this
// instead — and is honored in production with a logged warning.
const EnvOverride = "DAEMONKIT_HOME"

var warnOverride sync.Once

// Dir returns EnvOverride when set, else the passwd home of the current uid.
func Dir() (string, error) {
	if override := os.Getenv(EnvOverride); override != "" {
		warnOverride.Do(func() {
			slog.Warn("realhome: "+EnvOverride+" overrides the passwd home", "dir", override)
		})
		return override, nil
	}
	current, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("realhome: resolve passwd entry: %w", err)
	}
	if current.HomeDir == "" {
		return "", fmt.Errorf("realhome: passwd entry for uid %s has no home directory", current.Uid)
	}
	return current.HomeDir, nil
}
