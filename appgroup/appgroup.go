// Package appgroup resolves a macOS App Group's shared container via
// -[NSFileManager containerURLForSecurityApplicationGroupIdentifier:] — the
// only prompt-free path (TCC kTCCServiceSystemPolicyAppData); a hand-built
// path join forfeits that contract and triggers a consent prompt.
package appgroup

import (
	"errors"
	"fmt"
)

// ErrNoGroupContainer means the OS reported no container for the app group.
var ErrNoGroupContainer = errors.New("no app-group container")

var resolveContainer = platformResolveContainer

// GroupContainerDir returns the App Group's shared-container path.
func GroupContainerDir(group string) (string, error) {
	dir, err := resolveContainer(group)
	if err != nil {
		return "", fmt.Errorf("app-group container %q: %w", group, err)
	}
	return dir, nil
}
