//go:build darwin

package deployment

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

type codesignVerifier struct{}

func newVerifier() Verifier { return codesignVerifier{} }

// Verify checks every architecture against the designated requirement and the
// strict resource seal, then returns the canonical CDHash reported by codesign.
func (codesignVerifier) Verify(ctx context.Context, appPath, requirement string) (string, error) {
	cmd := exec.CommandContext(ctx, "codesign", "--verify", "--strict", "--all-architectures", "-R="+requirement, appPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("%w: codesign --verify %q: %w: %s", ErrUntrusted, appPath, err, strings.TrimSpace(string(out)))
		}
		return "", fmt.Errorf("codesign --verify %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	cmd = exec.CommandContext(ctx, "codesign", "-d", "--verbose=4", appPath)
	out, err = cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("codesign -d %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	for _, line := range strings.Split(string(out), "\n") {
		if cdHash, ok := strings.CutPrefix(strings.TrimSpace(line), "CDHash="); ok {
			cdHash = strings.ToLower(cdHash)
			if validCDHash(cdHash) {
				return cdHash, nil
			}
			return "", fmt.Errorf("codesign -d %q returned invalid CDHash %q", appPath, cdHash)
		}
	}
	return "", fmt.Errorf("codesign -d %q returned no CDHash", appPath)
}
