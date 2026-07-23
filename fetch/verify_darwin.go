//go:build darwin

package fetch

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

type codesignVerifier struct{}

func newVerifier() Verifier { return codesignVerifier{} }

// Verify runs `codesign --verify -R=<requirement> <appPath>`; codesign exits
// non-zero when the bundle is unsigned, tampered, or fails the requirement.
// Plain --verify already checks the full resource seal — nested code by cdhash,
// every other file by content hash — so --deep/--strict add nothing here.
func (codesignVerifier) Verify(ctx context.Context, appPath, requirement string) error {
	cmd := exec.CommandContext(ctx, "codesign", "--verify", "-R="+requirement, appPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("%w: codesign --verify %q: %w: %s", ErrUntrusted, appPath, err, strings.TrimSpace(string(out)))
		}
		return fmt.Errorf("codesign --verify %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	return nil
}
