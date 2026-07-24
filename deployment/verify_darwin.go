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

func newVerifier() verifier { return codesignVerifier{} }

// Verify checks every architecture against the designated requirement and the
// strict resource seal, then returns the canonical CDHash reported by codesign.
func (codesignVerifier) Verify(ctx context.Context, appPath, requirement string) (signatureAttestation, error) {
	cmd := exec.CommandContext(ctx, "codesign", "--verify", "--strict", "--all-architectures", "-R="+requirement, appPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return signatureAttestation{}, ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return signatureAttestation{}, fmt.Errorf("%w: codesign --verify %q: %w: %s", ErrUntrusted, appPath, err, strings.TrimSpace(string(out)))
		}
		return signatureAttestation{}, fmt.Errorf("codesign --verify %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	cmd = exec.CommandContext(ctx, "codesign", "-d", "--verbose=4", appPath)
	out, err = cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return signatureAttestation{}, ctx.Err()
		}
		return signatureAttestation{}, fmt.Errorf("codesign -d %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	cdHash := ""
	for _, line := range strings.Split(string(out), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "CDHash="); ok {
			cdHash = strings.ToLower(value)
			break
		}
	}
	if !validCDHash(cdHash) {
		return signatureAttestation{}, fmt.Errorf("codesign -d %q returned invalid CDHash %q", appPath, cdHash)
	}
	cmd = exec.CommandContext(ctx, "codesign", "-d", "--entitlements", "-", "--xml", appPath)
	out, err = cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return signatureAttestation{}, ctx.Err()
		}
		return signatureAttestation{}, fmt.Errorf("codesign entitlements %q: %w: %s", appPath, err, strings.TrimSpace(string(out)))
	}
	entitlements, err := decodeEntitlementsOutput(out)
	if err != nil {
		return signatureAttestation{}, err
	}
	return signatureAttestation{CDHash: cdHash, EntitlementsDigest: entitlements}, nil
}
