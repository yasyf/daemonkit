//go:build !darwin

package deployment

import (
	"context"
	"errors"
)

// ErrUnsupported is returned by the non-darwin verifier: codesign is macOS-only.
var ErrUnsupported = errors.New("deployment: codesign verification is only supported on macOS")

type unsupportedVerifier struct{}

func newVerifier() Verifier { return unsupportedVerifier{} }

func (unsupportedVerifier) Verify(context.Context, string, string) (string, error) {
	return "", ErrUnsupported
}
