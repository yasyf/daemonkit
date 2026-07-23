//go:build !darwin

package fetch

import (
	"context"
	"errors"
)

// ErrUnsupported is returned by the non-darwin verifier: codesign is macOS-only.
var ErrUnsupported = errors.New("fetch: codesign verification is only supported on macOS")

type unsupportedVerifier struct{}

func newVerifier() Verifier { return unsupportedVerifier{} }

func (unsupportedVerifier) Verify(context.Context, string, string) error {
	return ErrUnsupported
}
