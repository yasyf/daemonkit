//go:build !darwin

package deployment

import (
	"context"
)

type unsupportedVerifier struct{}

func newVerifier() verifier { return unsupportedVerifier{} }

func (unsupportedVerifier) Verify(context.Context, string, string) (signatureAttestation, error) {
	return signatureAttestation{}, ErrUnsupported
}
