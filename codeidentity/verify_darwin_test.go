//go:build darwin && !daemonkit_unsigned

package codeidentity

import (
	"errors"
	"testing"
)

func TestCodeStatusRequiresHardenedRuntimeAndProvenLibraryValidation(t *testing.T) {
	for _, flags := range []int64{
		csRuntime | csRequireLV,
		csRuntime | csForcedLV,
		csRuntime | csRequireLV | csForcedLV,
	} {
		if err := checkCodeStatus(flags); err != nil {
			t.Errorf("checkCodeStatus(0x%x) = %v, want nil", flags, err)
		}
	}
}

func TestCodeStatusRejectsUnsafeOrUnprovenPosture(t *testing.T) {
	for _, flags := range []int64{
		csRequireLV,
		csRuntime | csRequireLV | csGetTaskAllow,
		csRuntime | csRequireLV | csDebugged,
		csRuntime,
	} {
		err := checkCodeStatus(flags)
		if !errors.Is(err, ErrUntrustedPeer) {
			t.Errorf("checkCodeStatus(0x%x) = %v, want ErrUntrustedPeer", flags, err)
		}
	}
}
