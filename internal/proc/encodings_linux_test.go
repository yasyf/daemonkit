package proc

import "testing"

func TestFrozenLinuxStampEncodings(t *testing.T) {
	t.Run("start ticks", func(t *testing.T) {
		got, err := parseLegacyStart("4242")
		if err != nil || got != 4242 {
			t.Fatalf("parseLegacyStart(4242) = %d, %v, want 4242", got, err)
		}
		if _, err := parseLegacyStart("12.5"); err == nil {
			t.Fatal("parseLegacyStart accepted a non-integer tick stamp")
		}
	})
	t.Run("boot id", func(t *testing.T) {
		got, err := parseLegacyBoot("b1946ac9-2d34-4f4c-9b3a-0123456789ab")
		if err != nil {
			t.Fatalf("parseLegacyBoot() = %v", err)
		}
		if got != 0xb1946ac92d344f4c {
			t.Fatalf("parseLegacyBoot() = %#x, want 0xb1946ac92d344f4c", got)
		}
		if _, err := parseLegacyBoot("b1946ac9"); err == nil {
			t.Fatal("parseLegacyBoot accepted a truncated boot id")
		}
		if _, err := parseLegacyBoot("zz946ac9-2d34-4f4c-9b3a-0123456789ab"); err == nil {
			t.Fatal("parseLegacyBoot accepted non-hex digits")
		}
	})
}
