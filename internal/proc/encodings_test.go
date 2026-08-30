package proc

import "testing"

func TestMicroStampGolden(t *testing.T) {
	if got := microStamp(1234567, 42); got != 1_234_567_000_042 {
		t.Fatalf("microStamp(1234567, 42) = %d, want 1234567000042", got)
	}
}
