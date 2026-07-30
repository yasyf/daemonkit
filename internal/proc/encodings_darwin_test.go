package proc

import "testing"

func TestFrozenDarwinStampEncodings(t *testing.T) {
	tests := []struct {
		name    string
		stamp   string
		want    uint64
		wantErr bool
	}{
		{"start", "1234567.000042", 1_234_567_000_042, false},
		{"boot", "1721234567.123456", 1_721_234_567_123_456, false},
		{"zero usec", "10.000000", 10_000_000, false},
		{"missing dot", "1234567", 0, true},
		{"short usec", "1234567.42", 0, true},
		{"overlong usec", "1.1000000", 0, true},
		{"non-numeric", "abc.def012", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLegacyStart(tt.stamp)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseLegacyStart(%q) accepted a malformed stamp", tt.stamp)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLegacyStart(%q) = %v", tt.stamp, err)
			}
			if got != tt.want {
				t.Fatalf("parseLegacyStart(%q) = %d, want %d", tt.stamp, got, tt.want)
			}
			boot, err := parseLegacyBoot(tt.stamp)
			if err != nil || boot != tt.want {
				t.Fatalf("parseLegacyBoot(%q) = %d, %v, want %d", tt.stamp, boot, err, tt.want)
			}
		})
	}
}

func TestMicroStampGolden(t *testing.T) {
	if got := microStamp(1234567, 42); got != 1_234_567_000_042 {
		t.Fatalf("microStamp(1234567, 42) = %d, want 1234567000042", got)
	}
}
