package proc

import "testing"

func TestParseBootSession(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want uint64
	}{
		{name: "uuid", raw: "01234567-89ab-cdef-0123-456789abcdef", want: 0x0123456789abcdef},
		{name: "linux newline", raw: "01234567-89ab-cdef-0123-456789abcdef\n", want: 0x0123456789abcdef},
		{name: "uppercase", raw: "01234567-89AB-CDEF-0123-456789ABCDEF", want: 0x0123456789abcdef},
		{name: "unhyphenated", raw: "0123456789abcdef0123456789abcdef", want: 0x0123456789abcdef},
		{name: "different boot", raw: "fedcba98-7654-3210-fedc-ba9876543210", want: 0xfedcba9876543210},
		{name: "empty"},
		{name: "invalid hex", raw: "01234567-89ab-cdef-0123-456789abcdeg"},
		{name: "short", raw: "01234567-89ab-cdef-0123-456789abcd"},
		{name: "long", raw: "01234567-89ab-cdef-0123-456789abcdef00"},
		{name: "zero", raw: "00000000-0000-0000-0000-000000000000"},
		{name: "zero folded prefix", raw: "00000000-0000-0000-0123-456789abcdef"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBootSession(tt.raw, "test boot uuid")
			if tt.want == 0 {
				if err == nil || got != 0 {
					t.Fatalf("parseBootSession(%q) = %d, %v, want zero and an error", tt.raw, got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("parseBootSession(%q) = %d, %v, want %d and no error", tt.raw, got, err, tt.want)
			}
		})
	}
}
