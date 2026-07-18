//go:build !darwin

package proc

import (
	"fmt"
	"os"
	"strings"
)

func bootSession() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot_id: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}
