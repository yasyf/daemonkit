//go:build darwin

package proc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func bootSession() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", fmt.Errorf("sysctl kern.boottime: %w", err)
	}
	return fmt.Sprintf("%d.%06d", tv.Sec, tv.Usec), nil
}
