package proc

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// MonotonicUptime returns the kernel monotonic clock for the current boot.
// It is independent of wall-clock changes and comparable across processes.
func MonotonicUptime() (time.Duration, error) {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &value); err != nil {
		return 0, fmt.Errorf("clock_gettime(CLOCK_MONOTONIC): %w", err)
	}
	if value.Sec < 0 || value.Nsec < 0 {
		return 0, fmt.Errorf("clock_gettime(CLOCK_MONOTONIC): negative value")
	}
	return time.Duration(value.Sec)*time.Second + time.Duration(value.Nsec), nil
}
