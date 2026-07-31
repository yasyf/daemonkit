//go:build !darwin

package proc

import (
	"fmt"
	"os"
	"strconv"
)

func processIDs() ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("enumerate process table: %w", err)
	}
	pids := make([]int, 0, len(entries))
	for _, entry := range entries {
		if pid, err := strconv.Atoi(entry.Name()); err == nil && pid > 1 {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
