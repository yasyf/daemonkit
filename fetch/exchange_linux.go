//go:build linux

package fetch

import (
	"os"

	"golang.org/x/sys/unix"
)

func exchangePaths(leftDir *os.File, left string, rightDir *os.File, right string) error {
	return unix.Renameat2(int(leftDir.Fd()), left, int(rightDir.Fd()), right, unix.RENAME_EXCHANGE)
}

func publishExclusive(sourceDir *os.File, source string, targetDir *os.File, target string) error {
	return unix.Renameat2(int(sourceDir.Fd()), source, int(targetDir.Fd()), target, unix.RENAME_NOREPLACE)
}
