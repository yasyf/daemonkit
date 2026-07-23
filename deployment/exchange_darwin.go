//go:build darwin

package deployment

import (
	"os"

	"golang.org/x/sys/unix"
)

func exchangePaths(leftDir *os.File, left string, rightDir *os.File, right string) error {
	return unix.RenameatxNp(
		int(leftDir.Fd()), left, int(rightDir.Fd()), right, unix.RENAME_SWAP|unix.RENAME_NOFOLLOW_ANY,
	)
}

func publishExclusive(sourceDir *os.File, source string, targetDir *os.File, target string) error {
	return unix.RenameatxNp(
		int(sourceDir.Fd()), source, int(targetDir.Fd()), target, unix.RENAME_EXCL|unix.RENAME_NOFOLLOW_ANY,
	)
}
