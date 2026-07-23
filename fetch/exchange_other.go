//go:build !darwin && !linux

package fetch

import (
	"errors"
	"os"
)

func exchangePaths(*os.File, string, *os.File, string) error {
	return errors.New("fetch: atomic directory exchange is unsupported on this platform")
}

func publishExclusive(*os.File, string, *os.File, string) error {
	return errors.New("fetch: exclusive atomic rename is unsupported on this platform")
}
