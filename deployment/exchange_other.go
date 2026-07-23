//go:build !darwin && !linux

package deployment

import (
	"errors"
	"os"
)

func exchangePaths(*os.File, string, *os.File, string) error {
	return errors.New("deployment: atomic directory exchange is unsupported on this platform")
}

func publishExclusive(*os.File, string, *os.File, string) error {
	return errors.New("deployment: exclusive atomic rename is unsupported on this platform")
}
