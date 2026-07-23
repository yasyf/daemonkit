//go:build !darwin && !linux

package deployment

import (
	"errors"
	"os"
)

func openDirectoryNoFollow(string) (*os.File, error) {
	return nil, errors.New("deployment: no-follow directory opens are unsupported on this platform")
}
