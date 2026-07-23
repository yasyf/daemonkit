//go:build !darwin && !linux

package fetch

import (
	"errors"
	"os"
)

func openDirectoryNoFollow(string) (*os.File, error) {
	return nil, errors.New("fetch: no-follow directory opens are unsupported on this platform")
}
