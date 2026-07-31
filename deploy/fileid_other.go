//go:build !darwin && !linux

package deploy

import (
	"errors"
	"os"
)

// FileID is one bundle directory's durable device and inode identity, so a
// generation cannot be impersonated by a same-named tree at another inode.
type FileID struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

func identifyPath(string) (FileID, error) {
	return FileID{}, errors.New("deploy: durable file identity is unsupported on this platform")
}

func directoryOwner(os.FileInfo) (int, error) {
	return 0, errors.New("deploy: file ownership is unsupported on this platform")
}
