//go:build !darwin && !linux

package fetch

import (
	"errors"
	"os"
)

type fileID struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

func identifyPath(string) (fileID, error) {
	return fileID{}, errors.New("fetch: durable file identity is unsupported on this platform")
}

func identifyAt(*os.File, string) (fileID, error) {
	return fileID{}, errors.New("fetch: durable file identity is unsupported on this platform")
}
