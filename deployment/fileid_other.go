//go:build !darwin && !linux

package deployment

import (
	"errors"
)

type fileID struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

func identifyPath(string) (fileID, error) {
	return fileID{}, errors.New("deployment: durable file identity is unsupported on this platform")
}
