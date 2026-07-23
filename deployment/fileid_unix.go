//go:build darwin || linux

package deployment

import (
	"fmt"
	"os"
	"syscall"
)

type fileID struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

func identifyPath(path string) (fileID, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return fileID{}, err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return fileID{Device: fmt.Sprint(stat.Dev), Inode: fmt.Sprint(stat.Ino)}, nil
}
