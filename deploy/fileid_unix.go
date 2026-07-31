//go:build darwin || linux

package deploy

import (
	"fmt"
	"os"
	"syscall"
)

// FileID is one bundle directory's durable device and inode identity, so a
// generation cannot be impersonated by a same-named tree at another inode.
type FileID struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

func identifyPath(path string) (FileID, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return FileID{}, err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return FileID{Device: fmt.Sprint(stat.Dev), Inode: fmt.Sprint(stat.Ino)}, nil
}

func directoryOwner(info os.FileInfo) (int, error) {
	return int(info.Sys().(*syscall.Stat_t).Uid), nil
}
