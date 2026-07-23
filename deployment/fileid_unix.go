//go:build darwin || linux

package deployment

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
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

func identifyAt(dir *os.File, name string) (fileID, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fileID{}, err
	}
	return fileID{Device: fmt.Sprint(stat.Dev), Inode: fmt.Sprint(stat.Ino)}, nil
}
