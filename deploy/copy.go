package deploy

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// copyBundleTree copies sourcePath's whole tree into targetPath through two
// os.Root scopes, so no entry — symlink included — can name anything outside
// either bundle. Directory modes are restored last, after their contents are
// written, so a read-only directory never blocks its own population.
func copyBundleTree(sourcePath, targetPath string) error {
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		return fmt.Errorf("deploy: open candidate source: %w", err)
	}
	defer source.Close()
	target, err := os.OpenRoot(targetPath)
	if err != nil {
		return fmt.Errorf("deploy: open candidate stage: %w", err)
	}
	defer target.Close()

	type directoryMode struct {
		path string
		mode fs.FileMode
	}
	var directories []directoryMode
	err = fs.WalkDir(source.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if path == "." {
			directories = append(directories, directoryMode{path: path, mode: info.Mode()})
			return nil
		}
		switch {
		case info.IsDir():
			if err := target.Mkdir(path, 0o700); err != nil {
				return err
			}
			directories = append(directories, directoryMode{path: path, mode: info.Mode()})
			return nil
		case info.Mode().IsRegular():
			return copyBundleFile(source, target, path, info)
		case info.Mode()&os.ModeSymlink != 0:
			link, err := source.Readlink(path)
			if err != nil {
				return err
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(path), link))
			if filepath.IsAbs(link) || resolved == ".." || strings.HasPrefix(resolved, ".."+string(filepath.Separator)) {
				return fmt.Errorf("deploy: candidate symlink %q escapes its bundle", path)
			}
			return target.Symlink(link, path)
		default:
			return fmt.Errorf("deploy: candidate contains unsupported entry %q", path)
		}
	})
	if err != nil {
		return fmt.Errorf("deploy: copy candidate bundle: %w", err)
	}
	slices.Reverse(directories)
	for _, directory := range directories {
		if err := target.Chmod(directory.path, directory.mode.Perm()); err != nil {
			return fmt.Errorf("deploy: restore candidate directory mode: %w", err)
		}
	}
	return nil
}

func copyBundleFile(source, target *os.Root, path string, walked fs.FileInfo) error {
	input, err := source.Open(path)
	if err != nil {
		return err
	}
	before, err := input.Stat()
	if err != nil {
		_ = input.Close()
		return err
	}
	if !before.Mode().IsRegular() || !os.SameFile(walked, before) {
		_ = input.Close()
		return fmt.Errorf("deploy: candidate source changed at %q", path)
	}
	output, err := target.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = input.Close()
		return err
	}
	_, copyErr := io.Copy(output, input)
	chmodErr := output.Chmod(before.Mode().Perm())
	syncErr := output.Sync()
	after, statErr := input.Stat()
	closeErr := errors.Join(output.Close(), input.Close())
	if err := errors.Join(copyErr, chmodErr, syncErr, statErr, closeErr); err != nil {
		return err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || before.Size() != after.Size() ||
		before.ModTime() != after.ModTime() || before.Mode() != after.Mode() {
		return fmt.Errorf("deploy: candidate source changed while copying %q", path)
	}
	return nil
}

func syncBundleTree(root string) error {
	handle, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer handle.Close()
	return fs.WalkDir(handle.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		file, err := handle.Open(path)
		if err != nil {
			return err
		}
		return errors.Join(file.Sync(), file.Close())
	})
}
