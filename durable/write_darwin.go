//go:build darwin

package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// WriteFileNew publishes data at path durably without ever replacing anything.
// The temp beside path is written, chmodded, and fsynced as [WriteFile] does,
// then renamed into place with RENAME_EXCL, so a path that is occupied by the
// time of that rename keeps exactly what it holds. That refusal returns an
// error wrapping [os.ErrExist] and leaves no temp behind. On success it fsyncs
// the directory and returns the published file's FileInfo, read from the
// temp's descriptor before the rename, so its Name is the temp's: it is there
// for [os.SameFile], which a caller asks later to tell whether the file at
// path is still the one this call published.
//
// A directory fsync that fails after the rename returns that FileInfo together
// with its error: the file is published, its directory durability unconfirmed,
// and a caller that wants it gone still has what it needs to remove exactly it.
func WriteFileNew(path string, data []byte, perm os.FileMode) (os.FileInfo, error) {
	w, err := Create(path, perm)
	if err != nil {
		return nil, err
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("durable: write temp for %s: %w", path, err)
	}
	if err := w.file.Chmod(perm); err != nil {
		return nil, fmt.Errorf("durable: chmod temp for %s: %w", path, err)
	}
	if err := w.file.Sync(); err != nil {
		return nil, fmt.Errorf("durable: fsync temp for %s: %w", path, err)
	}
	info, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("durable: stat temp for %s: %w", path, err)
	}
	if err := w.file.Close(); err != nil {
		return nil, fmt.Errorf("durable: close temp for %s: %w", path, err)
	}
	if err := unix.RenamexNp(w.file.Name(), path, unix.RENAME_EXCL); err != nil {
		return nil, errors.Join(fmt.Errorf("durable: publish %s: %w", path, err), w.Close())
	}
	w.settled = true
	return info, SyncDir(w.dir)
}

// RenameNew renames from to to without ever replacing anything, then fsyncs
// both parent directories as [Rename] does, once when they are the same. The
// rename is RENAME_EXCL, so a to that is occupied — by a file, or by a
// directory even when it is empty, which a plain rename replaces — is refused
// with an error wrapping [os.ErrExist], and both paths keep exactly what they
// held. An error from either directory fsync comes after the rename landed.
func RenameNew(from, to string) error {
	switch err := unix.RenamexNp(from, to, unix.RENAME_EXCL); {
	case errors.Is(err, unix.EEXIST), errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.ENOTDIR):
		return fmt.Errorf("durable: rename %s to %s: %w: %w", from, to, os.ErrExist, err)
	case err != nil:
		return fmt.Errorf("durable: rename %s to %s: %w", from, to, err)
	}
	source, target := filepath.Dir(from), filepath.Dir(to)
	if err := SyncDir(target); err != nil {
		return err
	}
	if source == target {
		return nil
	}
	return SyncDir(source)
}
