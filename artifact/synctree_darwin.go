package artifact

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type treeSyncFile struct {
	file    *os.File
	regular bool
}

type treeSyncBatch struct {
	files map[int32]treeSyncFile
}

func syncTree(root string) error {
	return syncTreeWith(root, unix.Fsync, (*os.File).Sync)
}

func syncTreeWith(root string, fsync func(int) error, fullSync func(*os.File) error) (err error) {
	batch := treeSyncBatch{files: make(map[int32]treeSyncFile)}
	defer func() {
		err = errors.Join(err, batch.close())
	}()
	if err := walkSyncTree(root, func(file *os.File) error {
		stat, err := fsyncTreeFile(file, fsync)
		if err != nil {
			return errors.Join(err, file.Close())
		}
		return batch.retain(file, stat.Dev, stat.Mode&unix.S_IFMT == unix.S_IFREG)
	}); err != nil {
		return err
	}
	return batch.sync(fullSync)
}

func fsyncTreeFile(file *os.File, fsync func(int) error) (unix.Stat_t, error) {
	var stat unix.Stat_t
	raw, err := file.SyscallConn()
	if err != nil {
		return stat, err
	}
	var syncErr error
	err = raw.Control(func(fd uintptr) {
		if syncErr = unix.Fstat(int(fd), &stat); syncErr != nil {
			return
		}
		for {
			syncErr = fsync(int(fd))
			if !errors.Is(syncErr, unix.EINTR) {
				return
			}
		}
	})
	if err := errors.Join(err, syncErr); err != nil {
		return stat, &os.PathError{Op: "sync", Path: file.Name(), Err: err}
	}
	return stat, nil
}

func (b *treeSyncBatch) retain(file *os.File, device int32, regular bool) error {
	prior, exists := b.files[device]
	if exists && (prior.regular || !regular) {
		return file.Close()
	}
	b.files[device] = treeSyncFile{file: file, regular: regular}
	if exists {
		return prior.file.Close()
	}
	return nil
}

func (b *treeSyncBatch) sync(fullSync func(*os.File) error) error {
	for _, entry := range b.files {
		if err := fullSync(entry.file); err != nil {
			return err
		}
	}
	return nil
}

func (b *treeSyncBatch) close() error {
	var err error
	for _, entry := range b.files {
		err = errors.Join(err, entry.file.Close())
	}
	return err
}
