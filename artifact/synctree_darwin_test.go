package artifact

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSyncTreeBatchesDriveBarriers(t *testing.T) {
	for _, files := range []int{0, 256} {
		t.Run(fmt.Sprintf("files-%d", files), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "a", "b")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			for i := range files {
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), []byte("data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			kernelCalls, driveCalls := 0, 0
			err := syncTreeWith(root, func(fd int) error {
				kernelCalls++
				return unix.Fsync(fd)
			}, func(file *os.File) error {
				if kernelCalls != files+3 {
					t.Fatalf("drive barrier after %d kernel fsyncs, want %d", kernelCalls, files+3)
				}
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() != (files > 0) {
					t.Fatalf("representative mode = %v, files = %d", info.Mode(), files)
				}
				driveCalls++
				return file.Sync()
			})
			if err != nil {
				t.Fatal(err)
			}
			if kernelCalls != files+3 || driveCalls != 1 {
				t.Fatalf("kernel fsyncs = %d, drive barriers = %d; want %d, 1", kernelCalls, driveCalls, files+3)
			}
			t.Logf("entries=%d kernel_fsync_calls=%d terminal_file_sync_calls=%d filesystems=1", files+3, kernelCalls, driveCalls)
		})
	}
}

func TestTreeSyncBatchKeepsOneRepresentativePerFilesystem(t *testing.T) {
	root := t.TempDir()
	open := func(name string, dir bool) *os.File {
		t.Helper()
		path := filepath.Join(root, name)
		if dir {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		return file
	}
	firstDir, regular, extra, secondDir := open("first", true), open("regular", false), open("extra", false), open("second", true)
	batch := treeSyncBatch{files: make(map[int32]treeSyncFile)}
	for _, entry := range []struct {
		file    *os.File
		device  int32
		regular bool
	}{{firstDir, 1, false}, {regular, 1, true}, {extra, 1, true}, {secondDir, 2, false}} {
		if err := batch.retain(entry.file, entry.device, entry.regular); err != nil {
			t.Fatal(err)
		}
	}
	if len(batch.files) != 2 || batch.files[1].file != regular || batch.files[2].file != secondDir {
		t.Fatalf("representatives = %v, want regular and second filesystem directory", batch.files)
	}
	for _, file := range []*os.File{firstDir, extra} {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("discarded descriptor %s = %v, want closed", file.Name(), err)
		}
	}
	calls := make(map[*os.File]int)
	if err := batch.sync(func(file *os.File) error { calls[file]++; return nil }); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[regular] != 1 || calls[secondDir] != 1 {
		t.Fatalf("barriers = %v, want one per filesystem", calls)
	}
	if err := batch.close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{regular, secondDir} {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("retained descriptor %s = %v, want closed", file.Name(), err)
		}
	}
}

func TestSyncTreePropagatesDurabilityFailures(t *testing.T) {
	for _, stage := range []string{"kernel", "drive", "close"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			driveCalls := 0
			err := syncTreeWith(root, func(fd int) error {
				if stage == "kernel" {
					return unix.EIO
				}
				return unix.Fsync(fd)
			}, func(file *os.File) error {
				driveCalls++
				if stage == "drive" {
					return unix.EIO
				}
				return file.Close()
			})
			wantErr, wantCalls := error(unix.EIO), 1
			if stage == "kernel" {
				wantCalls = 0
			}
			if stage == "close" {
				wantErr = os.ErrClosed
			}
			if !errors.Is(err, wantErr) || driveCalls != wantCalls {
				t.Fatalf("error = %v, drive calls = %d; want %v, %d", err, driveCalls, wantErr, wantCalls)
			}
		})
	}
}

func TestFsyncTreeFileRetriesEINTRWithALiveDescriptor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	calls := 0
	stat, err := fsyncTreeFile(file, func(fd int) error {
		calls++
		if calls == 1 {
			return unix.EINTR
		}
		var info unix.Stat_t
		return unix.Fstat(fd, &info)
	})
	if err != nil || calls != 2 || stat.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Fatalf("stat mode = %v, error = %v, calls = %d; want regular, nil, 2", stat.Mode, err, calls)
	}
}
