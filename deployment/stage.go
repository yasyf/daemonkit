package deployment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/yasyf/daemonkit/daemon"
)

func (c *Controller) stageInstalledCandidate(
	ctx context.Context,
	config ApplyInstalledCandidateConfig,
	validated validatedApply,
	paths deploymentPaths,
) (returnErr error) {
	if fileExists(validated.candidatePath) {
		candidate, err := inspectInstalled(ctx, c.verifier, validated.candidatePath, config.CandidateVersion, config.Target.Identity)
		if err != nil {
			return err
		}
		if candidate.BundleDigest != config.CandidateBundleDigest.String() {
			return fmt.Errorf("%w: staged candidate digest differs from request", ErrInstallConflict)
		}
		return nil
	}

	source, err := inspectInstalled(ctx, c.verifier, config.CandidateSourcePath, config.CandidateVersion, config.Target.Identity)
	if err != nil {
		return err
	}
	if source.BundleDigest != config.CandidateBundleDigest.String() {
		return fmt.Errorf("%w: source bundle digest differs from request", ErrInstallConflict)
	}
	if err := c.inject("apply:source_attested"); err != nil {
		return err
	}

	stageID, err := c.operationID()
	if err != nil {
		return err
	}
	if !validOperationID(stageID) {
		return errors.New("deployment: operation ID source returned a noncanonical value")
	}
	stagePath := filepath.Join(paths.metadataDir, ".candidate-"+stageID+".app")
	if err := os.Mkdir(stagePath, 0o700); err != nil {
		return fmt.Errorf("deployment: create private candidate stage: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(stagePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, err)
		}
	}()

	if err := copyBundleTree(config.CandidateSourcePath, stagePath, func(path string) error {
		return c.inject("apply:copied:" + filepath.ToSlash(path))
	}); err != nil {
		return err
	}
	if err := syncBundleTree(stagePath); err != nil {
		return err
	}
	sourceAfter, err := inspectInstalled(ctx, c.verifier, config.CandidateSourcePath, config.CandidateVersion, config.Target.Identity)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sourceAfter, source) {
		return fmt.Errorf("%w: source bundle changed while staging", ErrInstallConflict)
	}
	staged, err := inspectInstalled(ctx, c.verifier, stagePath, config.CandidateVersion, config.Target.Identity)
	if err != nil {
		return err
	}
	if !sameGenerationBytes(staged, source) {
		return fmt.Errorf("%w: private candidate differs from source", ErrInstallConflict)
	}
	if err := os.Rename(stagePath, validated.candidatePath); err != nil {
		return fmt.Errorf("deployment: publish private candidate: %w", err)
	}
	if err := daemon.SyncDir(filepath.Dir(validated.candidatePath)); err != nil {
		return err
	}
	published, err := inspectInstalled(ctx, c.verifier, validated.candidatePath, config.CandidateVersion, config.Target.Identity)
	if err != nil {
		return err
	}
	if !sameGenerationBytes(published, source) {
		return fmt.Errorf("%w: published candidate differs from source", ErrInstallConflict)
	}
	return nil
}

func sameGenerationBytes(left, right storedGeneration) bool {
	left.Path, left.FileID = "", fileID{}
	right.Path, right.FileID = "", fileID{}
	return reflect.DeepEqual(left, right)
}

func copyBundleTree(sourcePath, targetPath string, copied func(string) error) error {
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		return fmt.Errorf("deployment: open candidate source: %w", err)
	}
	defer source.Close()
	target, err := os.OpenRoot(targetPath)
	if err != nil {
		return fmt.Errorf("deployment: open private candidate stage: %w", err)
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
			if err := copyBundleFile(source, target, path, info); err != nil {
				return err
			}
			return copied(path)
		case info.Mode()&os.ModeSymlink != 0:
			link, err := source.Readlink(path)
			if err != nil {
				return err
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(path), link))
			if filepath.IsAbs(link) || resolved == ".." || strings.HasPrefix(resolved, ".."+string(filepath.Separator)) {
				return fmt.Errorf("deployment: candidate symlink %q escapes its bundle", path)
			}
			return target.Symlink(link, path)
		default:
			return fmt.Errorf("deployment: candidate contains unsupported entry %q", path)
		}
	})
	if err != nil {
		return fmt.Errorf("deployment: copy candidate bundle: %w", err)
	}
	slices.Reverse(directories)
	for _, directory := range directories {
		if err := target.Chmod(directory.path, directory.mode.Perm()); err != nil {
			return fmt.Errorf("deployment: restore candidate directory mode: %w", err)
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
		return fmt.Errorf("deployment: candidate source changed at %q", path)
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
		return fmt.Errorf("deployment: candidate source changed while copying %q", path)
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

func removeTreeDurable(path string) error {
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return daemon.SyncDir(filepath.Dir(path))
}
