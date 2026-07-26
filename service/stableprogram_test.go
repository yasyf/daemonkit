package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/version"
)

func stableTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func fakeExecutable(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(stableTestRoot(t), name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func stableBytes(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func stableIdentity(t *testing.T, path string) (uint64, int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %q has no syscall.Stat_t", path)
	}
	return stat.Ino, info.ModTime().UnixNano()
}

func devBuild(nanos int64) string { return version.DevString(time.Unix(0, nanos)) }

func TestStableProgramMaterializesCallingExecutable(t *testing.T) {
	root := stableTestRoot(t)
	self := fakeExecutable(t, "cc-review", "release one")

	got, err := stableProgram(root, "cc-review", "v1.0.0", self)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "bin", "cc-review")
	if got != want {
		t.Fatalf("stableProgram() = %q, want %q", got, want)
	}
	if body := stableBytes(t, got); body != "release one" {
		t.Fatalf("stable program bytes = %q, want %q", body, "release one")
	}
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("stable program mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o755))
	}
	if err := validateProgramTree(Agent{Program: got}); err != nil {
		t.Fatalf("validateProgramTree(%q) = %v, want nil", got, err)
	}
	meta, err := readStableMeta(stableMetaPath(got))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("release one"))
	wantMeta := stableProgramMeta{
		Schema: stableProgramSchema,
		Policy: stableProgramPolicyBuild,
		Build:  "v1.0.0",
		Digest: hex.EncodeToString(sum[:]),
		Size:   info.Size(),
		MTime:  info.ModTime().UnixNano(),
	}
	if meta != wantMeta {
		t.Fatalf("meta = %+v, want %+v", meta, wantMeta)
	}
	for _, directory := range []string{filepath.Join(root, "bin"), filepath.Join(root, "locks")} {
		info, err := os.Stat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("directory %q mode = %v, want %v", directory, info.Mode().Perm(), os.FileMode(0o700))
		}
	}
}

func TestStableProgramSameBuildTakesFastPath(t *testing.T) {
	root := stableTestRoot(t)
	self := fakeExecutable(t, "cc-review", "release one")
	stable, err := stableProgram(root, "cc-review", "v1.0.0", self)
	if err != nil {
		t.Fatal(err)
	}
	inode, mtime := stableIdentity(t, stable)
	lock := filepath.Join(root, "locks", "stable-cc-review.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(self, []byte("rebuilt one"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := stableProgram(root, "cc-review", "1.0.0", self)
	if err != nil {
		t.Fatal(err)
	}
	if got != stable {
		t.Fatalf("stableProgram() = %q, want %q", got, stable)
	}
	if body := stableBytes(t, got); body != "release one" {
		t.Fatalf("stable program bytes = %q, want %q", body, "release one")
	}
	if gotInode, gotMtime := stableIdentity(t, got); gotInode != inode || gotMtime != mtime {
		t.Fatalf("stable program identity = (%d, %d), want (%d, %d)", gotInode, gotMtime, inode, mtime)
	}
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("stat %q = %v, want not exist — the fast path must not lock", lock, err)
	}
}

func TestStableProgramReplacesSymlinkedStablePath(t *testing.T) {
	root := stableTestRoot(t)
	stable, err := stableProgram(root, "cc-review", "v1.0.0", fakeExecutable(t, "cc-review", "installed"))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := readStableMeta(stableMetaPath(stable))
	if err != nil {
		t.Fatal(err)
	}
	target := fakeExecutable(t, "target", "installed")
	targetTime := time.Unix(0, meta.MTime)
	if err := os.Chtimes(target, targetTime, targetTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(stable); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, stable); err != nil {
		t.Fatal(err)
	}

	got, err := stableProgram(root, "cc-review", "v1.0.0", fakeExecutable(t, "cc-review", "replacement"))
	if err != nil {
		t.Fatal(err)
	}
	if got != stable {
		t.Fatalf("stableProgram() = %q, want %q", got, stable)
	}
	if got == target {
		t.Fatalf("stableProgram() returned symlink target %q", target)
	}
	info, err := os.Lstat(got)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("stable program mode = %v, want regular file", info.Mode())
	}
	if body := stableBytes(t, got); body != "replacement" {
		t.Fatalf("stable program bytes = %q, want %q", body, "replacement")
	}
}

func TestStableProgramRepairsNonExecutableMode(t *testing.T) {
	root := stableTestRoot(t)
	self := fakeExecutable(t, "cc-review", "release one")
	stable, err := stableProgram(root, "cc-review", "v1.0.0", self)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stable, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := stableProgram(root, "cc-review", "v1.0.0", self); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(stable)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("stable program mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o755))
	}
}

func TestStableProgramRefreshesTouchedMTimeForFastPath(t *testing.T) {
	root := stableTestRoot(t)
	self := fakeExecutable(t, "cc-review", "release one")
	stable, err := stableProgram(root, "cc-review", "v1.0.0", self)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := readStableMeta(stableMetaPath(stable))
	if err != nil {
		t.Fatal(err)
	}
	touched := time.Unix(0, meta.MTime).Add(time.Second)
	if err := os.Chtimes(stable, touched, touched); err != nil {
		t.Fatal(err)
	}

	if _, err := stableProgram(root, "cc-review", "v1.0.0", self); err != nil {
		t.Fatal(err)
	}
	refreshed, err := readStableMeta(stableMetaPath(stable))
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.MTime != touched.UnixNano() {
		t.Fatalf("refreshed meta mtime = %d, want %d", refreshed.MTime, touched.UnixNano())
	}
	lock := filepath.Join(root, "locks", "stable-cc-review.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}

	if _, err := stableProgram(root, "cc-review", "v1.0.0", self); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("stat %q = %v, want not exist — the refreshed fast path must not lock", lock, err)
	}
}

func TestStableProgramNewerBuildReplacesWithoutDisturbingOpenCopy(t *testing.T) {
	root := stableTestRoot(t)
	stable, err := stableProgram(root, "cc-review", "v1.0.0", fakeExecutable(t, "cc-review", "release one"))
	if err != nil {
		t.Fatal(err)
	}
	inode, _ := stableIdentity(t, stable)
	open, err := os.Open(stable)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()

	got, err := stableProgram(root, "cc-review", "v1.1.0", fakeExecutable(t, "cc-review", "release two"))
	if err != nil {
		t.Fatal(err)
	}
	if got != stable {
		t.Fatalf("stableProgram() = %q, want %q", got, stable)
	}
	if body := stableBytes(t, got); body != "release two" {
		t.Fatalf("stable program bytes = %q, want %q", body, "release two")
	}
	held := make([]byte, len("release one"))
	if _, err := open.Read(held); err != nil {
		t.Fatal(err)
	}
	if string(held) != "release one" {
		t.Fatalf("open copy bytes = %q, want %q", held, "release one")
	}
	if gotInode, _ := stableIdentity(t, got); gotInode == inode {
		t.Fatalf("stable program inode = %d, want a replacement inode", gotInode)
	}
	meta, err := readStableMeta(stableMetaPath(got))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Build != "v1.1.0" {
		t.Fatalf("meta build = %q, want %q", meta.Build, "v1.1.0")
	}
}

func TestStableProgramBuildOrdering(t *testing.T) {
	tests := []struct {
		name    string
		have    string
		arrives string
		want    string
	}{
		{name: "newer release replaces", have: "v1.0.0", arrives: "v1.0.1", want: "arriving"},
		{name: "older release keeps", have: "v1.2.0", arrives: "v1.1.9", want: "installed"},
		{name: "equal release keeps", have: "v1.2.0", arrives: "1.2.0", want: "installed"},
		{name: "dev outranks release", have: "v9.9.9", arrives: devBuild(1), want: "arriving"},
		{name: "release yields to dev", have: devBuild(1), arrives: "v9.9.9", want: "installed"},
		{name: "newer dev replaces", have: devBuild(1000), arrives: devBuild(2000), want: "arriving"},
		{name: "older dev keeps", have: devBuild(2000), arrives: devBuild(1000), want: "installed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			stable, err := stableProgram(root, "cc-review", test.have, fakeExecutable(t, "cc-review", "installed"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stableProgram(root, "cc-review", test.arrives, fakeExecutable(t, "cc-review", "arriving")); err != nil {
				t.Fatal(err)
			}
			if body := stableBytes(t, stable); body != test.want {
				t.Fatalf("stable program bytes = %q, want %q", body, test.want)
			}
		})
	}
}

func TestStableProgramRepairsDriftedBytesAtAnyBuild(t *testing.T) {
	tests := []struct {
		name    string
		have    string
		arrives string
	}{
		{name: "equal build", have: "v1.2.0", arrives: "v1.2.0"},
		{name: "older build", have: "v1.2.0", arrives: "v1.0.0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			stable, err := stableProgram(root, "cc-review", test.have, fakeExecutable(t, "cc-review", "installed"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(stable, []byte("truncated"), 0o755); err != nil {
				t.Fatal(err)
			}

			if _, err := stableProgram(root, "cc-review", test.arrives, fakeExecutable(t, "cc-review", "repaired")); err != nil {
				t.Fatal(err)
			}
			if body := stableBytes(t, stable); body != "repaired" {
				t.Fatalf("stable program bytes = %q, want %q", body, "repaired")
			}
			meta, err := readStableMeta(stableMetaPath(stable))
			if err != nil {
				t.Fatal(err)
			}
			if meta.Build != test.arrives {
				t.Fatalf("meta build = %q, want %q", meta.Build, test.arrives)
			}
		})
	}
}

func TestStableProgramReplacesOnUnusableMeta(t *testing.T) {
	tests := []struct {
		name string
		meta string
	}{
		{name: "missing"},
		{name: "truncated json", meta: `{"schema":2,"policy":"build","build":"v1.2.0"`},
		{name: "trailing json", meta: `{"schema":2,"policy":"build","build":"v1.2.0","digest":"ab","size":9,"mtime":1} {}`},
		{name: "unknown field", meta: `{"schema":2,"policy":"build","build":"v1.2.0","digest":"ab","size":9,"mtime":1,"extra":true}`},
		{name: "legacy schema", meta: `{"schema":1,"build":"v1.2.0","digest":"ab","size":9,"mtime":1}`},
		{name: "missing policy", meta: `{"schema":2,"build":"v1.2.0","digest":"ab","size":9,"mtime":1}`},
		{name: "digest policy with build", meta: `{"schema":2,"policy":"digest","build":"v1.2.0","digest":"ab","size":9,"mtime":1}`},
		{name: "empty digest", meta: `{"schema":2,"policy":"build","build":"v1.2.0","digest":"","size":9,"mtime":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			stable, err := stableProgram(root, "cc-review", "v1.2.0", fakeExecutable(t, "cc-review", "installed"))
			if err != nil {
				t.Fatal(err)
			}
			metaPath := stableMetaPath(stable)
			if test.meta == "" {
				if err := os.Remove(metaPath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(metaPath, []byte(test.meta), 0o600); err != nil {
				t.Fatal(err)
			}

			if _, err := stableProgram(root, "cc-review", "v1.2.0", fakeExecutable(t, "cc-review", "rewritten")); err != nil {
				t.Fatal(err)
			}
			if body := stableBytes(t, stable); body != "rewritten" {
				t.Fatalf("stable program bytes = %q, want %q", body, "rewritten")
			}
			if _, err := readStableMeta(metaPath); err != nil {
				t.Fatalf("readStableMeta() = %v, want a rewritten sidecar", err)
			}
		})
	}
}

func TestStableProgramRejectsUncanonicalName(t *testing.T) {
	tests := []struct {
		name    string
		program string
		err     string
	}{
		{name: "empty", program: "", err: "is empty"},
		{name: "path separator", program: "a/b", err: "not canonical"},
		{name: "parent", program: "..", err: "not canonical"},
		{name: "hidden", program: ".cc-review", err: "not canonical"},
		{name: "uppercase", program: "CC-Review", err: "not canonical"},
		{name: "leading dash", program: "-cc-review", err: "not canonical"},
		{name: "space", program: "cc review", err: "not canonical"},
		{name: "dashed", program: "cc-review"},
		{name: "underscored dotted", program: "cc_patch.v2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			got, err := stableProgram(root, test.program, "v1.0.0", fakeExecutable(t, "self", "installed"))
			if test.err != "" {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("stableProgram(%q) error = %v, want %q", test.program, err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if want := filepath.Join(root, "bin", test.program); got != want {
				t.Fatalf("stableProgram(%q) = %q, want %q", test.program, got, want)
			}
		})
	}
}

func TestStableProgramRejectsEmptyBuildWithoutWriting(t *testing.T) {
	root := stableTestRoot(t)

	if _, err := stableProgram(root, "cc-review", "", fakeExecutable(t, "self", "installed")); err == nil ||
		!strings.Contains(err.Error(), "build is empty") {
		t.Fatalf("stableProgram() error = %v, want empty build rejection", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("stableProgram() wrote entries for empty build: %v", entries)
	}
}

func TestStableProgramShortCircuitsWhenSelfIsStable(t *testing.T) {
	root := stableTestRoot(t)
	stable, err := stableProgram(root, "cc-review", "v1.0.0", fakeExecutable(t, "cc-review", "release one"))
	if err != nil {
		t.Fatal(err)
	}
	inode, mtime := stableIdentity(t, stable)

	got, err := stableProgram(root, "cc-review", "v2.0.0", stable)
	if err != nil {
		t.Fatal(err)
	}
	if got != stable {
		t.Fatalf("stableProgram() = %q, want %q", got, stable)
	}
	if body := stableBytes(t, got); body != "release one" {
		t.Fatalf("stable program bytes = %q, want %q", body, "release one")
	}
	if gotInode, gotMtime := stableIdentity(t, got); gotInode != inode || gotMtime != mtime {
		t.Fatalf("stable program identity = (%d, %d), want (%d, %d)", gotInode, gotMtime, inode, mtime)
	}
	meta, err := readStableMeta(stableMetaPath(got))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Build != "v1.0.0" {
		t.Fatalf("meta build = %q, want %q", meta.Build, "v1.0.0")
	}
}

func TestStableProgramSerializesConcurrentBuilds(t *testing.T) {
	root := stableTestRoot(t)
	selves := map[string]string{"v1.0.0": fakeExecutable(t, "cc-review", "release one"), "v2.0.0": fakeExecutable(t, "cc-review", "release two")}
	paths := make(map[string]string, len(selves))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for build, self := range selves {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := stableProgram(root, "cc-review", build, self)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Error(err)
				return
			}
			paths[build] = got
		}()
	}
	wg.Wait()

	stable := filepath.Join(root, "bin", "cc-review")
	for build, got := range paths {
		if got != stable {
			t.Fatalf("stableProgram(%q) = %q, want %q", build, got, stable)
		}
	}
	if body := stableBytes(t, stable); body != "release two" {
		t.Fatalf("stable program bytes = %q, want %q", body, "release two")
	}
	meta, err := readStableMeta(stableMetaPath(stable))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Build != "v2.0.0" {
		t.Fatalf("meta build = %q, want %q", meta.Build, "v2.0.0")
	}
}

func TestStableProgramFromMaterializesForeignSourceUnderLabelName(t *testing.T) {
	root := stableTestRoot(t)
	source := fakeExecutable(t, "reposync", "helper payload")
	label := "com.github.yasyf.synckit.helper.reposync"

	got, err := stableProgramFrom(root, label, source)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "bin", label)
	if got != want {
		t.Fatalf("stableProgramFrom() = %q, want %q", got, want)
	}
	if body := stableBytes(t, got); body != "helper payload" {
		t.Fatalf("stable program bytes = %q, want %q", body, "helper payload")
	}
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("stable program mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o755))
	}
	if err := validateProgramTree(Agent{Program: got}); err != nil {
		t.Fatalf("validateProgramTree(%q) = %v, want nil", got, err)
	}
	meta, err := readStableMeta(stableMetaPath(got))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("helper payload"))
	wantMeta := stableProgramMeta{
		Schema: stableProgramSchema,
		Policy: stableProgramPolicyDigest,
		Digest: hex.EncodeToString(sum[:]),
		Size:   info.Size(),
		MTime:  info.ModTime().UnixNano(),
	}
	if meta != wantMeta {
		t.Fatalf("meta = %+v, want %+v", meta, wantMeta)
	}
}

func TestStableProgramFromKeysReplacementOnDigest(t *testing.T) {
	tests := []struct {
		name     string
		second   string
		tamper   func(t *testing.T, stable string)
		want     string
		replaces bool
		locks    bool
	}{
		{name: "identical bytes from a moved source keep", second: "helper one", want: "helper one"},
		{name: "changed bytes replace", second: "helper two", want: "helper two", replaces: true, locks: true},
		{
			name:   "drifted staged bytes repaired",
			second: "helper one",
			tamper: func(t *testing.T, stable string) {
				t.Helper()
				if err := os.WriteFile(stable, []byte("tampered"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			want:     "helper one",
			replaces: true,
			locks:    true,
		},
		{
			name:   "non-executable mode repaired",
			second: "helper one",
			tamper: func(t *testing.T, stable string) {
				t.Helper()
				if err := os.Chmod(stable, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want:     "helper one",
			replaces: true,
			locks:    true,
		},
		{
			name:   "missing sidecar rewritten without replacing",
			second: "helper one",
			tamper: func(t *testing.T, stable string) {
				t.Helper()
				if err := os.Remove(stableMetaPath(stable)); err != nil {
					t.Fatal(err)
				}
			},
			want:  "helper one",
			locks: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			stable, err := stableProgramFrom(root, "helper", fakeExecutable(t, "helper", "helper one"))
			if err != nil {
				t.Fatal(err)
			}
			inode, mtime := stableIdentity(t, stable)
			lock := filepath.Join(root, "locks", "stable-helper.lock")
			if err := os.Remove(lock); err != nil {
				t.Fatal(err)
			}
			if test.tamper != nil {
				test.tamper(t, stable)
			}

			got, err := stableProgramFrom(root, "helper", fakeExecutable(t, "helper", test.second))
			if err != nil {
				t.Fatal(err)
			}
			if got != stable {
				t.Fatalf("stableProgramFrom() = %q, want %q", got, stable)
			}
			if body := stableBytes(t, got); body != test.want {
				t.Fatalf("stable program bytes = %q, want %q", body, test.want)
			}
			info, err := os.Lstat(got)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o755 {
				t.Fatalf("stable program mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o755))
			}
			gotInode, gotMTime := stableIdentity(t, got)
			if replaced := gotInode != inode || gotMTime != mtime; replaced != test.replaces {
				t.Fatalf("stable program replaced = %t, want %t", replaced, test.replaces)
			}
			meta, err := readStableMeta(stableMetaPath(got))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256([]byte(test.want))
			if meta.Digest != hex.EncodeToString(sum[:]) {
				t.Fatalf("meta digest = %q, want the digest of %q", meta.Digest, test.want)
			}
			if _, err := os.Stat(lock); (err == nil) != test.locks {
				t.Fatalf("stat %q = %v, want lock recreated %t", lock, err, test.locks)
			}
		})
	}
}

func TestStableProgramFromReadsThroughSymlinkedSource(t *testing.T) {
	root := stableTestRoot(t)
	target := fakeExecutable(t, "helper-0.27.4", "helper payload")
	link := filepath.Join(stableTestRoot(t), "helper")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	got, err := stableProgramFrom(root, "helper", link)
	if err != nil {
		t.Fatal(err)
	}
	if body := stableBytes(t, got); body != "helper payload" {
		t.Fatalf("stable program bytes = %q, want %q", body, "helper payload")
	}
	if err := validateProgramTree(Agent{Program: got}); err != nil {
		t.Fatalf("validateProgramTree(%q) = %v, want nil", got, err)
	}
}

func TestStableProgramRefusesCrossPolicyName(t *testing.T) {
	tests := []struct {
		name        string
		digestFirst bool
		sameBytes   bool
		wantErr     string
	}{
		{name: "digest staging over build-keyed name", wantErr: "digest-keyed staging refused"},
		{name: "digest staging over build-keyed name with identical bytes", sameBytes: true, wantErr: "digest-keyed staging refused"},
		{name: "build staging over digest-keyed name", digestFirst: true, wantErr: "build-keyed staging refused"},
		{name: "build staging over digest-keyed name with identical bytes", digestFirst: true, sameBytes: true, wantErr: "build-keyed staging refused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			first := fakeExecutable(t, "helper", "first bytes")
			var stable string
			var err error
			if test.digestFirst {
				stable, err = stableProgramFrom(root, "helper", first)
			} else {
				stable, err = stableProgram(root, "helper", "v1.0.0", first)
			}
			if err != nil {
				t.Fatal(err)
			}
			wantMeta, err := readStableMeta(stableMetaPath(stable))
			if err != nil {
				t.Fatal(err)
			}
			secondBytes := "second bytes"
			if test.sameBytes {
				secondBytes = "first bytes"
			}
			second := fakeExecutable(t, "helper", secondBytes)

			if test.digestFirst {
				_, err = stableProgram(root, "helper", "v2.0.0", second)
			} else {
				_, err = stableProgramFrom(root, "helper", second)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("cross-policy staging error = %v, want %q", err, test.wantErr)
			}
			if body := stableBytes(t, stable); body != "first bytes" {
				t.Fatalf("stable program bytes = %q, want %q", body, "first bytes")
			}
			meta, err := readStableMeta(stableMetaPath(stable))
			if err != nil {
				t.Fatal(err)
			}
			if meta != wantMeta {
				t.Fatalf("meta after refusal = %+v, want %+v", meta, wantMeta)
			}
		})
	}
}

func TestStableProgramFromDistinguishesAbsentFromUnreadableSidecar(t *testing.T) {
	tests := []struct {
		name    string
		sidecar string
		wantErr string
	}{
		{name: "legacy build-keyed sidecar refused", sidecar: `{"schema":1,"build":"v1.0.0","digest":"ab","size":9,"mtime":1}`, wantErr: "digest-keyed staging refused"},
		{name: "garbled sidecar refused", sidecar: `{"schema":2,"policy":`, wantErr: "digest-keyed staging refused"},
		{name: "absent sidecar proceeds"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := stableTestRoot(t)
			stable, err := stableProgram(root, "helper", "v1.0.0", fakeExecutable(t, "helper", "first bytes"))
			if err != nil {
				t.Fatal(err)
			}
			metaPath := stableMetaPath(stable)
			if test.sidecar == "" {
				if err := os.Remove(metaPath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(metaPath, []byte(test.sidecar), 0o600); err != nil {
				t.Fatal(err)
			}
			source := fakeExecutable(t, "helper", "second bytes")

			got, err := stableProgramFrom(root, "helper", source)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("stableProgramFrom() error = %v, want %q", err, test.wantErr)
				}
				if body := stableBytes(t, stable); body != "first bytes" {
					t.Fatalf("stable program bytes = %q, want %q", body, "first bytes")
				}
				sidecar, err := os.ReadFile(metaPath)
				if err != nil {
					t.Fatal(err)
				}
				if string(sidecar) != test.sidecar {
					t.Fatalf("sidecar after refusal = %q, want %q", sidecar, test.sidecar)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != stable {
				t.Fatalf("stableProgramFrom() = %q, want %q", got, stable)
			}
			if body := stableBytes(t, got); body != "second bytes" {
				t.Fatalf("stable program bytes = %q, want %q", body, "second bytes")
			}
			meta, err := readStableMeta(metaPath)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256([]byte("second bytes"))
			if meta.Policy != stableProgramPolicyDigest || meta.Digest != hex.EncodeToString(sum[:]) {
				t.Fatalf("meta after absent-sidecar stage = %+v", meta)
			}
		})
	}
}

func TestStableProgramFromSerializesConcurrentSources(t *testing.T) {
	root := stableTestRoot(t)
	sources := []string{fakeExecutable(t, "helper", "helper one"), fakeExecutable(t, "helper", "helper two")}
	paths := make([]string, len(sources))
	var wg sync.WaitGroup
	for index, source := range sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := stableProgramFrom(root, "helper", source)
			if err != nil {
				t.Error(err)
				return
			}
			paths[index] = got
		}()
	}
	wg.Wait()

	stable := filepath.Join(root, "bin", "helper")
	for _, got := range paths {
		if got != stable {
			t.Fatalf("stableProgramFrom() = %q, want %q", got, stable)
		}
	}
	body := stableBytes(t, stable)
	if body != "helper one" && body != "helper two" {
		t.Fatalf("stable program bytes = %q, want a staged source", body)
	}
	meta, err := readStableMeta(stableMetaPath(stable))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	want := stableProgramMeta{
		Schema: stableProgramSchema,
		Policy: stableProgramPolicyDigest,
		Digest: hex.EncodeToString(sum[:]),
		Size:   int64(len(body)),
		MTime:  meta.MTime,
	}
	if meta != want {
		t.Fatalf("meta = %+v, want %+v", meta, want)
	}
	info, err := os.Lstat(stable)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != meta.Size || info.ModTime().UnixNano() != meta.MTime {
		t.Fatalf("staged fingerprint = (%d, %d), want meta fingerprint (%d, %d)",
			info.Size(), info.ModTime().UnixNano(), meta.Size, meta.MTime)
	}
}

func TestStableProgramFromMissingSourceWritesNothing(t *testing.T) {
	root := stableTestRoot(t)

	_, err := stableProgramFrom(root, "helper", filepath.Join(root, "absent"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stableProgramFrom() error = %v, want os.ErrNotExist", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("stableProgramFrom() wrote entries for a missing source: %v", entries)
	}
}

func TestRemoveStableProgramIsIdempotent(t *testing.T) {
	root := stableTestRoot(t)
	stable, err := stableProgram(root, "cc-review", "v1.0.0", fakeExecutable(t, "cc-review", "release one"))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := removeStableProgram(root, "cc-review"); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{stable, stableMetaPath(stable)} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("stat %q = %v, want not exist", path, err)
			}
		}
	}
}
