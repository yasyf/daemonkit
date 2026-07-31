package deploy

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestBundleTreeDigestSeparatesEveryHashedField asserts the digest moves for
// every fact it claims to cover and for nothing else. Fields are
// length-prefixed, so a name, a mode, a size, and a symlink target can never
// be re-partitioned into each other to forge a match.
func TestBundleTreeDigestSeparatesEveryHashedField(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, root string)
		same  bool
	}{
		{
			name:  "identical trees",
			build: func(t *testing.T, root string) { write(t, root, "Contents/MacOS/x", "body", 0o755) },
			same:  true,
		},
		{
			name:  "content",
			build: func(t *testing.T, root string) { write(t, root, "Contents/MacOS/x", "other", 0o755) },
		},
		{
			name:  "file mode",
			build: func(t *testing.T, root string) { write(t, root, "Contents/MacOS/x", "body", 0o644) },
		},
		{
			name:  "file name",
			build: func(t *testing.T, root string) { write(t, root, "Contents/MacOS/y", "body", 0o755) },
		},
		{
			name:  "directory depth",
			build: func(t *testing.T, root string) { write(t, root, "Contents/MacOS/n/x", "body", 0o755) },
		},
		{
			name: "an added sibling",
			build: func(t *testing.T, root string) {
				write(t, root, "Contents/MacOS/x", "body", 0o755)
				write(t, root, "Contents/MacOS/extra", "", 0o644)
			},
		},
		{
			name: "a symlink in place of the file",
			build: func(t *testing.T, root string) {
				write(t, root, "Contents/MacOS/keep", "body", 0o755)
				symlink(t, root, "keep", "Contents/MacOS/x")
			},
		},
	}
	base := t.TempDir()
	baseline := filepath.Join(base, "Baseline.app")
	write(t, baseline, "Contents/MacOS/x", "body", 0o755)
	want, err := bundleTreeDigest(baseline)
	if err != nil {
		t.Fatalf("bundleTreeDigest: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "Case.app")
			tt.build(t, root)
			got, err := bundleTreeDigest(root)
			if err != nil {
				t.Fatalf("bundleTreeDigest: %v", err)
			}
			if (got == want) != tt.same {
				t.Fatalf("digest == baseline is %v, want %v", got == want, tt.same)
			}
		})
	}
}

// TestBundleTreeDigestLengthPrefixesItsFields splits the same bytes across a
// name and a symlink target at two different offsets. Concatenated the two
// trees are identical; length-prefixed they cannot be.
func TestBundleTreeDigestLengthPrefixesItsFields(t *testing.T) {
	root := t.TempDir()
	left := filepath.Join(root, "Left.app")
	symlink(t, left, "ab", "Contents/MacOS/c")
	right := filepath.Join(root, "Right.app")
	symlink(t, right, "a", "Contents/MacOS/bc")
	leftDigest, err := bundleTreeDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := bundleTreeDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest == rightDigest {
		t.Fatal("two trees re-partitioning the same bytes share a digest")
	}
}

func TestBundleTreeDigestRefusesAnUnsupportedEntry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "Case.app")
	write(t, root, "Contents/MacOS/x", "body", 0o755)
	if err := syscall.Mkfifo(filepath.Join(root, "Contents", "MacOS", "pipe"), 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	_, err := bundleTreeDigest(root)
	if err == nil || !strings.Contains(err.Error(), "unsupported entry") {
		t.Fatalf("bundleTreeDigest err = %v, want an unsupported-entry refusal", err)
	}
}

func TestValidateCanonicalAppPathRefusesSymlinksAndNonDirectories(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genuine := filepath.Join(root, "Real.app")
	write(t, genuine, "Contents/Info.plist", "", 0o644)
	link := filepath.Join(root, "Link.app")
	if err := os.Symlink(genuine, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "File.app")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		path string
		want error
	}{
		{"genuine bundle", genuine, nil},
		{"symlinked bundle", link, ErrConflict},
		{"regular file", file, ErrConflict},
		{"relative path", "Real.app", ErrConfig},
		{"unclean path", root + "/./Real.app", ErrConfig},
		{"bare suffix", filepath.Join(root, ".app"), ErrConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCanonicalAppPath(tt.path)
			switch {
			case tt.want == nil && err != nil:
				t.Fatalf("validateCanonicalAppPath = %v, want nil", err)
			case tt.want != nil && !isSentinel(err, tt.want):
				t.Fatalf("validateCanonicalAppPath = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestRequirePrivateDirectoryRefusesAnythingButAPrivateOwnedDirectory pins the
// only authentication the metadata directory's records have: nothing signs the
// swap record, so who owns the directory and who may write it is the whole
// proof that the instruction inside it is deploy's own.
func TestRequirePrivateDirectoryRefusesAnythingButAPrivateOwnedDirectory(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		mode os.FileMode
		want error
	}{
		{"private", 0o700, nil},
		{"owner readable only", 0o500, nil},
		{"group writable", 0o770, ErrConflict},
		{"world writable", 0o707, ErrConflict},
		{"group readable", 0o740, ErrConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(root, tt.name)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(dir, tt.mode); err != nil {
				t.Fatal(err)
			}
			err := requirePrivateDirectory(dir)
			switch {
			case tt.want == nil && err != nil:
				t.Fatalf("requirePrivateDirectory = %v, want nil", err)
			case tt.want != nil && !isSentinel(err, tt.want):
				t.Fatalf("requirePrivateDirectory = %v, want %v", err, tt.want)
			}
		})
	}
}

func isSentinel(err, want error) bool {
	return err != nil && strings.Contains(err.Error(), want.Error())
}

func write(t *testing.T, root, rel, body string, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, root, target, rel string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}
