package bundle

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"howett.net/plist"
)

func TestShortVersionReadsBinaryPlist(t *testing.T) {
	var encoded bytes.Buffer
	if err := plist.NewEncoderForFormat(&encoded, plist.BinaryFormat).Encode(map[string]any{
		"CFBundleShortVersionString": "1.2.3",
	}); err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(t.TempDir(), "Binary.app")
	contents := filepath.Join(app, "Contents")
	if err := os.MkdirAll(contents, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contents, "Info.plist"), encoded.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	version, err := ShortVersion(app)
	if err != nil {
		t.Fatal(err)
	}
	if version != "1.2.3" {
		t.Fatalf("version = %q, want 1.2.3", version)
	}
}
