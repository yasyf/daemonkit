package bundle

import (
	"os"
	"path/filepath"
	"testing"
)

const releasePlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleIdentifier</key><string>com.yasyf.fusekit-holder</string>
  <key>CFBundleName</key><string>fusekit-holder</string>
  <key>CFBundleShortVersionString</key><string>0.38.0</string>
  <key>CFBundleVersion</key><string>123</string>
  <key>LSBackgroundOnly</key><true/>
</dict></plist>
`

func writePlist(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "Info.plist")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStringValue(t *testing.T) {
	cases := []struct {
		name    string
		content string
		key     string
		want    string
		wantErr bool
	}{
		{name: "release format", content: releasePlist, key: "CFBundleShortVersionString", want: "0.38.0"},
		{name: "any key, not just the version", content: releasePlist, key: "CFBundleIdentifier", want: "com.yasyf.fusekit-holder"},
		{name: "key after other strings", content: `<plist><dict><key>A</key><string>x</string><key>CFBundleShortVersionString</key><string>1.2.3</string></dict></plist>`, key: "CFBundleShortVersionString", want: "1.2.3"},
		{name: "missing key", content: `<plist><dict><key>A</key><string>x</string></dict></plist>`, key: "CFBundleShortVersionString", wantErr: true},
		{name: "non-string value after key", content: `<plist><dict><key>CFBundleShortVersionString</key><true/><key>B</key><string>x</string></dict></plist>`, key: "CFBundleShortVersionString", wantErr: true},
		{name: "empty value", content: `<plist><dict><key>A</key><string></string></dict></plist>`, key: "A", wantErr: true},
		{name: "binary junk", content: "bplist00\x00\x01\x02", key: "A", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := StringValue(writePlist(t, tc.content), tc.key)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("value = %q, want %q", got, tc.want)
			}
		})
	}
	if _, err := StringValue(filepath.Join(t.TempDir(), "missing.plist"), "A"); err == nil {
		t.Fatal("missing plist read without error")
	}
}

func TestShortVersion(t *testing.T) {
	app := t.TempDir()
	contents := filepath.Join(app, "Contents")
	if err := os.MkdirAll(contents, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contents, "Info.plist"), []byte(releasePlist), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ShortVersion(app)
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.38.0" {
		t.Fatalf("ShortVersion = %q, want 0.38.0", got)
	}
	if _, err := ShortVersion(t.TempDir()); err == nil {
		t.Fatal("ShortVersion on a bundle without Info.plist read without error")
	}
}
