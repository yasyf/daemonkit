// Package bundle reads a macOS .app's Info.plist and resolves the stable
// bundle paths a daemon installs to. macOS keys TCC and notification grants
// to bundle identity — identifier, team, install path — so consumers freeze
// all three across releases.
package bundle

import "path/filepath"

// AppPath returns the stable <dir>/<name>.app location for a bundle.
func AppPath(dir, name string) string {
	return filepath.Join(dir, name+".app")
}

// ExePath returns the inner Mach-O at <appPath>/Contents/MacOS/<bin>.
func ExePath(appPath, bin string) string {
	return filepath.Join(appPath, "Contents", "MacOS", bin)
}
