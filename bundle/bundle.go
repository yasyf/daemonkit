// Package bundle reads a macOS .app's Info.plist and resolves the stable
// bundle paths a daemon installs to.
//
// macOS keys TCC, notification, and Full Disk Access grants to a bundle's
// identity — its identifier, signing team, and stable install path — and
// changing any of them resets the user's existing grants. Consumers therefore
// freeze their bundle identifier, team, and install path across releases.
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
