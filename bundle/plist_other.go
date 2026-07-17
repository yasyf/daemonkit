//go:build !darwin

package bundle

import "errors"

// ShortVersion reports that reading an .app Info.plist is darwin-only.
func ShortVersion(appPath string) (string, error) {
	return "", errors.New("bundle: reading Info.plist is only supported on darwin")
}
