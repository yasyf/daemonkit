package bundle

import (
	"fmt"
	"os"
	"path/filepath"

	"howett.net/plist"
)

// ShortVersion reads CFBundleShortVersionString from an XML or binary Info.plist.
func ShortVersion(appPath string) (string, error) {
	return readShortVersion(filepath.Join(appPath, "Contents", "Info.plist"))
}

func readShortVersion(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var info struct {
		ShortVersion string `plist:"CFBundleShortVersionString"`
	}
	if err := plist.NewDecoder(file).Decode(&info); err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	if info.ShortVersion == "" {
		return "", fmt.Errorf("no CFBundleShortVersionString in %s", path)
	}
	return info.ShortVersion, nil
}
