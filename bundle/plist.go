package bundle

import (
	"fmt"
	"os"
	"path/filepath"

	"howett.net/plist"
)

// ShortVersion reads CFBundleShortVersionString from an XML or binary Info.plist.
func ShortVersion(appPath string) (string, error) {
	return StringValue(filepath.Join(appPath, "Contents", "Info.plist"), "CFBundleShortVersionString")
}

// StringValue reads a non-empty string-valued key from an XML or binary plist.
func StringValue(path, key string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var values map[string]any
	if err := plist.NewDecoder(file).Decode(&values); err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	raw, ok := values[key]
	if !ok {
		return "", fmt.Errorf("no %s in %s", key, path)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s in %s is not a string", key, path)
	}
	if value == "" {
		return "", fmt.Errorf("%s in %s is empty", key, path)
	}
	return value, nil
}
