// Package label holds the one rule a daemon label must pass. Every path
// daemonkit joins a label into is joined past it, on every platform, so the
// service layers and the root package cannot disagree about what a label is.
package label

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Validate accepts exactly one path component with no leading or trailing dot,
// no "..", and nothing outside [A-Za-z0-9.-].
func Validate(label string) error {
	if label == "" || filepath.Base(label) != label || label == "." || label == ".." ||
		strings.HasPrefix(label, ".") || strings.HasSuffix(label, ".") || strings.Contains(label, "..") {
		return fmt.Errorf("label %q is not canonical", label)
	}
	for _, value := range label {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '.' || value == '-' {
			continue
		}
		return fmt.Errorf("label %q is not canonical", label)
	}
	return nil
}
