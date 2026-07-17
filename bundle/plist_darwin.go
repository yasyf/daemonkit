//go:build darwin

package bundle

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ShortVersion reads CFBundleShortVersionString from appPath's Contents/Info.plist.
func ShortVersion(appPath string) (string, error) {
	return readShortVersion(filepath.Join(appPath, "Contents", "Info.plist"))
}

// readShortVersion extracts CFBundleShortVersionString from an XML Info.plist
// (the format the release workflow writes). A binary plist — or a missing key —
// is an error.
func readShortVersion(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	dec := xml.NewDecoder(f)
	wantNext := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", path, err)
		}
		el, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch el.Name.Local {
		case "key":
			var k string
			if err := dec.DecodeElement(&k, &el); err != nil {
				return "", fmt.Errorf("parse %s: %w", path, err)
			}
			wantNext = k == "CFBundleShortVersionString"
		case "string":
			if !wantNext {
				continue
			}
			var v string
			if err := dec.DecodeElement(&v, &el); err != nil {
				return "", fmt.Errorf("parse %s: %w", path, err)
			}
			return v, nil
		default:
			wantNext = false
		}
	}
	return "", fmt.Errorf("no CFBundleShortVersionString in %s", path)
}
