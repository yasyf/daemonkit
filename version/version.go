// Package version classifies and compares release and development builds for
// launcher-owned runtime settlement and release ordering.
package version

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const devSentinelPrefix = "9999."

var releaseTriple = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)`)

// Version is a classified build version: either a Release or a Dev.
type Version interface {
	// Newer reports whether this build is strictly newer than other.
	Newer(other Version) bool
	// Equal reports whether this build is the exact same build as other.
	Equal(other Version) bool
}

// Release is a v?X.Y.Z release triple.
type Release struct {
	Major, Minor, Patch int
}

// Dev is an unreleased build ordered by the nanosecond the binary was stamped;
// plain "dev" has BuildUnixNano zero.
type Dev struct {
	BuildUnixNano int64
}

// Newer reports whether r is a strictly newer release than other; a Release
// never outranks a Dev.
func (r Release) Newer(other Version) bool {
	o, ok := other.(Release)
	if !ok {
		return false
	}
	if r.Major != o.Major {
		return r.Major > o.Major
	}
	if r.Minor != o.Minor {
		return r.Minor > o.Minor
	}
	return r.Patch > o.Patch
}

// Equal reports whether r is the exact same release as other; a Release is never
// equal to a Dev, and v?X.Y.Z parses to one Release regardless of the "v" prefix,
// so TAG and BARE spellings of one release compare equal.
func (r Release) Equal(other Version) bool {
	o, ok := other.(Release)
	return ok && r == o
}

// Newer reports whether d is a strictly newer dev build than other; a Dev
// outranks every Release and orders against another Dev by BuildUnixNano.
func (d Dev) Newer(other Version) bool {
	o, ok := other.(Dev)
	if !ok {
		return true
	}
	return d.BuildUnixNano > o.BuildUnixNano
}

// Equal reports whether d is the exact same dev build as other, ordered by the
// nanosecond the binary was stamped; a Dev is never equal to a Release.
func (d Dev) Equal(other Version) bool {
	o, ok := other.(Dev)
	return ok && d == o
}

// Parse classifies s as a Dev or a Release. The "9999."-prefixed dev sentinel
// and every non-triple string classify Dev; this check precedes triple
// parsing because the sentinel form is itself a valid triple — else
// newest-dev-wins inverts.
func Parse(s string) Version {
	s = strings.TrimSpace(s)
	if nanos, ok := parseDevSentinel(s); ok {
		return Dev{BuildUnixNano: nanos}
	}
	if r, ok := parseRelease(s); ok {
		return r
	}
	return Dev{}
}

func parseDevSentinel(s string) (int64, bool) {
	rest, ok := strings.CutPrefix(s, devSentinelPrefix)
	if !ok {
		return 0, false
	}
	nanos, _, ok := strings.Cut(rest, ".")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseRelease(s string) (Release, bool) {
	m := releaseTriple.FindStringSubmatch(s)
	if m == nil {
		return Release{}, false
	}
	var t [3]int
	for i := range t {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return Release{}, false
		}
		t[i] = n
	}
	return Release{Major: t[0], Minor: t[1], Patch: t[2]}, true
}

// Newer reports whether build a is strictly newer than build b.
func Newer(a, b string) bool {
	return Parse(a).Newer(Parse(b))
}

// Equal reports whether builds a and b are the exact same build. Two spellings
// of one release ("v12.15.3" and "12.15.3") are equal; a release and a dev
// build, or two different builds, never are.
func Equal(a, b string) bool {
	return Parse(a).Equal(Parse(b))
}

// DevString returns the canonical development version for buildTime.
func DevString(buildTime time.Time) string {
	return fmt.Sprintf("%s%d.0-dev", devSentinelPrefix, buildTime.UnixNano())
}

// Resolve applies the build-version rule to a stamp and binary modification time.
func Resolve(stamped string, mtime time.Time, ok bool) string {
	if stamped != "dev" {
		return stamped
	}
	if !ok {
		return "dev"
	}
	return DevString(mtime)
}

var (
	runningOnce sync.Once
	runningVer  string
)

// Running returns the running binary's resolved, memoized build version.
func Running(stamped string) string {
	runningOnce.Do(func() { runningVer = resolveRunning(stamped) })
	return runningVer
}

func resolveRunning(stamped string) string {
	if stamped != "dev" {
		return stamped
	}
	exe, err := os.Executable()
	if err != nil {
		return Resolve(stamped, time.Time{}, false)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return Resolve(stamped, time.Time{}, false)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return Resolve(stamped, time.Time{}, false)
	}
	return Resolve(stamped, info.ModTime(), true)
}
