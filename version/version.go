// Package version classifies and compares build version strings for the
// daemon's same-or-newer-wins socket eviction. A version is either a
// Release{Major,Minor,Patch} triple or a Dev{BuildUnixNano} build; every Dev
// outranks every Release, so a dev daemon is never evicted by a release. The
// dev sentinel "9999.<unix-nanos>.0-dev" parses as a triple, so Parse classifies
// Dev before any triple parsing — else newest-dev-wins inverts. The binary's own
// version string is injected by the consumer, never by this package.
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
	// Newer reports whether this build is strictly newer than other. Every Dev
	// outranks every Release; Dev-vs-Dev orders by BuildUnixNano; Release-vs-Release
	// by semver triple. Ties are never newer.
	Newer(other Version) bool
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

// Newer reports whether d is a strictly newer dev build than other; a Dev
// outranks every Release and orders against another Dev by BuildUnixNano.
func (d Dev) Newer(other Version) bool {
	o, ok := other.(Dev)
	if !ok {
		return true
	}
	return d.BuildUnixNano > o.BuildUnixNano
}

// Parse classifies s as a Dev or a Release. The "9999."-prefixed dev sentinel
// and every non-triple string classify Dev (plain "dev" gets zero nanos); this
// check precedes triple parsing because the sentinel form is itself a valid
// triple.
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

// Newer reports whether build a is strictly newer than build b under the Dev/
// Release taxonomy. A dev daemon is never evicted, and a dev binary always takes
// over a release daemon — preserving the dev-daemon workflow.
func Newer(a, b string) bool {
	return Parse(a).Newer(Parse(b))
}

// DevString is the one canonical dev version for a binary stamped at buildTime:
// "9999.<unix-nanos>.0-dev". The 9999 sentinel major keeps existing fleet
// daemons ordering correctly, and the nanosecond field lets same-second rebuilds
// still evict.
func DevString(buildTime time.Time) string {
	return fmt.Sprintf("%s%d.0-dev", devSentinelPrefix, buildTime.UnixNano())
}

// Resolve is the pure build-version rule: a stamped release passes through, an
// unstamped "dev" build with a known binary mtime becomes DevString(mtime), and
// an unstamped build with no mtime stays plain "dev".
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

// Running reports the running binary's build version, resolved and memoized on
// first call: a stamped release passes through; an unstamped "dev" build
// resolves the executable (os.Executable + EvalSymlinks) and stats it exactly
// once for DevString of its mtime, so a rebuild on disk never changes a running
// daemon's reported version.
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
