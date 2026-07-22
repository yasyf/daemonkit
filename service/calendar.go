package service

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CalendarInterval is one launchd StartCalendarInterval match. A nil field is
// a wildcard; a set field must match for the job to fire. launchd treats a set
// of intervals as a logical OR.
type CalendarInterval struct {
	// Minute is 0-59.
	Minute *int
	// Hour is 0-23.
	Hour *int
	// Day is the day of the month, 1-31.
	Day *int
	// Weekday is 0-7 (0 and 7 are both Sunday).
	Weekday *int
	// Month is 1-12.
	Month *int
}

// Daily returns a CalendarInterval that fires once a day at hour:minute.
func Daily(hour, minute int) CalendarInterval {
	return CalendarInterval{Hour: &hour, Minute: &minute}
}

func (c CalendarInterval) plistBody() (string, error) {
	fields := []struct {
		key   string
		value *int
		lo    int
		hi    int
	}{
		{"Minute", c.Minute, 0, 59},
		{"Hour", c.Hour, 0, 23},
		{"Day", c.Day, 1, 31},
		{"Weekday", c.Weekday, 0, 7},
		{"Month", c.Month, 1, 12},
	}
	var b strings.Builder
	set := 0
	for _, f := range fields {
		if f.value == nil {
			continue
		}
		if *f.value < f.lo || *f.value > f.hi {
			return "", fmt.Errorf("service: calendar %s=%d out of range [%d,%d]", f.key, *f.value, f.lo, f.hi)
		}
		fmt.Fprintf(&b, "            <key>%s</key>\n            <integer>%d</integer>\n", f.key, *f.value)
		set++
	}
	if set == 0 {
		return "", fmt.Errorf("service: calendar interval has no fields set")
	}
	return b.String(), nil
}

func renderCalendarIntervals(intervals []CalendarInterval) ([]string, error) {
	out := make([]string, 0, len(intervals))
	for i, iv := range intervals {
		body, err := iv.plistBody()
		if err != nil {
			return nil, fmt.Errorf("calendar interval %d: %w", i, err)
		}
		out = append(out, body)
	}
	return out, nil
}

func canonicalWatchPaths(paths []string) ([]string, error) {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return nil, fmt.Errorf("service: watch path %q is not exact and absolute", p)
		}
		out = append(out, xmlEscape(p))
	}
	return out, nil
}
