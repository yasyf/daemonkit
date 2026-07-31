package launchd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/yasyf/daemonkit/internal/realhome"
)

const plistTemplateText = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
{{range .Args}}        <string>{{.}}</string>
{{end}}    </array>
    <key>RunAtLoad</key>
    <true/>
{{.Restart}}
{{if .StartInterval}}    <key>StartInterval</key>
    <integer>{{.StartInterval}}</integer>
{{end}}{{if .WatchPaths}}    <key>WatchPaths</key>
    <array>
{{range .WatchPaths}}        <string>{{.}}</string>
{{end}}    </array>
{{end}}{{if .StartCalendarInterval}}    <key>StartCalendarInterval</key>
    <array>
{{range .StartCalendarInterval}}        <dict>
{{.}}        </dict>
{{end}}    </array>
{{end}}
    <key>ThrottleInterval</key>
    <integer>10</integer>
{{if .ExitTimeOut}}    <key>ExitTimeOut</key>
    <integer>{{.ExitTimeOut}}</integer>
{{end}}{{if .ProcessType}}
    <key>ProcessType</key>
    <string>{{.ProcessType}}</string>
{{end}}
    <key>StandardOutPath</key>
    <string>{{.Log}}</string>
    <key>StandardErrorPath</key>
    <string>{{.Log}}</string>
{{if .AssociatedBundleIdentifiers}}    <key>AssociatedBundleIdentifiers</key>
    <array>
{{range .AssociatedBundleIdentifiers}}        <string>{{.}}</string>
{{end}}    </array>
{{end}}    <key>EnvironmentVariables</key>
    <dict>
{{range .Env}}        <key>{{.Key}}</key>
        <string>{{.Value}}</string>
{{end}}    </dict>
</dict>
</plist>
`

var plistTemplate = template.Must(template.New("plist").Parse(plistTemplateText))

// OwnerEnvKey is the EnvironmentVariables key every daemonkit-rendered plist
// carries so [Apply] and [Remove] can tell whether the plist at the one label
// they touch is daemonkit's, without an external store. It is reserved: an
// Agent.Env that sets it is rejected.
const OwnerEnvKey = "DAEMONKIT_AGENT_OWNER"

const ownerEnvValue = "daemonkit"

// <, >, and & are legal in APFS paths and would produce a plist launchctl rejects.
func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

type plistKV struct{ Key, Value string }

type plistData struct {
	Label                       string
	Args                        []string
	Log                         string
	Env                         []plistKV
	Restart                     string
	StartInterval               int64
	ExitTimeOut                 int64
	WatchPaths                  []string
	StartCalendarInterval       []string
	ProcessType                 string
	AssociatedBundleIdentifiers []string
}

// Agent is one exact desired macOS user LaunchAgent specification.
type Agent struct {
	// Label is the LaunchAgent label / reverse-DNS identifier naming the plist
	// and the launchctl service target. Required.
	Label string
	// Program is the exact non-symlinked absolute path launchd execs. Required.
	Program string
	// Args are the arguments passed after Program (e.g. {"daemon"}).
	Args []string
	// LogPath is where launchd points StandardOut/StandardError; its parent directory is created 0700.
	LogPath string
	// Env are EnvironmentVariables entries written into the plist. Keys are
	// emitted in sorted order so the rendered plist is reproducible. OwnerEnvKey
	// is reserved for the daemonkit ownership marker.
	Env map[string]string
	// AssociatedBundleIdentifiers attributes a launcher job to fixed signed
	// application bundle identifiers. Entries are canonical, unique, and
	// rendered in sorted order.
	AssociatedBundleIdentifiers []string
	// RestartPolicy defines when launchd restarts the daemon. Required.
	RestartPolicy RestartPolicy
	// StartInterval schedules the job at a whole-second interval. Zero omits
	// the launchd key.
	StartInterval time.Duration
	// ExitTimeOut is the grace launchd allows between the SIGTERM it sends a
	// job it is booting out and the SIGKILL that backstops a drain that wedges.
	// It is whole seconds; zero omits the launchd key and leaves launchd's own
	// 20-second default. Two agents whose exit timeouts differ are different
	// agents, so it is part of the digested identity — encoded only when set,
	// so a plan digested before the field existed still restores.
	ExitTimeOut time.Duration `json:",omitzero"`
	// WatchPaths starts the job whenever any listed path is modified. Entries
	// must be exact absolute paths. Empty omits the launchd key.
	WatchPaths []string
	// StartCalendarInterval schedules the job at each calendar match (launchd
	// treats the set as a logical OR). Empty omits the launchd key.
	StartCalendarInterval []CalendarInterval
	// ProcessType declares launchd's resource policy. The zero value omits the
	// launchd key.
	ProcessType ProcessType
	// LimitLoadToSessionType is accepted and dropped: never rendered into the
	// plist, never stored with the agent, and cleared from every agent daemonkit
	// canonicalizes, so a Plan reads it back as the zero value.
	//
	// Deprecated: launchd refuses any job whose session type names a domain
	// other than the bootstrap domain's own (error 134, "Service cannot load in
	// requested session"), and daemonkit bootstraps only into gui/<uid> — so
	// SessionTypeAqua was always a no-op and every other value was a guaranteed
	// permanent refusal. Setting anything but SessionTypeAqua logs a warning.
	// Removed in a future breaking release.
	LimitLoadToSessionType SessionType `json:"-"`
}

// PlistPath is the LaunchAgent plist location (~/Library/LaunchAgents/<Label>.plist),
// the home resolved through the passwd database so a sandboxed caller HOME
// cannot redirect the bootstrap.
func (a Agent) PlistPath() (string, error) {
	if err := validateLabel(a.Label); err != nil {
		return "", err
	}
	return plistPath(a.Label)
}

func plistPath(label string) (string, error) {
	home, err := realhome.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist"), nil
}

// Plist renders the exact LaunchAgent plist without mutating the filesystem.
func (a Agent) Plist() ([]byte, error) {
	if err := validateLabel(a.Label); err != nil {
		return nil, err
	}
	restart, err := a.RestartPolicy.plist()
	if err != nil {
		return nil, fmt.Errorf("render restart policy: %w", err)
	}
	startInterval, err := wholeSeconds("start interval", a.StartInterval)
	if err != nil {
		return nil, err
	}
	exitTimeOut, err := wholeSeconds("exit timeout", a.ExitTimeOut)
	if err != nil {
		return nil, err
	}
	processType, err := a.ProcessType.plistValue()
	if err != nil {
		return nil, err
	}
	associated, err := canonicalAssociatedBundleIdentifiers(a.AssociatedBundleIdentifiers)
	if err != nil {
		return nil, err
	}
	watchPaths, err := canonicalWatchPaths(a.WatchPaths)
	if err != nil {
		return nil, err
	}
	calendar, err := renderCalendarIntervals(a.StartCalendarInterval)
	if err != nil {
		return nil, err
	}
	bin, err := a.programPath()
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(a.LogPath) || filepath.Clean(a.LogPath) != a.LogPath {
		return nil, fmt.Errorf("launchd: log path %q is not exact and absolute", a.LogPath)
	}
	args := make([]string, 0, len(a.Args)+1)
	args = append(args, xmlEscape(bin))
	for _, arg := range a.Args {
		args = append(args, xmlEscape(arg))
	}
	env, err := a.renderEnv()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := plistTemplate.Execute(&buf, plistData{
		Label:                       xmlEscape(a.Label),
		Args:                        args,
		Log:                         xmlEscape(a.LogPath),
		Env:                         env,
		Restart:                     restart,
		StartInterval:               startInterval,
		ExitTimeOut:                 exitTimeOut,
		WatchPaths:                  watchPaths,
		StartCalendarInterval:       calendar,
		ProcessType:                 processType,
		AssociatedBundleIdentifiers: associated,
	}); err != nil {
		return nil, fmt.Errorf("render plist: %w", err)
	}
	return buf.Bytes(), nil
}

// renderEnv merges the daemonkit ownership marker into the agent's environment
// and returns the entries in sorted key order. The marker is injected only at
// render time, so it never enters the agent's digest.
func (a Agent) renderEnv() ([]plistKV, error) {
	merged := make(map[string]string, len(a.Env)+1)
	for key, value := range a.Env {
		if key == OwnerEnvKey {
			return nil, fmt.Errorf("launchd: environment variable %q is reserved for the daemonkit ownership marker", OwnerEnvKey)
		}
		merged[key] = value
	}
	merged[OwnerEnvKey] = ownerEnvValue
	env := make([]plistKV, 0, len(merged))
	for _, key := range slices.Sorted(maps.Keys(merged)) {
		env = append(env, plistKV{Key: xmlEscape(key), Value: xmlEscape(merged[key])})
	}
	return env, nil
}

func (a Agent) programPath() (string, error) {
	bin := a.Program
	if bin == "" || !filepath.IsAbs(bin) || filepath.Clean(bin) != bin {
		return "", fmt.Errorf("launchd: program path %q is not exact and absolute", bin)
	}
	return bin, nil
}

func canonicalAssociatedBundleIdentifiers(values []string) ([]string, error) {
	canonical := append([]string(nil), values...)
	slices.Sort(canonical)
	for index, value := range canonical {
		if !validBundleIdentifier(value) {
			return nil, fmt.Errorf("launchd: associated bundle identifier %q is not canonical", value)
		}
		if index > 0 && canonical[index-1] == value {
			return nil, fmt.Errorf("launchd: associated bundle identifier %q is duplicated", value)
		}
	}
	return canonical, nil
}

func validBundleIdentifier(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
		for _, char := range []byte(part) {
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func validateLabel(label string) error {
	if label == "" || filepath.Base(label) != label || label == "." || label == ".." ||
		strings.HasPrefix(label, ".") || strings.HasSuffix(label, ".") || strings.Contains(label, "..") {
		return fmt.Errorf("launchd: launch agent label %q is not canonical", label)
	}
	for _, value := range label {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '.' || value == '-' {
			continue
		}
		return fmt.Errorf("launchd: launch agent label %q is not canonical", label)
	}
	return nil
}
