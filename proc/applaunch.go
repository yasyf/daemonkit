package proc

import (
	"errors"
	"os"
	"os/exec"
)

// ErrAppLaunchUnsupported is the non-darwin refusal from AppLaunchNew — a
// permanent platform condition, never folded into a transient sentinel (or a
// platform with no launch path is retried forever).
var ErrAppLaunchUnsupported = errors.New("launching a .app bundle is only supported on macOS")

// AppLaunchNew is the LaunchStrategy that starts a child as a backgrounded
// .app via `open -n -g`, inside the user's Aqua session. A launchd daemon
// must never direct-exec the bundle's inner Mach-O: outside Aqua, fuse-t
// volume bring-up and the volume-access TCC grant fail.
type AppLaunchNew struct {
	App string
}

func (a AppLaunchNew) launch(s Spawn) (*exec.Cmd, *os.File, error) { return appLaunchCmd(s, a.App) }
