package proc

import (
	"errors"
	"os"
	"os/exec"
)

// ErrAppLaunchUnsupported is the non-darwin refusal from AppLaunchNew — a
// permanent platform condition; never fold it into a transient "unavailable"
// sentinel, or a platform with no launch path is retried forever.
var ErrAppLaunchUnsupported = errors.New("launching a .app bundle is only supported on macOS")

// AppLaunchNew is the LaunchStrategy that brings a child up as a fresh,
// backgrounded .app instance via `open -n -g`, letting LaunchServices start it
// inside the user's Aqua session. A launchd daemon must never direct-exec the
// bundle's inner Mach-O: outside the Aqua session fuse-t volume bring-up and the
// volume-access TCC grant fail. Non-darwin builds refuse with
// ErrAppLaunchUnsupported.
type AppLaunchNew struct {
	// App is the .app bundle path to open.
	App string
}

func (a AppLaunchNew) launch(s Spawn) (*exec.Cmd, *os.File, error) { return appLaunchCmd(s, a.App) }
