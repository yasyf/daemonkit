//go:build darwin

package proc

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAppLaunchNewCommand(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "app.log")
	app := "/Applications/Foo.app"

	cmd, logFile, err := AppLaunchNew{App: app}.launch(Spawn{LogPath: logPath})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	defer logFile.Close()

	wantArgs := []string{"open", "-n", "-g", app}
	if !reflect.DeepEqual(cmd.Args, wantArgs) {
		t.Errorf("argv = %q, want %q", cmd.Args, wantArgs)
	}
	if cmd.Stdout != logFile || cmd.Stderr != logFile {
		t.Errorf("Stdout/Stderr = %v/%v, want the log file %v", cmd.Stdout, cmd.Stderr, logFile)
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Errorf("stat log: %v, want the log created", err)
	}
}
