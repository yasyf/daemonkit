//go:build darwin

package proc

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAppLaunchNewCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "application",
			want: []string{"open", "-n", "-g", "/Applications/Foo.app"},
		},
		{
			name: "child mode arguments",
			args: []string{"--fusekit-broker-child", "--daemon-socket", "/tmp/fusekit socket"},
			want: []string{
				"open", "-n", "-g", "/Applications/Foo.app", "--args",
				"--fusekit-broker-child", "--daemon-socket", "/tmp/fusekit socket",
			},
		},
		{
			name: "arguments stay literal",
			args: []string{"--value=$(touch /tmp/not-run)", "semi;colon", "--args"},
			want: []string{
				"open", "-n", "-g", "/Applications/Foo.app", "--args",
				"--value=$(touch /tmp/not-run)", "semi;colon", "--args",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "app.log")
			cmd, logFile, err := AppLaunchNew{App: "/Applications/Foo.app"}.launch(Spawn{
				LogPath: logPath,
				Args:    test.args,
			})
			if err != nil {
				t.Fatalf("launch: %v", err)
			}
			defer logFile.Close()
			if !reflect.DeepEqual(cmd.Args, test.want) {
				t.Errorf("argv = %q, want %q", cmd.Args, test.want)
			}
			if cmd.Stdout != logFile || cmd.Stderr != logFile {
				t.Errorf("Stdout/Stderr = %v/%v, want the log file %v", cmd.Stdout, cmd.Stderr, logFile)
			}
			if _, err := os.Stat(logPath); err != nil {
				t.Errorf("stat log: %v, want the log created", err)
			}
		})
	}
}
