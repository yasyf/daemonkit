package daemon

import (
	"os"
	"strings"
	"testing"
)

func TestExtendPathAppendsMissingUserDirsOnce(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin")

	extendPath()
	want := "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin:" +
		home + "/.local/bin:" + home + "/.bun/bin"
	if got := os.Getenv("PATH"); got != want {
		t.Fatalf("PATH = %q, want %q", got, want)
	}

	extendPath()
	if got := os.Getenv("PATH"); got != want {
		t.Fatalf("PATH after second extend = %q, want %q", got, want)
	}
	if strings.Count(os.Getenv("PATH"), "/opt/homebrew/bin") != 1 {
		t.Fatalf("duplicate entries in %q", os.Getenv("PATH"))
	}
}

func TestExtendPathSeedsHermeticBaseWhenEmpty(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", "")

	extendPath()
	want := "/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/homebrew/bin:" +
		home + "/.local/bin:" + home + "/.bun/bin"
	if got := os.Getenv("PATH"); got != want {
		t.Fatalf("PATH = %q, want %q", got, want)
	}
}
