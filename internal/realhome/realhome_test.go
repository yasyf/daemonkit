package realhome

import (
	"os/user"
	"path/filepath"
	"testing"
)

func TestDirIgnoresCallerHome(t *testing.T) {
	poisoned := t.TempDir()
	t.Setenv("HOME", poisoned)
	t.Setenv(EnvOverride, "")

	got, err := Dir()
	if err != nil {
		t.Fatal(err)
	}
	if got == poisoned {
		t.Fatalf("Dir() = %q, followed the caller HOME", got)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if got != current.HomeDir {
		t.Fatalf("Dir() = %q, want passwd home %q", got, current.HomeDir)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("Dir() = %q, want an absolute path", got)
	}
}

func TestDirHonorsOverride(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	override := t.TempDir()
	t.Setenv(EnvOverride, override)

	got, err := Dir()
	if err != nil {
		t.Fatal(err)
	}
	if got != override {
		t.Fatalf("Dir() = %q, want override %q", got, override)
	}
}
