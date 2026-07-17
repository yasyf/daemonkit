//go:build !darwin

package proc

import (
	"errors"
	"testing"
)

func TestAppLaunchNewUnsupported(t *testing.T) {
	if _, _, err := (AppLaunchNew{App: "/x/Foo.app"}).launch(Spawn{}); !errors.Is(err, ErrAppLaunchUnsupported) {
		t.Errorf("launch off darwin = %v, want ErrAppLaunchUnsupported", err)
	}
}
