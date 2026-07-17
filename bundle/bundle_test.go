package bundle

import "testing"

func TestAppPath(t *testing.T) {
	cases := []struct{ name, dir, bundle, want string }{
		{"applications", "/Applications", "fusekit-holder", "/Applications/fusekit-holder.app"},
		{"nested dir", "/opt/x", "Foo", "/opt/x/Foo.app"},
		{"trailing slash cleaned", "/a/b/", "Bar", "/a/b/Bar.app"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := AppPath(tc.dir, tc.bundle); got != tc.want {
				t.Errorf("AppPath(%q, %q) = %q, want %q", tc.dir, tc.bundle, got, tc.want)
			}
		})
	}
}

func TestExePath(t *testing.T) {
	cases := []struct{ name, app, bin, want string }{
		{"holder", "/Applications/fusekit-holder.app", "fusekit-holder", "/Applications/fusekit-holder.app/Contents/MacOS/fusekit-holder"},
		{"foo", "/Applications/Foo.app", "foo", "/Applications/Foo.app/Contents/MacOS/foo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExePath(tc.app, tc.bin); got != tc.want {
				t.Errorf("ExePath(%q, %q) = %q, want %q", tc.app, tc.bin, got, tc.want)
			}
		})
	}
}
