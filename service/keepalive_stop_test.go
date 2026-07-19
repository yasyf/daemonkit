package service

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

type recordingAppReaper struct {
	events       *[]string
	record       proc.Record
	reapErr      error
	trackErr     error
	terminateErr error
	onTerminate  func()
}

type recordingAppRecovery struct{ events *[]string }

func (r recordingAppRecovery) Reap(context.Context) error {
	*r.events = append(*r.events, "recover")
	return nil
}

func (r *recordingAppReaper) Reap(context.Context) error {
	*r.events = append(*r.events, "reap-workers")
	return r.reapErr
}

func (r *recordingAppReaper) TrackIdentity(_ context.Context, identity proc.Identity) (proc.Record, error) {
	*r.events = append(*r.events, "track")
	if r.trackErr != nil {
		return proc.Record{}, r.trackErr
	}
	r.record = proc.Record{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Generation: "stop",
	}
	return r.record, nil
}

func (r *recordingAppReaper) Terminate(_ context.Context, record proc.Record) error {
	*r.events = append(*r.events, "terminate")
	if record != r.record {
		return errors.New("unexpected process record")
	}
	if r.onTerminate != nil {
		r.onTerminate()
	}
	return r.terminateErr
}

func fixedAppFixture(t *testing.T) (AppKeepAlive, AppStopSpec, AuthenticatedAppPeer, *[]string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(root, "Fixed.app")
	executable := filepath.Join(app, "Contents", "MacOS", "Fixed")
	if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("fixed"), 0o700); err != nil {
		t.Fatal(err)
	}
	events := &[]string{}
	reaper := &recordingAppReaper{events: events}
	peer := wire.Peer{
		PID: 4242, UID: os.Geteuid(), StartTime: "111.222", Comm: "Fixed", Boot: "boot", Executable: executable,
		Audit: make([]byte, 32),
	}
	requirement := trust.Requirement{TeamID: "TEAM", SigningIdentifier: "com.example.fixed"}
	validationDigest, err := requirement.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	expected := NewAuthenticatedAppPeer(peer, validationDigest)
	now := time.Unix(100, 0)
	spec := AppStopSpec{
		ExecutableName:              "Fixed",
		Requirement:                 requirement,
		EntitlementValidationDigest: validationDigest,
		Reaper:                      reaper, Dependents: recordingAppRecovery{events: events},
		Dial: func(context.Context) (net.Conn, error) {
			*events = append(*events, "dial")
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
		peerFromConn: func(net.Conn) (wire.Peer, error) {
			*events = append(*events, "peer")
			return peer, nil
		},
		checkPeer: func(got wire.Peer, _ trust.Requirement) error {
			*events = append(*events, "trust")
			if got.UID != peer.UID || got.ProcessIdentity() != peer.ProcessIdentity() {
				return errors.New("wrong peer")
			}
			return nil
		},
		processes: func(string) ([]proc.Identity, error) {
			*events = append(*events, "inventory")
			return []proc.Identity{peer.ProcessIdentity()}, nil
		},
		now: func() time.Time { return now },
		pause: func(context.Context, time.Duration) error {
			now = now.Add(25 * time.Millisecond)
			return nil
		},
		deadline: 500 * time.Millisecond,
		quiet:    50 * time.Millisecond,
	}
	return AppKeepAlive{Label: "com.example.fixed", AppPath: app, BundleID: "com.example.fixed", RestartPolicy: RestartAlways}, spec, expected, events, executable
}

func launchState(t *testing.T, keepalive AppKeepAlive, events *[]string, loaded *bool) {
	t.Helper()
	notLoadedErr := shExit(t, 3)
	stubLaunchctl(t, func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "print":
			*events = append(*events, "print")
			if *loaded {
				return "loaded", nil
			}
			return "not loaded", notLoadedErr
		case "bootout":
			if !slices.Equal(args, []string{"bootout", serviceTarget(keepalive.Label)}) {
				t.Fatalf("launchctl args = %v", args)
			}
			*events = append(*events, "bootout")
			*loaded = false
			return "", nil
		default:
			t.Fatalf("unexpected launchctl args %v", args)
			return "", nil
		}
	})
}

func TestAppKeepAliveStopVerifiesTracksBootsOutTerminatesAndProvesAbsence(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	live := true
	loaded := true
	reaper := spec.Reaper.(*recordingAppReaper)
	reaper.onTerminate = func() { live = false }
	spec.Dial = func(context.Context) (net.Conn, error) {
		*events = append(*events, "dial")
		if !live {
			return nil, syscall.ENOENT
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}
	spec.processes = func(string) ([]proc.Identity, error) {
		*events = append(*events, "inventory")
		if live {
			return []proc.Identity{{PID: 4242}}, nil
		}
		return nil, nil
	}
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"reap-workers", "peer", "trust", "track", "bootout", "terminate", "recover"} {
		if !slices.Contains(*events, event) {
			t.Fatalf("events = %v, missing %q", *events, event)
		}
	}
	if slices.Index(*events, "track") > slices.Index(*events, "bootout") || slices.Index(*events, "terminate") > slices.Index(*events, "recover") {
		t.Fatalf("unsafe stop order: %v", *events)
	}
}

func TestAppKeepAliveStopRejectsAuthenticatedReplacement(t *testing.T) {
	keepalive, spec, expected, events, executable := fixedAppFixture(t)
	generation := 1
	loaded := true
	reaper := spec.Reaper.(*recordingAppReaper)
	reaper.onTerminate = func() {
		if generation == 1 {
			generation = 2
			loaded = true
		} else {
			generation = 0
		}
	}
	spec.Dial = func(context.Context) (net.Conn, error) {
		if generation == 0 {
			return nil, syscall.ENOENT
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}
	spec.peerFromConn = func(net.Conn) (wire.Peer, error) {
		return wire.Peer{PID: 4241 + generation, UID: os.Geteuid(), StartTime: time.Now().String(), Boot: "boot", Executable: executable, Audit: make([]byte, 32)}, nil
	}
	spec.checkPeer = func(wire.Peer, trust.Requirement) error { return nil }
	spec.processes = func(string) ([]proc.Identity, error) {
		if generation == 0 {
			return nil, nil
		}
		return []proc.Identity{{PID: 4241 + generation}}, nil
	}
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
		t.Fatal("Stop accepted a replacement app that did not own the proof")
	}
	if got := count(*events, "terminate"); got != 0 {
		t.Fatalf("terminate count = %d, want 0; events=%v", got, *events)
	}
}

func TestAppKeepAliveStopWaitsForRebindingLiveAppBeforeBootout(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	live, bound := true, false
	loaded := true
	reaper := spec.Reaper.(*recordingAppReaper)
	reaper.onTerminate = func() { live = false }
	spec.Dial = func(context.Context) (net.Conn, error) {
		if !bound {
			bound = true
			return nil, syscall.ECONNREFUSED
		}
		if !live {
			return nil, syscall.ENOENT
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}
	spec.processes = func(string) ([]proc.Identity, error) {
		if live {
			return []proc.Identity{{PID: 4242}}, nil
		}
		return nil, nil
	}
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err != nil {
		t.Fatal(err)
	}
	if slices.Index(*events, "bootout") < slices.Index(*events, "track") {
		t.Fatalf("bootout preceded authenticated tracking: %v", *events)
	}
}

func TestAppKeepAliveStopNoSocketLiveProcessFailsClosed(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	loaded := true
	spec.Dial = func(context.Context) (net.Conn, error) { return nil, syscall.ENOENT }
	spec.processes = func(string) ([]proc.Identity, error) { return []proc.Identity{{PID: 4242}}, nil }
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
		t.Fatal("Stop claimed absence while an exact executable process remained live without a socket")
	}
	if slices.Contains(*events, "bootout") || slices.Contains(*events, "terminate") {
		t.Fatalf("unauthenticated live app gained kill authority: %v", *events)
	}
}

func TestAppKeepAliveStopAbsentUsesServiceAndInventoryQuietProof(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	loaded := true
	spec.Dial = func(context.Context) (net.Conn, error) { return nil, syscall.ENOENT }
	spec.processes = func(string) ([]proc.Identity, error) { return nil, nil }
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(*events, "bootout") || !slices.Contains(*events, "recover") {
		t.Fatalf("events = %v, want bootout and dependent recovery", *events)
	}
}

func TestAppKeepAliveStopRestartsQuietProofAfterServiceReload(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	loaded := false
	spec.Dial = func(context.Context) (net.Conn, error) { return nil, syscall.ENOENT }
	spec.processes = func(string) ([]proc.Identity, error) { return nil, nil }
	now := time.Unix(100, 0)
	pauses := 0
	spec.now = func() time.Time { return now }
	spec.pause = func(context.Context, time.Duration) error {
		pauses++
		now = now.Add(25 * time.Millisecond)
		if pauses == 2 {
			loaded = true
		}
		return nil
	}
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err != nil {
		t.Fatal(err)
	}
	if pauses < 4 {
		t.Fatalf("Stop settled after %d pauses, want a fresh quiet interval after reload", pauses)
	}
	if got := count(*events, "bootout"); got != 1 {
		t.Fatalf("bootout count = %d, want 1; events=%v", got, *events)
	}
}

func TestAppKeepAliveStopRepeatedServiceReloadFailsByDeadline(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	loaded := false
	spec.Dial = func(context.Context) (net.Conn, error) { return nil, syscall.ENOENT }
	spec.processes = func(string) ([]proc.Identity, error) { return nil, nil }
	now := time.Unix(100, 0)
	pauses := 0
	spec.now = func() time.Time { return now }
	spec.deadline = 100 * time.Millisecond
	spec.pause = func(context.Context, time.Duration) error {
		pauses++
		now = now.Add(25 * time.Millisecond)
		loaded = true
		return nil
	}
	launchState(t, keepalive, events, &loaded)
	if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
		t.Fatal("Stop accepted a service that reloaded throughout the quiet window")
	}
	if got := count(*events, "bootout"); got < 2 {
		t.Fatalf("bootout count = %d, want repeated reload settlement; events=%v", got, *events)
	}
	if slices.Contains(*events, "recover") {
		t.Fatalf("dependent recovery ran without continuous absence: %v", *events)
	}
}

func TestAppKeepAliveStopRejectsSignatureAndPIDReuseBeforeBootout(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*AppStopSpec)
	}{
		{"signature", func(spec *AppStopSpec) {
			spec.checkPeer = func(wire.Peer, trust.Requirement) error { return trust.ErrUntrustedPeer }
		}},
		{"pid reuse", func(spec *AppStopSpec) { spec.Reaper.(*recordingAppReaper).trackErr = proc.ErrIdentityChanged }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keepalive, spec, expected, events, _ := fixedAppFixture(t)
			tc.set(&spec)
			loaded := true
			launchState(t, keepalive, events, &loaded)
			if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
				t.Fatal("Stop accepted untrusted or recycled peer identity")
			}
			if slices.Contains(*events, "bootout") || slices.Contains(*events, "terminate") {
				t.Fatalf("events = %v, want no service/process mutation", *events)
			}
		})
	}
}

func TestAppKeepAliveStopRejectsBootAuditAndValidationDigestMismatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*AuthenticatedAppPeer)
	}{
		{"boot", func(peer *AuthenticatedAppPeer) { peer.Boot = "previous-boot" }},
		{"audit token", func(peer *AuthenticatedAppPeer) { peer.AuditTokenDigest[0]++ }},
		{"validation digest", func(peer *AuthenticatedAppPeer) { peer.EntitlementValidationDigest[0]++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keepalive, spec, expected, events, _ := fixedAppFixture(t)
			tc.set(&expected)
			loaded := true
			launchState(t, keepalive, events, &loaded)
			if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
				t.Fatal("Stop accepted changed authenticated proof identity")
			}
			if slices.Contains(*events, "bootout") || slices.Contains(*events, "terminate") {
				t.Fatalf("events = %v, want no service/process mutation", *events)
			}
		})
	}
}

func TestAppKeepAliveStopReapsStaleWorkersBeforeDial(t *testing.T) {
	keepalive, spec, expected, events, _ := fixedAppFixture(t)
	want := errors.New("stale worker settlement failed")
	spec.Reaper.(*recordingAppReaper).reapErr = want
	spec.Dial = func(context.Context) (net.Conn, error) {
		t.Fatal("dialed before stale worker recovery")
		return nil, nil
	}
	if err := keepalive.Stop(t.Context(), spec, expected); !errors.Is(err, want) {
		t.Fatalf("Stop error = %v, want %v", err, want)
	}
	if !slices.Equal(*events, []string{"reap-workers"}) {
		t.Fatalf("events = %v", *events)
	}
}

func TestAppKeepAliveStopRejectsSymlinkedApp(t *testing.T) {
	keepalive, spec, expected, _, _ := fixedAppFixture(t)
	target := keepalive.AppPath
	link := filepath.Join(filepath.Dir(target), "Link.app")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	keepalive.AppPath = link
	if err := keepalive.Stop(t.Context(), spec, expected); err == nil {
		t.Fatal("Stop accepted symlinked fixed app")
	}
}

func count(values []string, needle string) int {
	n := 0
	for _, value := range values {
		if value == needle {
			n++
		}
	}
	return n
}
