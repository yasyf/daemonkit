package service

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

const (
	stopControlHelperMode          = "DAEMONKIT_TEST_STOP_CONTROL_MODE"
	stopControlHelperReleaseMarker = "DAEMONKIT_TEST_STOP_CONTROL_RELEASE_MARKER"
)

func TestStopControlSpecRequiresExactAbsoluteRolePath(t *testing.T) {
	for _, path := range []string{"daemon", "./daemon", "/tmp/../tmp/daemon"} {
		t.Run(path, func(t *testing.T) {
			_, err := validateStopControlSpec(StopControlSpec{
				Executable: path, Role: "com.example.stop", RuntimeBuild: "v2", RuntimeProtocol: 1,
				TargetProcessGeneration: "runtime", Intent: "upgrade",
			})
			if err == nil {
				t.Fatal("validateStopControlSpec accepted an inexact role path")
			}
		})
	}
}

func TestStopControlFrameAcceptsExactLimitAndRejectsOverflow(t *testing.T) {
	value := strings.Repeat("x", stopControlFrameLimit-2)
	var encoded bytes.Buffer
	if err := writeStopFrame(&encoded, value); err != nil {
		t.Fatalf("write exact limit: %v", err)
	}
	if encoded.Len() != stopControlFrameLimit+1 {
		t.Fatalf("encoded length = %d, want %d", encoded.Len(), stopControlFrameLimit+1)
	}
	var got string
	reader := bufio.NewReaderSize(bytes.NewReader(encoded.Bytes()), stopControlFrameLimit+1)
	if err := readStopFrame(t.Context(), reader, &got); err != nil {
		t.Fatalf("read exact limit: %v", err)
	}
	if got != value {
		t.Fatal("exact-limit frame changed payload")
	}
	if err := writeStopFrame(io.Discard, strings.Repeat("x", stopControlFrameLimit-1)); err == nil {
		t.Fatal("writeStopFrame accepted an oversized frame")
	}
}

func TestStopControlFrameRejectsBeforeGrowingPastBound(t *testing.T) {
	payload := append(bytes.Repeat([]byte{'x'}, stopControlFrameLimit+1), '\n')
	reader := bufio.NewReaderSize(bytes.NewReader(payload), stopControlFrameLimit+1)
	var value any
	if err := readStopFrame(t.Context(), reader, &value); err == nil {
		t.Fatal("readStopFrame accepted an oversized frame")
	}
}

type oneByteWriter struct{ bytes.Buffer }

func (w *oneByteWriter) Write(payload []byte) (int, error) {
	if len(payload) > 1 {
		payload = payload[:1]
	}
	return w.Buffer.Write(payload)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestStopControlFrameHandlesShortWrites(t *testing.T) {
	var writer oneByteWriter
	if err := writeStopFrame(&writer, "value"); err != nil {
		t.Fatalf("writeStopFrame one-byte writer: %v", err)
	}
	if writer.String() != "\"value\"\n" {
		t.Fatalf("short-write output = %q", writer.String())
	}
	if err := writeStopFrame(zeroWriter{}, "value"); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write error = %v, want io.ErrShortWrite", err)
	}
}

func TestStopControlFrameRejectsUnknownAndTrailingFields(t *testing.T) {
	type exact struct {
		Value string `json:"value"`
	}
	for _, payload := range []string{
		`{"value":"ok","extra":true}` + "\n",
		`{"value":"ok"} {}` + "\n",
	} {
		reader := bufio.NewReaderSize(strings.NewReader(payload), stopControlFrameLimit+1)
		var value exact
		if err := readStopFrame(t.Context(), reader, &value); err == nil {
			t.Fatalf("readStopFrame accepted %q", payload)
		}
	}
}

func TestStopControlHelperProcess(_ *testing.T) {
	mode := os.Getenv(stopControlHelperMode)
	if mode == "" {
		return
	}
	if mode == "before-identity" {
		select {}
	}
	report := os.NewFile(stopReportFD, "daemonkit-test-stop-report")
	release := os.NewFile(stopReleaseFD, "daemonkit-test-stop-release")
	if report == nil || release == nil {
		os.Exit(91)
	}
	identity, err := currentStopChildIdentity()
	if err != nil || writeStopFrame(report, identity) != nil {
		os.Exit(92)
	}
	var released [1]byte
	if _, err := io.ReadFull(release, released[:]); err != nil || released[0] != 1 {
		os.Exit(93)
	}
	if marker := os.Getenv(stopControlHelperReleaseMarker); marker != "" {
		if err := os.WriteFile(marker, []byte("released"), 0o600); err != nil {
			os.Exit(96)
		}
	}
	if mode == "after-release" {
		select {}
	}
	if mode == "after-authority-expiry" || mode == "declined" {
		if mode == "after-authority-expiry" {
			time.Sleep(400 * time.Millisecond)
		}
		result := stopChildResult{Result: wire.StopResult{
			ProcessGeneration: "runtime-generation",
			RuntimeBuild:      "v1.0.0",
			RuntimeProtocol:   1,
			Stopped:           mode != "declined",
		}}
		if writeStopFrame(report, result) != nil {
			os.Exit(95)
		}
		os.Exit(0)
	}
	os.Exit(94)
}

func TestStopRuntimeBoundsAndReapsWedgedChildPhases(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"before-identity", "after-release"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(stopControlHelperMode, mode)
			generation, err := proc.ProcessGeneration()
			if err != nil {
				t.Fatal(err)
			}
			store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")}
			controller := &Controller{stopReaper: &proc.Reaper{Store: store, Generation: generation}}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err = controller.StopRuntime(ctx, StopControlSpec{
				Executable: executable,
				Args:       []string{"-test.run=^TestStopControlHelperProcess$"},
				Role:       "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
				TargetProcessGeneration: "runtime-generation", Intent: "restart",
			})
			if err == nil {
				t.Fatal("StopRuntime accepted a wedged helper")
			}
			if elapsed := time.Since(started); elapsed > stopChildSettleBound+time.Second {
				t.Fatalf("StopRuntime took %v, exceeded hard settlement bound", elapsed)
			}
			records, loadErr := store.Load(t.Context())
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if len(records) != 0 {
				t.Fatalf("settled helper records = %+v, want none", records)
			}
		})
	}
}

func TestStopRuntimeSeparatesAuthorityExpiryFromOperationSettlement(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(stopControlHelperMode, "after-authority-expiry")
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")}
	releaseMarker := filepath.Join(t.TempDir(), "released")
	t.Setenv(stopControlHelperReleaseMarker, releaseMarker)
	controller := &Controller{
		stopReaper: &proc.Reaper{Store: store, Generation: generation},
		stopTiming: stopControlTiming{identity: 5 * time.Second, authority: 250 * time.Millisecond, operation: 5 * time.Second},
	}
	result, err := controller.StopRuntime(t.Context(), StopControlSpec{
		Executable: executable,
		Args:       []string{"-test.run=^TestStopControlHelperProcess$"},
		Role:       "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
		TargetProcessGeneration: "runtime-generation", Intent: wire.StopIntentRestart,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Stopped || result.ProcessGeneration != "runtime-generation" {
		t.Fatalf("result = %+v", result)
	}
	if data, err := os.ReadFile(releaseMarker); err != nil || string(data) != "released" {
		t.Fatalf("release marker = %q, %v; want released", data, err)
	}
}

func TestStopRuntimeRevokesWithoutReleaseWhenFullAuthorityWindowWasConsumed(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(stopControlHelperMode, "after-authority-expiry")
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")}
	releaseMarker := filepath.Join(t.TempDir(), "released")
	t.Setenv(stopControlHelperReleaseMarker, releaseMarker)
	revoked := make(chan struct{})
	finish := make(chan struct{})
	controller := &Controller{
		stopReaper: &proc.Reaper{Store: store, Generation: generation},
		stopTiming: stopControlTiming{
			identity: 5 * time.Second, authority: 50 * time.Millisecond, operation: 5 * time.Second,
			now: func() time.Time { return time.Now().Add(time.Second) },
			afterRevoke: func() {
				close(revoked)
				<-finish
			},
		},
	}
	type outcome struct {
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		_, err := controller.StopRuntime(context.Background(), StopControlSpec{
			Executable: executable,
			Args:       []string{"-test.run=^TestStopControlHelperProcess$"},
			Role:       "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
			TargetProcessGeneration: "runtime-generation", Intent: wire.StopIntentRestart,
		})
		result <- outcome{err: err}
	}()
	<-revoked
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].StopAuthorityState != proc.StopAuthorityRevoked {
		t.Fatalf("records before cleanup = %+v, want one revoked stop child", records)
	}
	if _, err := os.Stat(releaseMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("release marker before cleanup: %v; FD4 must remain unreadable", err)
	}
	identity := proc.Identity{
		PID: records[0].PID, StartTime: records[0].StartTime, Boot: records[0].Boot,
		Comm: records[0].Comm, Executable: records[0].Executable, AuditToken: records[0].AuditToken,
	}
	if consumed, ok, err := store.ConsumeStopControl(
		t.Context(), identity, records[0].Role, records[0].TargetProcessGeneration, time.Now(),
	); err != nil || ok || consumed != (proc.Record{}) {
		t.Fatalf("revoked consume = %+v, %v, %v; want zero, false, nil", consumed, ok, err)
	}
	close(finish)
	got := <-result
	if got.err == nil || !strings.Contains(got.err.Error(), "authority does not retain the full window") {
		t.Fatalf("StopRuntime error = %v, want full-window rejection", got.err)
	}
	records, err = store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after fail-closed cleanup = %+v, want none", records)
	}
	if _, err := os.Stat(releaseMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("release marker after cleanup: %v; FD4 was released", err)
	}
}

func TestStopRuntimePropagatesDeclinedResult(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(stopControlHelperMode, "declined")
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{
		stopReaper: &proc.Reaper{Store: &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")}, Generation: generation},
	}
	result, err := controller.StopRuntime(t.Context(), StopControlSpec{
		Executable: executable,
		Args:       []string{"-test.run=^TestStopControlHelperProcess$"},
		Role:       "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
		TargetProcessGeneration: "runtime-generation", Intent: wire.StopIntentUpgrade,
	})
	if !errors.Is(err, ErrStopDeclined) || result.Stopped || result.ProcessGeneration != "runtime-generation" {
		t.Fatalf("StopRuntime = %+v, %v; want declined result", result, err)
	}
}

type stopRoundTripClassifier struct{}

func (stopRoundTripClassifier) Validate() error { return nil }
func (stopRoundTripClassifier) Classify(context.Context, wire.Peer) (bool, error) {
	return true, nil
}

type stopRoundTripWorkers struct{}

func (stopRoundTripWorkers) Close()                     {}
func (stopRoundTripWorkers) Cancel()                    {}
func (stopRoundTripWorkers) Wait(context.Context) error { return nil }

type stopRoundTripCloser struct{}

func (stopRoundTripCloser) Close() error { return nil }

func stopRoundTripArgs() []string {
	for index, arg := range os.Args {
		if arg == "--" {
			return os.Args[index+1:]
		}
	}
	return nil
}

func TestStopControlRoundTripHelper(_ *testing.T) {
	args := stopRoundTripArgs()
	if len(args) == 0 {
		return
	}
	switch args[0] {
	case "stop-child":
		if len(args) != 2 {
			os.Exit(81)
		}
		result, err := RunStopControlChild(context.Background(), StopControlClientConfig{
			Dial: wire.UnixDialer(args[1]), WireBuild: "stop-suite-v1", RuntimeProtocol: 1,
		})
		if err != nil || !result.Stopped {
			os.Exit(82)
		}
	case "target":
		if len(args) != 4 {
			os.Exit(83)
		}
		socket, processPath, readyPath := args[1], args[2], args[3]
		server := &wire.Server{WireBuild: "stop-suite-v1", MaxSessions: 4}
		intake := &drain.Intake{}
		classifier := stopRoundTripClassifier{}
		runtime, err := wire.NewRuntime(wire.RuntimeConfig{
			Socket: socket, RuntimeBuild: "v1.0.0", RuntimeProtocol: 1,
			Wire: server, Classifier: classifier, ReservedProtectedSessions: 1,
			StopVerifier: wire.StopVerifier{
				Classifier: classifier, Role: "com.example.stop",
				Store: &proc.FileStore{Path: processPath},
			},
			Admission: intake, Workers: stopRoundTripWorkers{},
			State: stopRoundTripCloser{}, Resources: stopRoundTripCloser{},
			Activate: func(daemon.Activation) error {
				generation, err := proc.ProcessGeneration()
				if err != nil {
					return err
				}
				return os.WriteFile(readyPath, []byte(generation), 0o600)
			},
		})
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(84)
		}
		if err := runtime.Run(context.Background()); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(85)
		}
	default:
		os.Exit(86)
	}
}

func TestStopRuntimeOldTargetNewSuccessorRoundTrip(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp("/tmp", "daemonkit-stop-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "runtime.sock")
	processPath := filepath.Join(directory, "processes.db")
	readyPath := filepath.Join(directory, "ready")
	target := exec.Command(
		executable, "-test.run=^TestStopControlRoundTripHelper$", "--",
		"target", socket, processPath, readyPath,
	)
	var targetOutput bytes.Buffer
	target.Stdout = &targetOutput
	target.Stderr = &targetOutput
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	targetWait := make(chan error, 1)
	go func() { targetWait <- target.Wait() }()
	settled := false
	defer func() {
		if !settled {
			_ = target.Process.Kill()
			<-targetWait
		}
	}()
	var targetProcessGeneration string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		payload, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			probeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			conn, dialErr := wire.UnixDialer(socket)(probeCtx)
			cancel()
			if dialErr == nil {
				_ = conn.Close()
				targetProcessGeneration = string(payload)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if targetProcessGeneration == "" {
		select {
		case waitErr := <-targetWait:
			settled = true
			t.Fatalf("target runtime exited before publish: %v: %s", waitErr, targetOutput.String())
		default:
		}
		t.Fatal("target runtime did not publish")
	}
	controllerGeneration, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	store := &proc.FileStore{Path: processPath}
	controller := &Controller{stopReaper: &proc.Reaper{Store: store, Generation: controllerGeneration}}
	result, err := controller.StopRuntime(t.Context(), StopControlSpec{
		Executable: executable,
		Args: []string{
			"-test.run=^TestStopControlRoundTripHelper$", "--", "stop-child", socket,
		},
		Role: "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
		TargetProcessGeneration: targetProcessGeneration, Intent: wire.StopIntentUpgrade,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Stopped || result.Process.PID != target.Process.Pid ||
		result.ProcessGeneration != targetProcessGeneration || result.RuntimeBuild != "v1.0.0" ||
		result.RuntimeProtocol != 1 {
		t.Fatalf("stop result = %+v", result)
	}
	if err := <-targetWait; err != nil {
		t.Fatalf("target exit: %v", err)
	}
	settled = true
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("post-round-trip records = %+v", records)
	}
}
