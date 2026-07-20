package supervise

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const trackedOwnerHelperEnv = "DAEMONKIT_TRACKED_OWNER_HELPER"

func TestTrackedOwnerHelper(_ *testing.T) {
	mode := os.Getenv(trackedOwnerHelperEnv)
	if mode == "" {
		return
	}
	record, err := ReceiveTrackedOwner(context.Background(), proc.RecoverySourceOwner)
	if mode == "reject" {
		if err == nil {
			fmt.Fprintln(os.Stderr, "forged tracked owner was accepted")
			os.Exit(91)
		}
		if _, err := fmt.Fprintln(os.Stdout, "rejected"); err != nil {
			os.Exit(96)
		}
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(92)
	}
	if err := json.NewEncoder(os.Stdout).Encode(record); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(93)
	}
	switch mode {
	case "accept":
		return
	case "block":
		for {
			time.Sleep(time.Hour)
		}
	case "descendant":
		child := exec.Command("/bin/sh", "-c", `trap "" TERM; while :; do sleep 10; done`)
		child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := child.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(94)
		}
		if _, err := fmt.Fprintln(os.Stdout, child.Process.Pid); err != nil {
			os.Exit(97)
		}
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		os.Exit(95)
	}
}

type recordingOwnerRegistry struct {
	*proc.Reaper
	started chan int
	release <-chan struct{}

	mu     sync.Mutex
	record proc.Record
}

func (r *recordingOwnerRegistry) TrackGroup(ctx context.Context, pid int, class proc.RecoveryClass) (proc.Record, error) {
	if r.started != nil {
		select {
		case r.started <- pid:
		case <-ctx.Done():
			return proc.Record{}, ctx.Err()
		}
	}
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return proc.Record{}, ctx.Err()
		}
	}
	record, err := r.Reaper.TrackGroup(ctx, pid, class)
	if err == nil {
		r.mu.Lock()
		r.record = record
		r.mu.Unlock()
	}
	return record, err
}

func (r *recordingOwnerRegistry) recorded() proc.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.record
}

func TestRunDeliversExactOwnerOnlyAfterDurableTracking(t *testing.T) {
	release := make(chan struct{})
	registry := &recordingOwnerRegistry{
		Reaper: &proc.Reaper{
			Store:      &proc.FileStore{Path: t.TempDir() + "/owners.db"},
			Generation: "owner-generation",
		},
		started: make(chan int, 1),
		release: release,
	}
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(context.Background(), Task{
			RecoveryClass: proc.RecoverySourceOwner,
			Path:          executable,
			Args:          []string{"-test.run=^TestTrackedOwnerHelper$"},
			Env:           append(os.Environ(), trackedOwnerHelperEnv+"=accept"),
			Stdout:        &stdout,
		})
	}()
	select {
	case <-registry.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker was not offered for durable tracking")
	}
	if stdout.Len() != 0 {
		t.Fatalf("worker ran before durable owner handoff: %q", stdout.String())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var received proc.Record
	if err := json.NewDecoder(&stdout).Decode(&received); err != nil {
		t.Fatalf("decode received owner: %v; output=%q", err, stdout.String())
	}
	if received != registry.recorded() {
		t.Fatalf("received owner = %+v, want exact tracked %+v", received, registry.recorded())
	}
}

func TestReceiveTrackedOwnerRejectsMissingHandoff(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestTrackedOwnerHelper$")
	command.Env = append(os.Environ(), trackedOwnerHelperEnv+"=reject")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("missing handoff helper: %v: %s", err, output)
	}
	if !strings.Contains(string(output), "rejected") {
		t.Fatalf("missing handoff output = %q", output)
	}
}

func TestReceiveTrackedOwnerRejectsForgedIdentity(t *testing.T) {
	tests := map[string]func(*proc.Record){
		"wrong pid": func(record *proc.Record) {
			record.PID++
			record.SessionID = record.PID
		},
		"reused start":       func(record *proc.Record) { record.StartTime += "-reused" },
		"foreign boot":       func(record *proc.Record) { record.Boot += "-foreign" },
		"wrong class":        func(record *proc.Record) { record.RecoveryClass = proc.RecoveryTask },
		"missing generation": func(record *proc.Record) { record.Generation = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			output := runForgedOwnerHelper(t, mutate)
			if !strings.Contains(output, "rejected") {
				t.Fatalf("forged handoff output = %q", output)
			}
		})
	}
}

func TestKilledOwnerProducesExactRecoveryReceipt(t *testing.T) {
	store := &proc.FileStore{Path: t.TempDir() + "/owners.db"}
	prior := &proc.Reaper{Store: store, Generation: "prior-generation"}
	command, ownerWrite, stdout := startDirectOwnerHelper(t, "block")
	record, err := prior.TrackGroup(context.Background(), command.Process.Pid, proc.RecoverySourceOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTrackedOwner(ownerWrite, record); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var received proc.Record
	if err := json.Unmarshal(line, &received); err != nil {
		t.Fatal(err)
	}
	if received != record {
		t.Fatalf("received owner = %+v, want %+v", received, record)
	}
	if err := syscall.Kill(-record.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed owner exited successfully")
	}
	_ = stdout.Close()

	next := &proc.Reaper{Store: store, Generation: "next-generation"}
	if err := next.Reap(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := next.ReapReceipts(context.Background(), proc.RecoverySourceOwner, proc.ReapReceiptCursor{}, proc.ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Receipts) != 1 || page.Receipts[0].Record != record || page.Receipts[0].Outcome != proc.ReapAbsent {
		t.Fatalf("recovery receipts = %+v, want exact absent owner", page)
	}
}

func TestRunReapsLateOwnerDescendantGroup(t *testing.T) {
	registry := &proc.Reaper{
		Store:      &proc.FileStore{Path: t.TempDir() + "/owners.db"},
		Generation: "owner-generation",
	}
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := pool.Run(context.Background(), Task{
		RecoveryClass: proc.RecoverySourceOwner,
		Path:          executable,
		Args:          []string{"-test.run=^TestTrackedOwnerHelper$"},
		Env:           append(os.Environ(), trackedOwnerHelperEnv+"=descendant"),
		Stdout:        &stdout,
	}); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(&stdout)
	if !scanner.Scan() {
		t.Fatalf("missing owner output: %v", scanner.Err())
	}
	if !scanner.Scan() {
		t.Fatalf("missing descendant pid: %v", scanner.Err())
	}
	childPID, err := strconv.Atoi(scanner.Text())
	if err != nil {
		t.Fatal(err)
	}
	assertPIDGone(t, childPID)
}

func runForgedOwnerHelper(t *testing.T, mutate func(*proc.Record)) string {
	t.Helper()
	command, ownerWrite, stdout := startDirectOwnerHelper(t, "reject")
	identity, err := proc.Probe(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	record := proc.Record{
		RecoveryClass: proc.RecoverySourceOwner,
		PID:           identity.PID,
		StartTime:     identity.StartTime,
		Boot:          identity.Boot,
		Comm:          identity.Comm,
		Generation:    "forged-generation",
		ProcessGroup:  true,
		SessionID:     identity.PID,
	}
	mutate(&record)
	if err := writeTrackedOwner(ownerWrite, record); err != nil {
		t.Fatal(err)
	}
	output, err := ioReadAllAndWait(stdout, command)
	if err != nil {
		t.Fatalf("forged helper: %v: %s", err, output)
	}
	return string(output)
}

func startDirectOwnerHelper(t *testing.T, mode string) (*exec.Cmd, *os.File, *os.File) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ownerRead, ownerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	nulls := make([]*os.File, 3)
	for index := range nulls {
		nulls[index], err = os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer nulls[index].Close()
	}
	command := exec.Command(executable, "-test.run=^TestTrackedOwnerHelper$")
	command.Env = append(os.Environ(), trackedOwnerHelperEnv+"="+mode)
	command.ExtraFiles = []*os.File{nulls[0], nulls[1], nulls[2], ownerRead}
	command.Stdout = stdoutWrite
	command.Stderr = stdoutWrite
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = ownerRead.Close()
	_ = stdoutWrite.Close()
	return command, ownerWrite, stdoutRead
}

func ioReadAllAndWait(stdout *os.File, command *exec.Cmd) ([]byte, error) {
	output, readErr := io.ReadAll(stdout)
	waitErr := command.Wait()
	_ = stdout.Close()
	return output, errors.Join(readErr, waitErr)
}
